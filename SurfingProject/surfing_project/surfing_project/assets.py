import pandas as pd
import folium
from io import BytesIO
import io
import base64
from PIL import Image

from dagster import (  # AssetExecutionContext,
                     MetadataValue,
                     asset,
                     MaterializeResult)

from .resources import postgres_con


@asset
def real_time_swell_list(localDB: postgres_con) -> MaterializeResult:
    # URL of the website containing the table
    url = "https://www.ndbc.noaa.gov/data/realtime2/"
    # Read HTML tables into a list of DataFrames
    dfs = pd.read_html(url)
    # Select the desired table (e.g., the first one)
    df = dfs[0]

    df[["ID", "Type"]] = df["Name"].str.split(".", n=1, expand=True)
    df = df.iloc[1:, :]
    df = df.dropna(axis=1)

    engine = localDB.make_con()
    df.to_sql("NewFiles",
              con=engine,
              if_exists='replace',
              index=False)

    # Print the DataFrame to the Metadata
    return MaterializeResult(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )


@asset
def bouy_names(localDB: postgres_con) -> MaterializeResult:
    engine = localDB.make_con()

    bouy_meta = pd.read_xml("https://www.ndbc.noaa.gov/activestations.xml")

    sql = "SELECT * FROM public.\"BouyMeta\";"
    bouy_loaded = pd.read_sql(sql, con=engine)

    existing_bouys = bouy_loaded['id'].values
    bouy_meta = bouy_meta[~bouy_meta['id'].isin(existing_bouys)]

    bouy_meta.to_sql("BouyMeta",
                     con=engine,
                     if_exists='append',
                     index=False)

    return MaterializeResult(
        metadata={
            "num_records": len(bouy_meta),
            "preview": MetadataValue.md(bouy_meta.head().to_markdown()),
        }
    )


@asset(deps=[bouy_names])
def east_coast_bouy_names(localDB: postgres_con) -> MaterializeResult:
    engine = localDB.make_con()

    sql = "SELECT * FROM public.\"BouyMeta\";"

    bouy_meta = pd.read_sql(sql,
                            con=engine)
    bouy_meta = bouy_meta[bouy_meta["lon"].between(-84, -55)
                          & bouy_meta["lat"].between(23, 47)]

    bouy_meta.to_sql("BouyMetaNorthEast",
                     con=engine,
                     if_exists='replace',
                     index=False)

    # marking metadata
    m = folium.Map(location=[bouy_meta['lat'].iloc[0],
                             bouy_meta['lon'].iloc[0]], zoom_start=5)

    for index, row in bouy_meta.iterrows():
        folium.Marker(
            location=[row['lat'], row['lon']],
            popup=row['name']
        ).add_to(m)

    buffer = BytesIO()
    img = m._to_png(5)
    img = Image.open(io.BytesIO(img))
    img.save(buffer, format="PNG")
    image_data = base64.b64encode(buffer.getvalue())

    # Convert the image to Markdown to preview it within Dagster
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    return MaterializeResult(
        metadata={
            "num_records": len(bouy_meta),
            "preview": MetadataValue.md(bouy_meta.head().to_markdown()),
            "map": MetadataValue.md(md_content)
        }
    )


def with_date_time(df):
    df['Timestamp'] = df['#YY : #yr'].astype(str) \
        + '-' + df['MM : mo'].astype(str)
    df['Timestamp'] = df['Timestamp'].astype(str) \
        + '-' + df['DD : dy'].astype(str)
    df['Timestamp'] = df['Timestamp'].astype(str) \
        + ' ' + df['hh : hr'].astype(str)
    df['Timestamp'] = df['Timestamp'].astype(str) \
        + ':' + df['mm : mn'].astype(str)

    df["Timestamp"] = pd.to_datetime(df['Timestamp'])

    return df


@asset(deps=[real_time_swell_list, east_coast_bouy_names])
def real_time_bouy_data(localDB: postgres_con) -> MaterializeResult:
    engine = localDB.make_con()

    sql = "SELECT * FROM public.\"NewFiles\";"
    files = pd.read_sql(sql, con=engine)

    sql = "SELECT * FROM public.\"BouyMetaNorthEast\";"

    bouy_meta = pd.read_sql(sql, con=engine)

    bouy_list = bouy_meta['id'].values
    files = files[files['ID'].isin(bouy_list)]

    wave_files = files[files["Type"] == "spec"]
    wave_files_i = wave_files["Name"].to_list()

    wave_data = pd.DataFrame([])
    url = "https://www.ndbc.noaa.gov/data/realtime2/"

    for file_name in wave_files_i:
        file = wave_files[wave_files["Name"] == file_name]
        data_url = rf"{url}{file_name}"
        print(data_url)
        data = pd.read_csv(data_url, sep=r'\s+', header=[0, 1])

        data.columns = data.columns.map(' : '.join)

        data["FileName"] = file_name
        data["Last modified"] = file["Last modified"]
        data["Last modified"] = file["ID"]
        wave_data = pd.concat([data, wave_data])

    wave_data = with_date_time(wave_data)

    wave_data.to_sql("BouyDataCurrent",
                     con=engine,
                     if_exists='replace',
                     index=False)

    # Print the DataFrame to the Metadata
    return MaterializeResult(
        metadata={
            "num_records": len(wave_data),
            "preview": MetadataValue.md(wave_data.head().to_markdown()),
        }
    )


@asset(deps=[real_time_bouy_data])
def store_new_data(localDB: postgres_con) -> MaterializeResult:
    engine = localDB.make_con()
    sql = "SELECT * FROM public.\"BouyDataCurrent\";"
    bouy_current = pd.read_sql(sql, con=engine)

    sql = "SELECT * FROM public.\"BouyDataCurrent\" \
           WHERE \"Timestamp\">= Current_Date -2;"
    bouy_hist = pd.read_sql(sql, con=engine)
    bouy_set = pd.concat([bouy_hist, bouy_current])
    bouy_set.drop_duplicates(inplace=True)

    bouy_set.to_sql("Bouy_Data")
    bouy_set.to_sql("BouyDataCurrent",
                    con=engine,
                    if_exists='replace',
                    index=False)

    # Print the DataFrame to the Metadata
    return MaterializeResult(
        metadata={
            "num_records": len(bouy_set),
            "preview": MetadataValue.md(bouy_set.head().to_markdown()),
        }
    )
