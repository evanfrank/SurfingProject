import pandas as pd

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
    df.to_sql("NewFiles", con=engine, if_exists='replace', index='False')

    # Print the DataFrame to the Metadata
    return MaterializeResult(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )


@asset(deps=[real_time_swell_list])
def bouy_data(localDB: postgres_con) -> MaterializeResult:
    engine = localDB.make_con()

    sql = "SELECT * FROM public.\"NewFiles\";"
    files = pd.read_sql(sql, con=engine)
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

    wave_data.to_sql("BouyData",
                     con=engine,
                     if_exists='replace',
                     index='False')

    # Print the DataFrame to the Metadata
    return MaterializeResult(
        metadata={
            "num_records": len(wave_data),
            "preview": MetadataValue.md(wave_data.head().to_markdown()),
        }
    )
