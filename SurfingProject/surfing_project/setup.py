from setuptools import find_packages, setup

setup(
    name="surfing_project",
    version='1.0.0',
    packages=find_packages(exclude=["surfing_project_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "requests",
        "matplotlib",
        "lxml",
        "psycopg2",
        "folium",
        "markdownify"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
