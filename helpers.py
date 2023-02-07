import logging
from typing import *

import pandas as pd
import yaml

logging.basicConfig(level=logging.INFO)


def load_config(file_name: str) -> dict:
    """
    Load the configuration file and store it as a dictionary.
    :returns: The configuration settings as a dictionary.
    """
    with open(file_name, "r") as file:
        return yaml.safe_load(file)


def yaml_to_df(text: Union[str, bytes]) -> pd.DataFrame:
    """
    Convert the YAML found on the EtherscamDB page to a pandas DataFrame.
    :param text: The YAML to convert.
    :returns: A pandas DataFrame representing the data.
    """
    # Account for malformed document (uris.yml)
    text = text.replace(b"  - name:", b"- name:")

    # Convert YAML to DataFrame
    data = yaml.safe_load(text)
    df = pd.json_normalize(data)

    # Filter and split columns with multiple addresses
    df_address = df[df["addresses"].astype(str).str.contains("-")].copy()
    df = df[~df["addresses"].astype(str).str.contains("-")].copy()

    # Split address column into multiple rows
    df_address = (
        df_address.set_index(["id", "name", "description", "category"])
        .apply(lambda x: x.apply(pd.Series).stack())
        .reset_index()
    )

    df_address.rename(columns={0: "address"}, inplace=True)

    # Concatenate the two dataframes
    df = pd.concat([df, df_address], sort=False)
    return df


def write_to_csv(df: pd.DataFrame, file: str) -> None:
    """
    Write a pandas DataFrame to a CSV file.
    :param df: The pandas DataFrame to use.
    :param file: The file to write to.
    """
    # Add .csv extension if it doesn't already exist
    if not file.endswith(".csv"):
        file = f"{file}.csv"

    df.to_csv(file, index=False)
    logging.info(f"Data was successfully written to {file}")
