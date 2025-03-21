import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()


def extract_data():
    df = pd.read_csv("./data/Electric_Vehicle_Population_Data.csv")
    return df


def transform_data(df):
    df = df.dropna()
    df = df[
        [
            "VIN (1-10)",
            "Make",
            "Model Year",
            "County",
            "Electric Range",
            "Electric Vehicle Type",
        ]
    ]
    return df


def load_data(df):
    engine = create_engine(os.getenv("DATABASE_URL"))

    table_name = "ev_table"
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)

    print(f"Data successfully inserted into {table_name} table.")
    return


if __name__ == "__main__":
    df = extract_data()
    df = transform_data(df)
    load_data(df)
