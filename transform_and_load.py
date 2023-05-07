import numpy as np
import pandas as pd
from cassandra.cluster import Cluster
from dagster import op, Out, In, get_dagster_logger
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from sqlalchemy import create_engine, exc
from sqlalchemy.pool import NullPool
from sqlalchemy.types import *
from pymongo import MongoClient, errors


postgres_connection_string = "postgresql://postgres:dap@127.0.0.1:5432/postgres"
mongo_connection_string = "mongodb://dap:dap@127.0.0.1"
logger = get_dagster_logger()

DemographicsDataFrame = create_dagster_pandas_dataframe_type(
    name="DemographicsDataFrame",
    columns=[
        PandasColumn.string_column(
            name="zip_code",
            non_nullable=True    
        ),
        PandasColumn.string_column(
            name="program_type",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="female_count",
            non_nullable=True
        ),
        PandasColumn.integer_column(
            name="male_count",
            non_nullable=True
        ),
        PandasColumn.integer_column(
            name="gender_nonconforming_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="american_indian_or_alaskan_native_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="asian_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="black_or_african_american_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="multi_race_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="native_hawaiian_or_other_pacific_islander_count",
            non_nullable=True
        ),
        PandasColumn.integer_column(
            name="native_hawaiian_or_other_pacific_islander_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="white_or_caucasian_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="hispanic_or_latinx_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="not_hispanic_or_latinx_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="not_sure_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="x_count",
            non_nullable=True
        ),
        PandasColumn.integer_column(
            name="another_gender_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="decline_to_answer_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="do_not_understand_the_question_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="female_gender_identity_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="male_gender_identity_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="multi_gender_identity_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="non_binary_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="not_sure_gender_identity_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="two_spirit_count",
            non_nullable=True
        ),
        PandasColumn.integer_column(
            name="middle_eastern_and_north_african_count",
            non_nullable=True    
        ),
        PandasColumn.integer_column(
            name="other_count",
            non_nullable=True    
        )
    ]
)

ZipCodesDataFrame = create_dagster_pandas_dataframe_type(
    name="ZipCodesDataFrame",
    columns=[
        PandasColumn.string_column(
            name="county",
            non_nullable=True
        ),
        PandasColumn.string_column(
            name="zip_code",
            non_nullable=True
        ),
        PandasColumn.string_column(
            name="county_code",
            non_nullable=True
        )
    ]
)

CrimesDataFrame = create_dagster_pandas_dataframe_type(
    name="CrimesDataFrame",
    columns=[
        PandasColumn.string_column(
            name="county",
            non_nullable=True
        ),
        PandasColumn.datetime_column(
            name="record_create_date",
            non_nullable=True
        )
    ]
)

@op(
    ins={"start": In(bool)},
    out=Out(DemographicsDataFrame)
)

def transform_demography(start):
    cassandra = Cluster(["127.0.0.1"])
    cassandra_session = cassandra.connect()
    
    keyspaces = cassandra.metadata.keyspaces.keys()
    cassandra_session.execute(
            """
            USE demographics;
            """
        )
        
    df_demographics= pd.DataFrame(list(
        cassandra_session.execute("SELECT * FROM demographics;")
    ))
    
    df_demographics.drop(["id"],axis=1,inplace=True)

    return df_demographics


@op(
    ins={"start": In(bool)},
    out=Out(ZipCodesDataFrame)
)

def transform_zipcodes(start):
    client = MongoClient("mongodb://dap:dap@127.0.0.1")
    
    zipcodes_db = client["zipcodes"]

    collection_zipcodes = zipcodes_db['zipcodes']

    df_zipcodes = pd.json_normalize(list(zipcodes_db.zipcodes.find()))

    df_zipcodes.drop(["_id"],axis=1,inplace=True)
    df_zipcodes.drop([":id"],axis=1,inplace=True)
    client.close()
    return df_zipcodes

@op(
    ins={"start": In(bool)},
    out=Out(str)
)

def transform_crimes(start):
    client = MongoClient("mongodb://dap:dap@127.0.0.1")
    crimes_db = client["hateCrimes"]

    collection_zipcodes = crimes_db['hateCrimes']

    df_crimes = pd.json_normalize(list(crimes_db.hateCrimes.find({'bias_motive_description': 'ANTI-ASIAN'})))

    grouped = df_crimes.groupby('county').size().reset_index(name='total_records')

    filtred_crimes_df = grouped.sort_values('total_records', ascending=False)[:1]

    filtered_county = filtred_crimes_df.loc[filtred_crimes_df.index[0], 'county']
    client.close()
    #print(filtered_county)
    return str(filtered_county)

@op(
    ins={"df_demographics": In(pd.DataFrame), "df_zipcodes": In(ZipCodesDataFrame)},
    out=Out(pd.DataFrame)
)

def join(df_demographics,df_zipcodes) -> pd.DataFrame:

    merged_df = df_demographics.merge(
        right=df_zipcodes,
        how="left",
        left_on="zip_code",
        right_on="zip_code"
    )
    
    merged_df.drop(["zip_code"],axis=1,inplace=True)
    return(merged_df)

@op(
    ins={"merged_df": In(pd.DataFrame)},
    out=Out(bool)
)

def load(merged_df):
    try:
        engine = create_engine(
            postgres_connection_string,
            poolclass=NullPool
        )
        database_datatypes = dict(
            zip(merged_df.columns,[INT]*len(merged_df.columns))
        )
        
        for column in ["zip_code","program_type", "county","county_code"]:
            database_datatypes[column] = VARCHAR
        
        with engine.connect() as conn:
            rowcount = merged_df.to_sql(
            name="demographic_counties",
            schema="public",
            dtype=database_datatypes,
            con=engine,
            index=False,
            if_exists="replace"
        )
        logger.info("{} records loaded".format(rowcount))
            
        engine.dispose(close=True)
        return rowcount > 0
    
    # Trap and handle any relevant errors
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False


