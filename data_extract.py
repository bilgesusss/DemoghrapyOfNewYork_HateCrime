import json
import pandas as pd
#from cassandra.cluster import Cluster
from dagster import op, Out, In, get_dagster_logger
from datetime import date, datetime
from pymongo import MongoClient, errors
from cassandra.cluster import Cluster
import requests

mongo_connection_string = "mongodb://dap:dap@127.0.0.1"
logger = get_dagster_logger()

@op(
    out=Out(bool)
)

def extract_zipcodes() -> bool:
    result = True
    try:
        client = MongoClient("mongodb://%s:%s@127.0.0.1" % ("dap", "dap"))
        db = client['zipcodes']
        #client.drop_database('zipcodes')
        collision_db = client['zipcodes']
        collection = collision_db['zipcodes']
        with open('C:\\Users\\Bilgesu\\Desktop\\MScDataAnalystic\\DatabaseAnalysticProgramming\\Project\\zipcodes.json') as file:
            file_data = json.load(file)
        meta = file_data["meta"]
        view = meta["view"]
        columns = view["columns"]
        data = file_data["data"]

        selected_columns = [":id", "county", "zip_code", "county_code"]
        json_data = []
        
        for row in data:
            rowDict = {}
            for i, value in enumerate(row):
                if(columns[i]["fieldName"] in selected_columns):
                    rowDict[columns[i]["fieldName"]] = value
            json_data.append(rowDict)
        collection.insert_many(json_data)

        print(json_data)
   
    except Exception as err:
        logger.error("Error: %s" % err)
        result = False
    
    return result

@op(
    out=Out(bool)
)

def extract_demography() -> bool:
    result = False
    try:
        #demographics_df = []
        demographics_df = pd.read_csv("C:\\Users\\Bilgesu\\Desktop\\MScDataAnalystic\\DatabaseAnalysticProgramming\\Project\\Demographics_by_Zip_Code.csv",
        usecols = [1,2,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49],
        skiprows = 1,
        memory_map = True,
        low_memory = False)


        for column in demographics_df.columns:
            #print(demographics_df[column].dtype)
            if demographics_df[column].dtype == 'float64':
                demographics_df[column] = demographics_df[column].fillna(0).astype(int)

        cassandra = Cluster(["127.0.0.1"])
        cassandra_session = cassandra.connect()

        cassandra_session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS demographics
            WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};
            """
        )
        
        cassandra_session.execute(
            """
            USE demographics;
            """
        )
        
        cassandra_session.execute(
            """
            CREATE TABLE IF NOT EXISTS demographics(
                id bigint,
                Zip_Code varchar,
                Program_Type varchar,
                Female_Count int,
                Male_Count int,
                Gender_Nonconforming_Count int,
                American_Indian_or_Alaskan_Native_Count int,
                Asian_Count int,
                Black_or_African_American_Count int,
                Multi_race_Count int,
                Native_Hawaiian_or_Other_Pacific_Islander_Count int,
                White_or_Caucasian_Count int,
                Hispanic_or_Latinx_Count int,
                Not_Hispanic_or_Latinx_Count int,
                Not_Sure_Count int,
                X_Count int,
                Another_Gender_Count int,
                Decline_to_Answer_Count int,
                Do_not_understand_the_question_Count int,
                Female_Gender_Identity_Count int,
                Male_Gender_Identity_Count int,
                Multi_Gender_Identity_Count int,
                Non_Binary_Count int,
                Not_Sure_Gender_Identity_Count int,
                Two_Spirit_Count int,
                Middle_Eastern_and_North_African_Count int,
                Other_Count int,
                PRIMARY KEY(id))
            """
        )

        insert_string = ''' INSERT INTO demographics (id,
        Zip_Code,
        Program_Type,
        Female_Count,
        Male_Count,
        Gender_Nonconforming_Count,
        American_Indian_or_Alaskan_Native_Count,
        Asian_Count,
        Black_or_African_American_Count,
        Multi_race_Count,
        Native_Hawaiian_or_Other_Pacific_Islander_Count,
        White_or_Caucasian_Count,
        Hispanic_or_Latinx_Count,
        Not_Hispanic_or_Latinx_Count,
        Not_Sure_Count,
        X_Count,
        Another_Gender_Count,
        Decline_to_Answer_Count,
        Do_not_understand_the_question_Count,
        Female_Gender_Identity_Count,
        Male_Gender_Identity_Count,
        Multi_Gender_Identity_Count,
        Non_Binary_Count,
        Not_Sure_Gender_Identity_Count,
        Two_Spirit_Count,
        Middle_Eastern_and_North_African_Count,
        Other_Count)
        VALUES ({}, '{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})'''

        row_values = []
        for index, row in demographics_df.iterrows():
            row_values = row.values.tolist()
            row_values.insert(0, index)
            cassandra_session.execute(insert_string.format(*row_values))
    
    except Exception as err:
        logger.error("Error: %s" % err)
        result = False
    
    return result
        
@op(
    out=Out(bool)
)

def extract_hateCrimes() -> bool:
    result = False
    try:
        url = f'https://data.cityofnewyork.us/resource/bqiq-cu78.json'
        response = requests.get(url)
        
        client_hateCrime = MongoClient("mongodb://%s:%s@127.0.0.1" % ("dap", "dap"))

        hateCrimes_db = client_hateCrime["hateCrimes2"]
        hateCrimes_collection = hateCrimes_db["hateCrimes2"]
        hateCrimes_collection.drop()
        data = response.json()
        for item in data:
            if(item['offense_category'] == "Race/Color"):
                try:
                    hateCrimes_collection.insert_one(item)
                except errors.WriteError as err:
                    logger.error("Error: %s" % err)
                    continue
    except Exception as err:
        logger.error("Error: %s" % err)
        result = False
    
    return result