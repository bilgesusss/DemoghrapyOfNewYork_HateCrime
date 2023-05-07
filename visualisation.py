import pandas.io.sql as sqlio
from dagster import op, In
from sqlalchemy import create_engine, text, exc
from sqlalchemy.engine.url import URL
from bokeh.models import ColumnDataSource, FixedTicker
from bokeh.transform import factor_cmap
from bokeh.plotting import  figure, show
import pandas as pd

@op(
    ins={"start": In(bool)}
)

def visualise(start, city):
    try:

        postgres_connection_string = "postgresql://postgres:dap@127.0.0.1:5432/postgres"

        query_str_race = f"""SELECT 
            SUM(american_indian_or_alaskan_native_count) as american_indian_or_alaskan_native_count,
            SUM(black_or_african_american_count) as black_or_african_american_count,
            SUM(native_hawaiian_or_other_pacific_islander_count) as native_hawaiian_or_other_pacific_islander_count,
            SUM(white_or_caucasian_count) as white_or_caucasian_count,
            SUM(other_count + Middle_Eastern_and_North_African_Count) as other_count
        FROM 
            demographic_counties
        WHERE
            county = '{city.title()}';"""

        engine = create_engine(postgres_connection_string)  
        with engine.connect() as connection:
            demographic_dataframe_race = pd.read_sql_query(text(query_str_race), connection)

        races = ['American Indian Or Alaskan', 'Black Or African American',
                'Hawaiian Or Other Pacific Islander','White Or Caucasian', 'Other']
        custom_colors_race = ['blue','yellow','purple','orange','green']

        count = [str(demographic_dataframe_race.loc[0, 'american_indian_or_alaskan_native_count']),
                str(demographic_dataframe_race.loc[0,'black_or_african_american_count']),
                str(demographic_dataframe_race.loc[0,'native_hawaiian_or_other_pacific_islander_count']),
                str(demographic_dataframe_race.loc[0,'white_or_caucasian_count']),
                str(demographic_dataframe_race.loc[0,'other_count']),
                ]

        source_race = ColumnDataSource(data={
            "races": races,
            "count": count
        })

        f_race = figure(x_range=races, height=900, width=800,
                title="Races", x_axis_label="races",
                y_axis_label="Number")


        f_race.vbar(x="races", top="count", width=0.9, source=source_race,
            fill_color=factor_cmap("races", palette=custom_colors_race, factors=source_race.data["races"]), line_color="white")

        f_race.xgrid.grid_line_color = None
        f_race.xaxis.major_label_orientation = 0.8
        y_ticks = [0, 500, 1000, 5000, 20000]
        f_race.yaxis.ticker = FixedTicker(ticks=y_ticks)

        query_str_gender = f"""SELECT 
        SUM(female_count + Female_Gender_Identity_Count) as female,
        SUM(male_count + Male_Gender_Identity_Count) as male
        FROM
        demographic_counties
        WHERE
        county = '{city.title()}';"""

        with engine.connect() as connection:
            demographic_dataframe_gender = pd.read_sql_query(text(query_str_gender), connection)

        genders = ['female', 'male']
        custom_colors = ["purple", "orange"]

        count_genders = [str(demographic_dataframe_gender.loc[0, 'female']),
        str(demographic_dataframe_gender.loc[0,'male'])
        ]

        source_gender = ColumnDataSource(data={
        "genders": genders,
        "count": count_genders
        })

        f_gender = figure(x_range=genders, height=400, width=600,
        title="Genders", x_axis_label="Genders",
        y_axis_label="Number of the Gender")


        f_gender.vbar(x="genders", top="count", width=0.9, source=source_gender,
        fill_color=factor_cmap("genders", palette=custom_colors, factors=source_gender.data["genders"]), line_color="white")

        f_gender.xgrid.grid_line_color = None
        f_gender.xaxis.major_label_orientation = 1.2


        query_str_ethnicity = f"""SELECT SUM(Hispanic_or_Latinx_Count) as hispanicorlatin, SUM(Not_Hispanic_or_Latinx_Count) as nothispanciorlatin
        FROM demographic_counties 
        WHERE
        county = '{city.title()}';"""

        with engine.connect() as connection:
            demographic_dataframe_ethnicity = pd.read_sql_query(text(query_str_ethnicity), connection)

        ethnicities = ['Hispanic Or Latino', 'Non Hispanic - Non Latino']
        custom_colors_ethnicity = ["blue", "green"]

        count_ethnicities = [str(demographic_dataframe_ethnicity.loc[0, 'hispanicorlatin']),
        str(demographic_dataframe_ethnicity.loc[0,'nothispanciorlatin'])
        ]

        source_ethnicity = ColumnDataSource(data={
            "ethnicities": ethnicities,
            "count": count_ethnicities
        })

        f_ethnicity = figure(x_range=ethnicities, height=400, width=600,
        title="Ethnicities", x_axis_label="Ethnicities",
        y_axis_label="Number of the Ethnicity")
        
        f_ethnicity.vbar(x="ethnicities", top="count", width=0.9, source=source_ethnicity,
        fill_color=factor_cmap("ethnicities", palette=custom_colors_ethnicity, factors=source_ethnicity.data["ethnicities"]), line_color="white")

        f_ethnicity.xgrid.grid_line_color = None
        f_ethnicity.xaxis.major_label_orientation = 1.2

        show(f_ethnicity)
        show(f_gender)
        show(f_race)
    
    except exc.SQLAlchemyError as dbError:
        print ("PostgreSQL Error", dbError)
    finally:
        if engine in locals(): 
            engine.close()