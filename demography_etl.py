from dagster import job
from data_extract import *
from transform_and_load import *
from visualisation import *

@job
def etl():
    visualise(
        load(
            join(
                transform_demography(
                    extract_demography()
                ),
                transform_zipcodes(
                    extract_zipcodes()
                )
            )
        ),
        transform_crimes(
            extract_hateCrimes()
        )
    )