__author__ = 'Rafiul Ahmed'
__project__ = 'Sales DWH ETL'

import os
import sys
from configparser import ConfigParser, ExtendedInterpolation
import logging
from datetime import datetime, timedelta, date

import dimension_etl
from load_to_staging import load_to_staging
from dimension_etl.dim_customer import dim_customer
from dimension_etl.dim_location import dim_location
from dimension_etl.dim_product import dim_product
from fact_etl.fact_sales_details import fact_sales_details


logging.basicConfig(filename='F:\\Rafiul_Ahmed_20220620\\WT\\ETL\\ETL.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d  %H:%M:%S',
                    level=logging.DEBUG)



def main():
    try:
        logger = logging.getLogger('urbanGUI')
        logger.info("\n\n\nSales DWH ETL Start Time: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
        
        # Step 1 : Load Input File Into Staging Table
        logger.info("Loading Input File Into Staging Table")
        load_to_staging()

        logger.info("## Populating Dimension Start Time: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
        # Step 2 : Populate Dimension Tables
        # Step 2.1 : Populate dim_customer
        logger.info("Running DIM_CUSTOMER")
        dim_customer()

        # Step 2.2 : Populate dim_location
        logger.info("Running DIM_LOCATION")
        dim_location()

        # Step 2.3 : Populate dim_product
        logger.info("Running DIM_PRODUCT")
        dim_product()
        logger.info("## Dimension Process End Time: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))


        # logger.info("## Populating Fact Start Time: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
        # # Step 3 : Populate Fact Tables
        # logger.info("Running FACT_SALES_DETAILS")
        # fact_sales_details()
        # logger.info("## Fact Process End Time: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

        logger.info("Sales DWH ETL End Time: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
    
    except ValueError as ve:
        return str(ve)


if __name__ == "__main__":
    sys.exit(main())


       