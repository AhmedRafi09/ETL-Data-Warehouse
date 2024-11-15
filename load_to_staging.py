#!/usr/bin/env python
# coding: utf-8

# /usr/bin/python3.8

"""Daily Active Memebr Snapshot- ODS ETL"""

# Setting up

import logging
import time
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text as sqltext
from datetime import date, datetime, timedelta
import pandas as pd
import configparser
import connections
from connections.db_connection import execute_dwh_sql, initialize_dwh

import os
import shutil
import psycopg2


logging.basicConfig(filename='F:\\Rafiul_Ahmed_20220620\\WT\\ETL\\ETL.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d  %H:%M:%S',
                    level=logging.DEBUG)



#table_name = 'fact_sales_details'
table_name = 'stg_fact_sales_details'

# SQL strings


today = date.today()
yesterday = today - timedelta(days=1)




sql_create_dwdb_table = """create table if not exists dwh.{table_name} 
                            (
                            order_id int,
                            order_date date,
                            customer_id int REFERENCES dwh.dim_customer(customer_id),
                            location_id int REFERENCES dwh.dim_location(location_id),
                            salesperson_id int REFERENCES dwh.dim_salesperson(salesperson_id), 
                            shipment_date Date,
                            ship_id int REFERENCES dwh.dim_ship(ship_id),
                            payment_type_id int REFERENCES dwh.dim_payment_type(payment_type_id),
                            product_id int REFERENCES dwh.dim_product(product_id),
                            quantity int,
                            revenue numeric(10,2),
                            shipping_fee numeric(10,2),
                            revenue_bins numeric(10,2)
                            );"""



sql_truncate_staging_table = """TRUNCATE TABLE stg_dwh.{table_name}"""

sql_copy_from_local_to_stgdb = """COPY stg_dwh.{table_name} FROM stdin WITH CSV HEADER DELIMITER as ','; """



#################### Specify input and output file, input directory and output directory here

input_file = 'Sales Data.xlsx'
output_file = 'sales_data_backup.csv'


data_directory = 'F:\\Rafiul_Ahmed_20220620\\WT\\ETL\\data\\'
backup_directory = 'F:\\Rafiul_Ahmed_20220620\\WT\\ETL\\backup\\'

def read_file_convert_csv():
# read an excel file and convert 
# into a dataframe object
    df = pd.DataFrame(pd.read_excel(data_directory+input_file))
    df.columns = ['order_id',	'order_date',	'customer_id',	'customer_name',	'city',	'state'	,'country',	'salesperson',	'region',
        'shipped_date',	'shipper_name',	'ship_name',	'ship_address',	'ship_city',	'ship_state',	'ship_country',	'payment_type',	
        'product_name'	,'category'	,'unit_price',	'quantity',	'revenue',	'shipping_fee',	'revenue_bins']
    df.to_csv(backup_directory+output_file, index=False)

def move_file_to_backup():
    shutil.move(data_directory+input_file, backup_directory+input_file)

final_file = backup_directory+output_file


def truncate_staging_table(table_name):
    db = initialize_dwh()
    # Truncate staging table
    execute_dwh_sql(db, sql_truncate_staging_table.format(
                        table_name=table_name
                    ))
    

def copy_data_to_staging(table_name):
    filename = 'F:\\Rafiul_Ahmed_20220620\\WT\\ETL\\database.ini'
    section = 'stg.db'

    parser = configparser.ConfigParser()
    # read config file
    parser.read(filename)
    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    try:
        params = db
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        # create a cursor
        cursor = conn.cursor()
        f = open(final_file, 'r', encoding="utf-8")
        copy_sql = """
           COPY stg_dwh.%s FROM stdin WITH CSV HEADER
           DELIMITER as ','
           """
        # f = open(f"{file_path}", 'r', encoding="utf-8")
        cursor.copy_expert(sql=copy_sql % table_name, file=f)
        conn.commit()
        print("Staging Table Populated")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def load_to_staging():
    try:
        logger = logging.getLogger('urbanGUI')

        # Step 1.1 : Read input file
        logger.info("Reading Input File")
        read_file_convert_csv()
        logger.info("File Read Complete & CSV Prepared to Upload")

        # Step 1.2 : Move input file to backup folder
        logger.info("Moving Input File to Backup Folder")
        move_file_to_backup()
        logger.info("File Moved")

        # Step 1.3: Truncate staging table
        logger.info("Truncating Staging Table")
        truncate_staging_table(table_name)
        logger.info("Staging Table Truncated")

        # Step 1.4 : Upload File To Staging Table
        logger.info("Uploading File to Staging Table")
        copy_data_to_staging(table_name)
        logger.info("File Uploaded")
    except ValueError as ve:
        return str(ve)