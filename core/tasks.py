#tasks.py
import ast
import json
import os
from contextlib import closing
from datetime import datetime, timedelta
from venv import logger

import mysql.connector
import pandas as pd
import psycopg2
from celery import shared_task

from accounts.views import LOGGER
from core import settings
from core.constants import Constants
from datahub.models import DatasetV2, DatasetV2File
from datahub.serializers import DatasetFileV2NewSerializer
from participant.serializers import DatabaseDataExportSerializer
from utils import file_operations
from utils import file_operations as file_ops


@shared_task
def database_xls_file(file_id, connecton_details, dataset_name, file_path, dataset_id, source):
    """
    Export the data extracted from the database by reading the db config from cookies to a temporary location.
    """
    print("Task database_xls_file started")
    filter_data = connecton_details.pop("filter_data")
    database_type = connecton_details.pop("database_type")
    t_name = filter_data.get("table_name")
    col_names = filter_data.get("col")
    # col_names = ast.literal_eval(col_names)
    col_names = ", ".join(col_names)
    source = source
    file_path = file_path
    # remove database_type before passing it to db conn

    if database_type == Constants.SOURCE_MYSQL_FILE_TYPE:
        """Create a PostgreSQL connection object on valid database credentials"""
        LOGGER.info(f"Connecting to {database_type}")

        try:
            mydb = mysql.connector.connect(**connecton_details)
            mycursor = mydb.cursor()
            db_name = connecton_details["database"]
            mycursor.execute("use " + db_name + ";")

            query_string = f"SELECT {col_names} FROM {t_name} WHERE "
            sub_queries = []  # List to store individual filter sub-queries

            # filter_data = json.loads(serializer.data.get("filter_data")[0])
            # for query_dict in filter_data:
            #     column_name = query_dict.get('column_name')
            #     operation = query_dict.get('operation')
            #     value = query_dict.get('value')
            #     sub_query = f"{column_name} {operation} '{value}'"  # Using %s as a placeholder for the value
            #     sub_queries.append(sub_query)
            # query_string += " AND ".join(sub_queries)

            mycursor.execute(query_string)
            result = mycursor.fetchall()

            # save the list of files to a temp directory
            file_path = file_operations.create_directory(
                settings.DATASET_FILES_URL, [dataset_name, source])
            df = pd.read_sql(query_string, mydb)
            df = df.astype(str)
            df.to_excel(file_path)
            # instance = DatasetV2File.objects.create(
            #     dataset=dataset,
            #     source=source,
            #     file=os.path.join(dataset_name, source,
            #                         file_name + ".xls"),
            #     file_size=os.path.getsize(
            #         os.path.join(settings.DATASET_FILES_URL, dataset_name, source, file_name + ".xls")),
            #     standardised_file=os.path.join(
            #         dataset_name, source, file_name + ".xls"),
            # )
            # # result = os.listdir(file_path)
            # serializer = DatasetFileV2NewSerializer(instance)
            return 
        except mysql.connector.Error as err:
            LOGGER.error(err, exc_info=True)
            if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
                return False
            elif err.errno == mysql.connector.errorcode.ER_NO_SUCH_TABLE:
                return False
            elif str(err).__contains__("Unknown column"):
                return False
            return False

    elif database_type == Constants.SOURCE_POSTGRESQL_FILE_TYPE:
        """Create a PostgreSQL connection object on valid database credentials"""
        LOGGER.info(f"Connecting to {database_type}")
        print(connecton_details)
        try:
            with closing(psycopg2.connect(**connecton_details)) as conn:
                try:

                    query_string = f"SELECT {col_names} FROM {t_name} WHERE "
                    sub_queries = []  # List to store individual filter sub-queries
                    # filter_data = json.loads(serializer.data.get("filter_data")[0])
                    # for query_dict in filter_data:
                    #     column_name = query_dict.get('column_name')
                    #     operation = query_dict.get('operation')
                    #     value = query_dict.get('value')
                    #     sub_query = f"{column_name} {operation} '{value}'"  # Using %s as a placeholder for the value
                    #     sub_queries.append(sub_query)
                    # query_string += " AND ".join(sub_queries)
                    df = pd.read_sql(query_string, conn)
                    df = df.astype(str)
                except pd.errors.DatabaseError as error:
                    LOGGER.error(error, exc_info=True)
                    return False
            file_path = file_ops.create_directory(
                settings.DATASET_FILES_URL, [dataset_name, source])
            df.to_excel(file_path)
            # instance = DatasetV2File.objects.create(
            #     dataset=dataset,
            #     source=source,
            #     file=os.path.join(dataset_name, source,
            #                         file_name + ".xls"),
            #     file_size=os.path.getsize(
            #         os.path.join(settings.DATASET_FILES_URL, dataset_name, source, file_name + ".xls")),
            #     standardised_file=os.path.join(
            #         dataset_name, source, file_name + ".xls"),
            # )
            # # result = os.listdir(file_path)
            # serializer = DatasetFileV2NewSerializer(instance)
            return False

        except psycopg2.Error as error:
            LOGGER.error(error, exc_info=True)
