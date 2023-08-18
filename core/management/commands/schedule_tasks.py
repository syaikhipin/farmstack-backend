# yourapp/management/commands/add_dynamic_tasks.py

from datetime import datetime, timedelta

from celery.schedules import crontab
from django.core.management.base import BaseCommand

from core.celery import app  # Import your Celery instance
from core.tasks import database_xls_file
from datahub.models import DatasetV2File, DatasetV2FileReload  # Import your model


class Command(BaseCommand):
    help = 'Add dynamic tasks from usage_policy'

    def handle(self, *args, **options):
        dataset_files = DatasetV2File.objects.select_related("dataset").exclude(connection_details={}).all()
        print(F"{len(dataset_files)} dataset files going to update at: ", datetime.now())
        for files in dataset_files:
            interval = files.connection_details.get('interval')  # Get interval from config
            task_name = f'fetch_data_{files.id}'  # Generate a unique task name
            
            # Generate a unique task name using file_id
            task_name = f'fetch_data_{files.id}'

            # Add the task to Celery Beat schedule with the calculated interval
            app.conf.beat_schedule[task_name] = {
                'task': task_name,
                'schedule': timedelta(seconds=10),
                'args': [files.id, files.connection_details, files.dataset, files.standardised_file, files.dataset, files.source],  # Pass DatasetV2File id as an argument
            }
            print(f"Task added to scheduler: {task_name}")
            app.conf.beat_schedule_changed = True
            for task_name, task_schedule in app.conf.beat_schedule.items():
                print(f"Task Name: {task_name}, Schedule: {task_schedule.get('schedule')}")
            self.stdout.write(self.style.SUCCESS(f'Added dynamic task: {task_name}'))




# # celery.py or celery_app.py

# from __future__ import absolute_import, unicode_literals

# import os

# from celery import Celery

# # Set the default Django settings module for Celery
# os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')

# # Create a Celery instance
# app = Celery('core')  # Use your project name as the app name

# # Load Celery configuration from Django settings
# app.config_from_object('django.conf:settings', namespace='CELERY')

# # Automatically discover tasks in all installed apps
# app.autodiscover_tasks()


# #tasks.py
# import ast
# import json
# import os
# from contextlib import closing
# from datetime import datetime, timedelta
# from venv import logger

# import mysql.connector
# import pandas as pd
# import psycopg2
# from celery import shared_task

# from accounts.views import LOGGER
# from core import settings
# from core.constants import Constants
# from datahub.models import DatasetV2, DatasetV2File
# from datahub.serializers import DatasetFileV2NewSerializer
# from participant.serializers import DatabaseDataExportSerializer
# from utils import file_operations
# from utils import file_operations as file_ops


# @shared_task
# def database_xls_file(file_id, connecton_details, dataset_name, file_path, dataset_id, source):
#     """
#     Export the data extracted from the database by reading the db config from cookies to a temporary location.
#     """
#     print("Task started")
#     filter_data = connecton_details.pop("filter_data")
#     database_type = connecton_details.pop("database_type")
#     t_name = filter_data.get("table_name")
#     col_names = filter_data.get("col")
#     # col_names = ast.literal_eval(col_names)
#     col_names = ", ".join(col_names)
#     source = source
#     file_path = file_path
#     # remove database_type before passing it to db conn

#     if database_type == Constants.SOURCE_MYSQL_FILE_TYPE:
#         """Create a PostgreSQL connection object on valid database credentials"""
#         LOGGER.info(f"Connecting to {database_type}")

#         try:
#             mydb = mysql.connector.connect(**connecton_details)
#             mycursor = mydb.cursor()
#             db_name = connecton_details["database"]
#             mycursor.execute("use " + db_name + ";")

#             query_string = f"SELECT {col_names} FROM {t_name} WHERE "
#             sub_queries = []  # List to store individual filter sub-queries

#             # filter_data = json.loads(serializer.data.get("filter_data")[0])
#             # for query_dict in filter_data:
#             #     column_name = query_dict.get('column_name')
#             #     operation = query_dict.get('operation')
#             #     value = query_dict.get('value')
#             #     sub_query = f"{column_name} {operation} '{value}'"  # Using %s as a placeholder for the value
#             #     sub_queries.append(sub_query)
#             # query_string += " AND ".join(sub_queries)

#             mycursor.execute(query_string)
#             result = mycursor.fetchall()

#             # save the list of files to a temp directory
#             file_path = file_operations.create_directory(
#                 settings.DATASET_FILES_URL, [dataset_name, source])
#             df = pd.read_sql(query_string, mydb)
#             df = df.astype(str)
#             df.to_excel(file_path)
#             # instance = DatasetV2File.objects.create(
#             #     dataset=dataset,
#             #     source=source,
#             #     file=os.path.join(dataset_name, source,
#             #                         file_name + ".xls"),
#             #     file_size=os.path.getsize(
#             #         os.path.join(settings.DATASET_FILES_URL, dataset_name, source, file_name + ".xls")),
#             #     standardised_file=os.path.join(
#             #         dataset_name, source, file_name + ".xls"),
#             # )
#             # # result = os.listdir(file_path)
#             # serializer = DatasetFileV2NewSerializer(instance)
#             return 
#         except mysql.connector.Error as err:
#             LOGGER.error(err, exc_info=True)
#             if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
#                 return False
#             elif err.errno == mysql.connector.errorcode.ER_NO_SUCH_TABLE:
#                 return False
#             elif str(err).__contains__("Unknown column"):
#                 return False
#             return False

#     elif database_type == Constants.SOURCE_POSTGRESQL_FILE_TYPE:
#         """Create a PostgreSQL connection object on valid database credentials"""
#         LOGGER.info(f"Connecting to {database_type}")
#         print(connecton_details)
#         try:
#             with closing(psycopg2.connect(**connecton_details)) as conn:
#                 try:

#                     query_string = f"SELECT {col_names} FROM {t_name} WHERE "
#                     sub_queries = []  # List to store individual filter sub-queries
#                     # filter_data = json.loads(serializer.data.get("filter_data")[0])
#                     # for query_dict in filter_data:
#                     #     column_name = query_dict.get('column_name')
#                     #     operation = query_dict.get('operation')
#                     #     value = query_dict.get('value')
#                     #     sub_query = f"{column_name} {operation} '{value}'"  # Using %s as a placeholder for the value
#                     #     sub_queries.append(sub_query)
#                     # query_string += " AND ".join(sub_queries)
#                     df = pd.read_sql(query_string, conn)
#                     df = df.astype(str)
#                 except pd.errors.DatabaseError as error:
#                     LOGGER.error(error, exc_info=True)
#                     return False
#             file_path = file_ops.create_directory(
#                 settings.DATASET_FILES_URL, [dataset_name, source])
#             df.to_excel(file_path)
#             # instance = DatasetV2File.objects.create(
#             #     dataset=dataset,
#             #     source=source,
#             #     file=os.path.join(dataset_name, source,
#             #                         file_name + ".xls"),
#             #     file_size=os.path.getsize(
#             #         os.path.join(settings.DATASET_FILES_URL, dataset_name, source, file_name + ".xls")),
#             #     standardised_file=os.path.join(
#             #         dataset_name, source, file_name + ".xls"),
#             # )
#             # # result = os.listdir(file_path)
#             # serializer = DatasetFileV2NewSerializer(instance)
#             return False

#         except psycopg2.Error as error:
#             LOGGER.error(error, exc_info=True)
