import os
import random
import time
from flask import Flask, request, render_template, session, flash, redirect, \
    url_for, jsonify
# from flask_mail import Mail, Message
from celery import Celery
from datetime import datetime, timedelta
from celery.schedules import crontab  # Import crontab schedule

app = Flask(__name__)
app.config['SECRET_KEY'] = 'top-secret!'

# Celery configuration
app.config['CELERY_BROKER_URL'] = os.environ.get("CELERY_BROKER_URL",'redis://localhost:6379/0')
app.config['result_backend'] = os.environ.get('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')


# Initialize extensions

# Initialize Celery
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

@celery.task(name="schedule_dynamic_tasks")
def schedule_dynamic_tasks():
    print("Triggered schedule_dynamic_tasks", datetime.now())
    data = fetch_data_task()
    for item in data:
        # metadata = item.get("metadata")  # Adjust based on your data structure
        # eta_time = datetime.now() + timedelta(minutes=item.get("schedule_time"))
        database_xls_file.apply_async()
        print("Triggered database_xls_file from schedule_dynamic_tasks", datetime.now())

@celery.task(name="database_xls_file")
def database_xls_file(*args):
    print("Started database_xls_file", datetime.now())
    print(args)
    return f"{args} completed successfully"

def fetch_data_task():
    print("Fetch data task started at", datetime.now())
    # dataset_files = DatasetV2File.objects.select_related("dataset").exclude(connection_details={}).all()
    data = [{"metadata1": "metadata", "schedule_time": 1}, {"metadata2": "metadata", "schedule_time": 1}]
    return data

if __name__ == '__main__':
    celery.conf.beat_schedule = {
        'run-database_xls_file': {
            'task': 'schedule_dynamic_tasks',
            'schedule': crontab(),  # Run every 1 minute
        },
    }
    
    # Run Celery Beat to schedule tasks
    celery.conf.timezone = 'UTC'  # Set the timezone
    celery.Beat().run()