# celery.py or celery_app.py

from __future__ import absolute_import, unicode_literals

import os

from celery import Celery

from core import settings

# Set the default Django settings module for Celery
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')

# Create a Celery instance
app = Celery('core')  # Use your project name as the app name

# Load Celery configuration from Django settings
app.conf.broker_url = os.environ.get('CELERY_BROKER_URL', 'amqp://guest@localhost//')

# Load Celery configuration from Django settings
app.config_from_object('django.conf:settings', namespace='CELERY')
# Automatically discover tasks in all installed apps
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)
