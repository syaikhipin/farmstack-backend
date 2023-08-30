# from celery.app.celery import make_celery


class Config:
    SQLALCHEMY_DATABASE_URI = "postgresql://postgrs:$farmstack@!21@db/postgres"
    # Other Flask configuration options

class C:
    broker_url = 'redis://redis_server:6379/0'
    result_backend = 'redis://redis_server:6379/0'
    # Other Celery configuration options
