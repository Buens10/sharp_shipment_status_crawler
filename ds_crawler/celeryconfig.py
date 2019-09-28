from celery import app

broker_url = 'redis://:buens02100@localhost:6379/0'
result_backend = 'redis://:buens02100@localhost:6379/0'

app.config_from_object(task_serializer='json',
                       result_serializer='json',
                       accept_content=['json'],
                       enable_utc=True)
