import redis
import os
from celery import Celery
from celery import app

CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL', 'redis://:buens02100@localhost:6379/0'),
CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND', 'redis://:buens02100@localhost:6379/0')

app = Celery('tasks', broker=CELERY_BROKER_URL,
             result_backend=CELERY_RESULT_BACKEND
             )

@app.task
def crawl(domain_pk):
    from crawl import domain_crawl
    return domain_crawl(domain_pk)