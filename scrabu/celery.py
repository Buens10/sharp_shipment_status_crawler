import os
from celery import Celery


os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'scrabu.settings')

app = Celery('scrabu')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))