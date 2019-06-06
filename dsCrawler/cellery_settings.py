# -*- coding: utf-8 -*-
from celery import app

#the location of your Redis database
app.conf.broker_url = 'redis://:buens02100@localhost:6379/0'
#visibility timeout: defines the number of seconds to wait for the worker to acknowledge the task
#before the message is redelivered to another worker.
app.conf.broker_transport_options = {'visibility_timeout': 3600}  # 1 hour.
# backend to store the state and return values of tasks in Redis
app.conf.result_backend = 'redis://:buens02100@localhost:6379/0'
# setting a transport option to prefix the messages so that they will only be received
# by the active virtual host
app.conf.broker_transport_options = {'fanout_prefix': True}
# fanout option so that the workers may only subscribe to worker related events
app.conf.broker_transport_options = {'fanout_patterns': True}
# using the configs by calling this
app.config_from_object('celeryconfig')


