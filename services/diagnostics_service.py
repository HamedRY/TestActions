from datetime import datetime
from celery import Celery
from api_access import ApiAccess
from pymongo import MongoClient
from service_manager import ServiceManager
from enums import Mode
import json
import os
import sys
sys.path.extend(['..', 'tests'])
from generator import TestGenerator

_test_generator = TestGenerator(__name__)

service_name = ServiceManager.get_service_name(__name__)
_celery = Celery(
    service_name,
    backend=ServiceManager.get_backend(service_name),
    broker=ServiceManager.get_broker(service_name))

@_celery.task
@_test_generator.create()
def get_diagnostics(begin, end):
    query = {}
    if begin and end:
        query['time'] = {'$gte': begin, '$lte': end}
    elif begin:
        query['time'] = {'$gte': begin}
    elif end:
        query['time'] = {'$lte': end}

    result = list(ApiAccess.get_skynet_db().backend_errors.\
        find(query, {'_id': 0}).\
        sort('_id', -1).\
        limit(100))
        
    return json.dumps(result)

@_celery.task
@_test_generator.create()
def get_audits(begin, end, user, max):
    query = {} if user is None or len(user) == 0 else {'username': user}
    if begin and end:
        query['time'] = {'$gte': begin, '$lte': end}
    elif begin:
        query['time'] = {'$gte': begin}
    elif end:
        query['time'] = {'$lte': end}

    result = list(ApiAccess.get_skynet_db().audit.\
        find(query, {'_id': 0}).\
        sort('_id', -1).\
        limit(int(max)))
        
    return json.dumps(result)

@_celery.task
def record_action(caller, username):
    if os.environ['APP_RUN_ENV'] == Mode.development.value:
        print('{} called {}'.format(username, caller))

    ApiAccess.get_skynet_db().audit.insert({
        'time': int(datetime.now().timestamp()),
        'username': (username or '-'),
        'caller': (caller or '-')
    })