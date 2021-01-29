import json
import requests
from celery import Celery
from bson.objectid import ObjectId
import sys
sys.path.extend(['..', 'tests'])
import math
from api_access import ApiAccess
from service_manager import ServiceManager
from datetime import datetime
from enums import SubscriptionStatus
import utils
from cache_manager import CacheManger
from generator import TestGenerator

_internal_cache = CacheManger()
_test_generator = TestGenerator(__name__)

service_name = ServiceManager.get_service_name(__name__)
_celery = Celery(
    service_name,
    backend=ServiceManager.get_backend(service_name),
    broker=ServiceManager.get_broker(service_name))

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_feedbacks(size, page, sort, order, name):
    query = {} if not name else {'$or': 
                                [{'message': {'$regex': '.*{}.*'.format(name), '$options': 'i'}}, 
                                {'version': {'$regex': '.*{}.*'.format(name), '$options': 'i'}}]}
        
    total_elements = ApiAccess.get_revenyou_db().feedback.find(query).count()
    cursor = ApiAccess.get_revenyou_db().feedback.\
                find(query).\
                skip(page * size).\
                limit(size).\
                collation({'locale': 'en' })

    if order != '':
        cursor = cursor.sort(sort,  -1 if order == 'desc' else 1)

    result = {
        'total_elements': total_elements,
        'total_pages': math.ceil(float(total_elements)/size),
        'current_page': page,
        'content': []}

    for document in cursor:
        result['content'].append(document)
        result['content'][-1]['_id'] = str(result['content'][-1]['_id'])

    return result