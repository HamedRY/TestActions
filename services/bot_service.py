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
from enums import SubscriptionStatus, TimelineSteps
import utils
from cache_manager import CacheManger
from generator import TestGenerator
import pandas as pd

_internal_cache = CacheManger()
_test_generator = TestGenerator(__name__)

service_name = ServiceManager.get_service_name(__name__)
_celery = Celery(
    service_name,
    backend=ServiceManager.get_backend(service_name),
    broker=ServiceManager.get_broker(service_name))

@_celery.task
@_test_generator.create()
@_internal_cache.cached()
def get_articles(size, page, sort, order, name, article_id):
    query = {} if not name else {'en.title': {'$regex': '.*{}.*'.format(name), '$options': 'i'}}
    query = query if not article_id else {'_id': ObjectId(article_id)}

    total_elements = ApiAccess.get_revenyou_db().bots_store_articles.find(query).count()
    cursor = ApiAccess.get_revenyou_db().bots_store_articles.\
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

@_celery.task
@_test_generator.create()
def delete_article(name, language):
    payload = {
        'identifier': ApiAccess._revenyou_api_key_change_article,
        'article_name': name
    }
    if language != 'en':
        payload['iso_language'] = language

    response = requests.post('{}/v1/skynet/articles/delete'.format(ApiAccess._revenyou_test_api_base), json=payload)
    return response.json()

@_celery.task
@_test_generator.create()
def update_article(name, language, image, type_, title, intro, body, bots):
    payload = {
        'identifier': ApiAccess._revenyou_api_key_change_article,
        'article_name': name,
        'iso_language': language,
        'header_image': image,
        'article_type': type_,
        'title': title,
        'introduction': intro,
        'body': body,
        'bots': bots,
    }
    response = requests.post('{}/v1/skynet/articles/update'.format(ApiAccess._revenyou_test_api_base), json=payload)
    return response.json()