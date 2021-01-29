import json
import http.client
from celery import Celery
from celery.schedules import crontab
from bson.objectid import ObjectId
import sys
sys.path.extend(['..', 'tests'])
from api_access import ApiAccess
from service_manager import ServiceManager
from datetime import datetime, timedelta, timezone
import time
from enums import TransactionType, OrderStatus
import pandas as pd
import utils
from cache_manager import CacheManger
from generator import TestGenerator
from pymongo import UpdateOne

_test_generator = TestGenerator(__name__)

service_name = ServiceManager.get_service_name(__name__)
_celery = Celery(
    service_name,
    backend=ServiceManager.get_backend(service_name),
    broker=ServiceManager.get_broker(service_name))

_max_limit = 1000

def get_last_saved_date():
    maybe_date = list(ApiAccess.get_skynet_db().first_deposit.\
        find({}, {'_id': 0, 'date_added': 1}).\
        sort('date_added', -1).\
        limit(1))
    if len(maybe_date) > 0:
        return maybe_date[0]['date_added']

def get_oldest_date():
    oldest_date = list(ApiAccess.get_revenyou_db().deposit.\
        find({}, {'_id': 0, 'date_added': 1}).\
        sort('date_added', 1).\
        limit(1))
    if len(oldest_date) > 0:
        return oldest_date[0]['date_added']

def get_last_date():
    last_date = list(ApiAccess.get_revenyou_db().deposit.\
        find({}, {'_id': 0, 'date_added': 1}).\
        sort('date_added', -1).\
        limit(1))
    if len(last_date) > 0:
        return last_date[0]['date_added']

def unordered_bulk_write(data):
    updates = []
    for item in data:
        updates.append(
            UpdateOne(
                {'_id': item['_id']}, 
                {'$setOnInsert': {
                    '_id': item['_id'], 
                    'quantity': item['quantity'], 
                    'date_added': item['date_added']}},
                upsert=True))

    ApiAccess.get_skynet_db().first_deposit.bulk_write(updates)

@_celery.task
@_test_generator.create()
def daily_new_deposit_aggregator():
    maybe_start = get_last_saved_date()
    start_date = maybe_start if maybe_start is not None else get_oldest_date()
    end_date = int((datetime.fromtimestamp(start_date) + timedelta(hours=24)).timestamp())

    pipeline = [
        {'$match': {
            'order_status': OrderStatus.approved.value,
            'bank': TransactionType.external.value,
            'date_added': {'$gte': start_date, '$lt': end_date}}},
        {'$group': {
            '_id': '$user_id',
            'quantity': {'$sum': {'$toDouble': '$quantity'}},
            'date_added': {'$first': '$date_added'}
        }}
    ]
    match_index = next((index for (index, stage) in enumerate(pipeline) if '$match' in stage), None)
    if match_index is None:
        raise KeyError('No $match provided')

    last_deposit_date = get_last_date()
    while True:
        data = list(ApiAccess.get_revenyou_db().deposit.aggregate(pipeline))
        if len(data) > 0:
            unordered_bulk_write(data)

        start_date = end_date
        end_date = int((datetime.fromtimestamp(start_date) + timedelta(hours=24)).timestamp())
        if start_date > last_deposit_date:
            break

        pipeline[match_index]['$match']['date_added'].update({'$gte': start_date, '$lt': end_date})

_celery.conf.beat_schedule = {
    'daily_new_deposit_aggregator_task': {
        'task': '{}.daily_new_deposit_aggregator'.format(__name__),
        'options': {'queue' : 'analytics'},
        'schedule': crontab(minute=0, hour=22, 
            day_of_week='*', day_of_month='*', month_of_year='*')
    }
}