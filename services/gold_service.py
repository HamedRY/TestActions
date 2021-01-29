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
from enums import TransactionType
import pandas as pd
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

_gold_threshold = 10000
_max_limit = 1000

@_celery.task
@_test_generator.create()
def get_new_gold_accounts():
    delta = timedelta(hours=24)

    pipeline = [
        {'$sort': {'_id': 1}},
        {'$match': {'bank': TransactionType.external.value}},
        {'$group': {'_id': '$user_id',
                    'deposit_sum': {'$sum': {'$toDouble': '$quantity'}},
                    'withdrawals': {'$last': '$withdrawals'},
                    'last_deposit': {'$last': '$date_added'},
                    'traverse_id': {'$last': '$_id'}
        }},
        {'$unwind': {
            'path': '$withdrawals', 
            'preserveNullAndEmptyArrays': True
        }},
        {'$group': {'_id': '$_id',
            'deposit_sum': {'$last': '$deposit_sum'},
            'withdrawal_sum': {'$sum': {'$toDouble': '$withdrawals.withdrawal_quantity'}},
            'last_deposit': {'$last': '$last_deposit'},
            'last_withdrawal': {'$last': '$withdrawals.date_added'},
            'traverse_id': {'$last': '$traverse_id'}
        }},
        {'$addFields': {
            'asset': {'$subtract': ['$deposit_sum', '$withdrawal_sum']}
        }},
        {'$sort': {'traverse_id': 1}}
    ]

    match_index = next((index for (index, stage) in enumerate(pipeline) if '$match' in stage), None)
    if match_index is None:
        raise KeyError('No $match provided')

    docs = []
    while True:
        partial_result = list(ApiAccess.get_skynet_db().deposit_withdrawal.aggregate(pipeline))
        if len(partial_result) == 0:
            break
        pipeline[match_index]['$match'].update({'_id': {'$gt': partial_result[-1].get('traverse_id', '')}})
        docs.extend(partial_result)

    prev_gold_members = []
    skip_ = 0
    while True:
        partial = list(ApiAccess.get_skynet_db().gold_members.\
            find({}, {'_id': 1}).\
            skip(skip_).\
            limit(_max_limit))
        skip_ += _max_limit
        if len(partial) == 0:
            break
        prev_gold_members.extend(partial)


    df = pd.DataFrame(docs).\
        groupby('_id', as_index=False).\
        agg({
            'asset': 'sum',
            'last_deposit': lambda x: x.tail(1),
            'last_withdrawal': lambda x: x.tail(1)})

    df = df[(df.asset >= _gold_threshold)]
    df_records = df[['_id', 'last_deposit', 'last_withdrawal', 'asset']]
    record_data = df_records.set_index('_id').to_dict()
    df_ids = df[['_id']]
    curr_gold_members = df_ids.to_dict('records')

    new_gold_members = utils.subtract_datasets(curr_gold_members, prev_gold_members)
    lost_gold_members = utils.subtract_datasets(prev_gold_members, curr_gold_members)

    observation_date = datetime.now().strftime('%Y-%m-%d')
    if len(new_gold_members) > 0:
        ApiAccess.get_skynet_db().gold_members.insert_many([
            {'_id': _id['_id'], 
            'last_deposit': record_data['last_deposit'][_id['_id']],
            'last_withdrawal': record_data['last_withdrawal'][_id['_id']],
            'asset': record_data['asset'][_id['_id']],
            'observation_date': observation_date} for _id in new_gold_members])
        _internal_cache.set_new_gold_members(json.dumps({'content': new_gold_members}), int(delta.total_seconds()))

    if len(lost_gold_members) > 0:
        ApiAccess.get_skynet_db().gold_members.remove(
            {'_id': {'$in': [member['_id'] for member in lost_gold_members]}})
        _internal_cache.set_lost_gold_members(json.dumps({'content': lost_gold_members}), int(delta.total_seconds()))


@_celery.task
@_test_generator.create()
def get_gold_ids(begin, end, order='desc'):
    date_filter = {}
    if begin and end: 
        date_filter = {'last_deposit': {'$gte': begin, '$lte': end}}
    elif begin:
        date_filter = {'last_deposit': {'$gte': begin}}
    elif end:
        date_filter = {'last_deposit': {'$lte': end}}

    gold_members = []
    skip_ = 0
    while True:
        partial = list(ApiAccess.get_skynet_db().gold_members.\
            find(date_filter).\
            sort('observation_date', (-1 if order == 'desc' else 1)).\
            skip(skip_).\
            limit(_max_limit))
        skip_ += _max_limit
        if len(partial) == 0:
            break
        gold_members.extend(partial)

    return {'content': gold_members}

@_celery.task
@_test_generator.create()
def get_new_gold_ids():
    new_ids = _internal_cache.get_new_gold_members()
    return json.loads(new_ids) if new_ids is not None else {'content': []}

@_celery.task
@_test_generator.create()
def get_lost_gold_ids():
    old_ids = _internal_cache.get_lost_gold_members()
    return json.loads(old_ids) if old_ids is not None else {'content': []}

_top_max = 20
@_celery.task
@_test_generator.create()
def get_top_ids():
    ids = list(ApiAccess.get_skynet_db().gold_members.\
            find({}).\
            sort('asset', -1).\
            limit(_top_max))

    return {'content': ids}

_celery.conf.beat_schedule = {
    'get_new_gold_accounts_task': {
        'task': '{}.get_new_gold_accounts'.format(__name__),
        'options': {'queue' : 'gold'},
        'schedule': crontab(minute=0, hour=23, 
            day_of_week='*', day_of_month='*', month_of_year='*')
    }
}