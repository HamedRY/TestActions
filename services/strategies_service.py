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
import time
from collections import defaultdict
import statistics

_internal_cache = CacheManger()
_test_generator = TestGenerator(__name__)

service_name = ServiceManager.get_service_name(__name__)
_celery = Celery(
    service_name,
    backend=ServiceManager.get_backend(service_name),
    broker=ServiceManager.get_broker(service_name))

_max_limit = 1000
@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_csv_exportable(cols):
    global _max_limit
    strategies = []
    skip_ = 0
    while True:
        partial = list(ApiAccess.get_revenyou_db().signal_provider.\
            find({}, {'_id': 0}).\
            skip(skip_).\
            limit(_max_limit))
        skip_ += _max_limit
        if len(partial) == 0:
            break
        strategies.extend(partial)

    result = []
    for strategy in strategies:
        result.append({})        
        for col in cols:
            if col =='date_added':
                result[-1]['date_added'] = utils.utc_ts_to_local_str(strategy.get('date_added', None))
                continue
            if col =='date_updated':
                result[-1]['date_updated'] = utils.utc_ts_to_local_str(strategy.get('date_updated', None))
                continue

            result[-1][col] = strategy.get(col, '')
        
    return json.dumps(result)
    
@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_user_subscribed_strategies(user_id, size, page, sort, order, begin, end):
    query = {'user_id': user_id}

    if begin and end: 
        query['date_added'] = {'$gte': begin, '$lte': end}
    elif begin:
        query['date_added'] = {'$gte': begin}
    elif end:
        query['date_added'] = {'$lte': end}
        
    total_elements = ApiAccess.get_revenyou_db().subscribed.find(query).count()
    cursor = ApiAccess.get_revenyou_db().subscribed.\
                find(query).\
                skip(page * size).\
                limit(size).\
                collation({'locale': 'en' })

    if order != '':
        cursor = cursor.sort(sort,  -1 if order == 'desc' else 1)
    cursor = list(cursor)

    result = {
        'total_elements': total_elements,
        'total_pages': math.ceil(float(total_elements)/size),
        'current_page': page,
        'content': []}

    signal_providers = ApiAccess.get_revenyou_db().signal_provider.\
        find({'_id': \
            {'$in' : [ObjectId(doc.get('strategy_id', '')) for doc in cursor]}}, 
            {'name': 1, 'pair': 1, 'icon_url': 1}).\
        limit(size)

    strategy_info = {}
    for item in signal_providers:
        strategy_info[str(item['_id'])] = {
            'strategy_name' : item.get('name', ''),
            'strategy_pair' : item.get('pair', ''),
            'strategy_icon' : item.get('icon_url', '')
        }

    for document in cursor:
        result['content'].append(document)
        result['content'][-1]['_id'] = str(result['content'][-1]['_id'])
        result['content'][-1]['date_added'] = utils.utc_ts_to_local_str(document.get('date_added', None))
        result['content'][-1]['date_modified'] = utils.utc_ts_to_local_str(document.get('date_modified', None))
        strategy_id = document.get('strategy_id', None)
        if strategy_id is not None and strategy_id in strategy_info:
            result['content'][-1].update(strategy_info[strategy_id])
        
        pnl = (float(result['content'][-1]['updated_quantity']) - float(result['content'][-1]['quantity'])) / float(result['content'][-1]['quantity']) * 100 \
            if result['content'][-1]['updated_quantity'].isnumeric() else None
        result['content'][-1]['pnl'] = "{:.2f}%".format(pnl) if pnl else ''       

    return result

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_user_unsubscribed_strategies(user_id, size, page, sort, order, begin, end):
    query = {'user_id': user_id}

    if begin and end: 
        query['date_added'] = {'$gte': begin, '$lte': end}
    elif begin:
        query['date_added'] = {'$gte': begin}
    elif end:
        query['date_added'] = {'$lte': end}
        
    total_elements = ApiAccess.get_revenyou_db().unsubscribed.find(query).count()
    cursor = ApiAccess.get_revenyou_db().unsubscribed.\
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
        result['content'][-1]['date_added'] = utils.utc_ts_to_local_str(document.get('date_added', None))
        result['content'][-1]['date_modified'] = utils.utc_ts_to_local_str(document.get('date_modified', None))
        result['content'][-1]['date_subscribed'] = utils.utc_ts_to_local_str(document.get('date_subscribed', None))
        balance = float(result['content'][-1]['deposit_balance'])
        result['content'][-1]['pnl'] = '{:.2f}%'.format(
            (float(result['content'][-1]['profit']) + balance - balance) / balance * 100)

    return result

@_celery.task
@_test_generator.create()
def get_unsub_value(user_id):
    pipeline = [
        {'$match': {'user_id': user_id}},
        {'$group': {
            '_id': '',
            'total': {'$sum': {'$toDouble': '$deposit_balance'}},
            'change': {'$sum': {'$toDouble': '$profit'}},
        }},
        {'$addFields': {
            'value': {'$add': ['$total', '$change']}
        }},
        {'$project': {'_id': 0, 'value': 1}}
    ]
    result = list(ApiAccess.get_revenyou_db().unsubscribed.aggregate(pipeline))
    return result[0].get('value', 0) if len(result) > 0 else 0

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_funded_strategies():
    global _max_limit
    pipeline = [
        {'$match': {'status': SubscriptionStatus.active.value}},
        {'$skip': 0},
        {'$limit': _max_limit},
        {'$project': {
            '_id': 0,
            'strategy_id': 1,
            'quantity_in_euro': {'$toDouble': '$quantity_euro'},
            'quantity_in_base': {'$toDouble': '$quantity'}
        }},
        {'$group': {
            '_id': '$strategy_id',
            'subscribers': {'$sum': 1},
            'quantity_in_euro': {'$sum': '$quantity_in_euro'},
            'quantity_in_base': {'$sum': '$quantity_in_base'},
            'quantity_avg_euro': {'$avg': '$quantity_in_euro'}
        }}
    ]
    match_index = next((index for (index, stage) in enumerate(pipeline) if '$match' in stage), None)
    sub_count = ApiAccess.get_revenyou_db().subscribed.\
        find(pipeline[match_index]['$match']).count()

    skip_index = next((index for (index, stage) in enumerate(pipeline) if '$skip' in stage), None)
    if skip_index is None:
        raise KeyError('No $skip provided')

    raw_result = []
    while pipeline[skip_index]['$skip'] < sub_count:
        partial_result = list(ApiAccess.get_revenyou_db().subscribed.aggregate(pipeline))
        pipeline[skip_index]['$skip'] += _max_limit
        if len(partial_result) == 0:
            continue
        raw_result.extend(partial_result)

    if len(raw_result) == 0:
        return raw_result

    return pd.DataFrame(raw_result).\
        groupby('_id').\
        agg({'subscribers': 'sum', 'quantity_in_euro': 'sum', 'quantity_in_base': 'sum', 'quantity_avg_euro': 'mean'}).\
        to_dict('index')
    

_long_cache_timeout = 28800 #8hours

@_celery.task
@_internal_cache.cached(expiration_s=_long_cache_timeout)
@_test_generator.create()
def group_by_team(strategies):
    sp_data = list(ApiAccess.get_revenyou_db().signal_provider.find(
        {'_id': {'$in': [ObjectId(_id) for _id in strategies.keys()]}}, {'team_name': 1, 'name': 1, 'user_id': 1, 'market_cap_eur': 1}))

    result = defaultdict(list)
    for info in sp_data:
        user_id = info.get('user_id', 'unknown')
        strategy_id = str(info.get('_id', ''))
        if user_id not in result:
            result[user_id] = {'bots': [], 'names': set()}

        result[user_id]['bots'].append({
            'strategy_id': strategy_id,
            'strategy_name': info.get('name', ''),
            'market_cap_eur': info.get('market_cap_eur', '')
        })
        result[user_id]['names'].add(info.get('team_name', 'unknown')) 

        result[user_id]['bots'][-1].update(strategies.get(strategy_id, {}))

    for team, info in result.items():
        info['names'] = list(info['names'])
        info['total_amount'] = sum([amount['quantity_in_euro'] for amount in info['bots']])
        info['total_count'] = sum([amount['subscribers'] for amount in info['bots']])
        info['total_avg'] = statistics.mean([amount['quantity_avg_euro'] for amount in info['bots']])
        info['total_base'] = sum([amount['quantity_in_base'] for amount in info['bots']])


    return result

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_strategy_info(strategy_id):
    if strategy_id is None:
        raise KeyError('None strategy id passed')
    return ApiAccess.get_revenyou_db().signal_provider.find_one({'_id': ObjectId(strategy_id)}, {'_id': False})

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def maybe_strategy_id(archive_id):
    cursor = ApiAccess.get_revenyou_db().subscribed_archived.\
        find_one({'_id': ObjectId(archive_id)}, {'_id': 0, 'strategy_id': 1})
    
    return None if cursor is None else cursor.get('strategy_id', None) 

_short_cache_timeout = 60 #1min

@_celery.task
@_internal_cache.cached(expiration_s=_short_cache_timeout)
@_test_generator.create()
def get_strategies(size, page, sort, order, begin, end, name, strategy_id):
    query = {} if not name else {'$or': 
                                [{'name': {'$regex': '.*{}.*'.format(name), '$options': 'i'}}, 
                                {'team_name': {'$regex': '.*{}.*'.format(name), '$options': 'i'}},
                                {'description': {'$regex': '.*{}.*'.format(name), '$options': 'i'}}, 
                                {'key': {'$regex': '.*{}.*'.format(name), '$options': 'i'}}]}
    query = query if not strategy_id else {'_id': ObjectId(strategy_id)}

    if begin and end: 
        query['date_added'] = {'$gte': begin, '$lte': end}
    elif begin:
        query['date_added'] = {'$gte': begin}
    elif end:
        query['date_added'] = {'$lte': end}
        
    total_elements = ApiAccess.get_revenyou_db().signal_provider.find(query).count()
    cursor = ApiAccess.get_revenyou_db().signal_provider.\
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
        result['content'][-1]['date_added'] = utils.utc_ts_to_local_str(document.get('date_added', None))
        result['content'][-1]['date_updated'] = utils.utc_ts_to_local_str(document.get('date_updated', None))

    return result

@_celery.task
@_test_generator.create()
def change_bot_description(name, description, language):
    payload = {
        'name': name,
        'description': description,
        'iso_language': language,
        'identifier': ApiAccess._revenyou_api_key_bot_desc
    }
    response = requests.post('{}/marketing/update/description'.format(ApiAccess._revenyou_api_base), json=payload)
    return response.json()

@_celery.task
@_test_generator.create()
def import_bot_description(data):
    responses = []
    for item in data:
        if not all(col in item for col in ('bot', 'language', 'description')):
            continue
        responses.append(change_bot_description(item['bot'], item['description'], item['language'].lower()))
        time.sleep(0.01)
    return json.dumps(responses)

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_customer_subscribed_snapshot(user_id):
    sub_cursor = ApiAccess.get_revenyou_db().subscribed.find(
        {'user_id': user_id},
        {'_id': 0, 'quantity': 1, 'strategy_id': 1, 'status': 1, 'date_added': 1})
    
    result = []
    for document in sub_cursor:
        strategy_name = ApiAccess.get_revenyou_db().signal_provider.find_one(
            {'_id': ObjectId(document.get('strategy_id', ''))}).get('name', '')
        date_time = utils.utc_ts_to_local_str(document.get('date_added', None)).split()
        result.append({
            'date' : date_time[0],
            'time' : (date_time[1] if len(date_time) > 1 else ''),
            'operation': TimelineSteps.bot_subscription.value['operation'],
            'description': '{}: {}/{}'.format(
                TimelineSteps.bot_subscription.value['description'], 
                strategy_name, document.get('quantity', '')),
            'status': document.get('status', SubscriptionStatus.unknown.value)
        })

    return json.dumps(result)

@_celery.task
@_test_generator.create()
def get_customer_unsubscribed_snapshot(user_id):
    unsub_cursor = ApiAccess.get_revenyou_db().unsubscribed.find(
        {'user_id': user_id},
        {'_id': 0, 'bot_name': 1, 'date_added': 1})

    result = []
    for document in unsub_cursor:
        date_time = utils.utc_ts_to_local_str(document.get('date_added', None)).split()
        result.append({
            'date' : date_time[0],
            'time' : (date_time[1] if len(date_time) > 1 else ''),
            'operation': TimelineSteps.bot_unsubscription.value['operation'],
            'description': '{}: {}'.format(
                TimelineSteps.bot_unsubscription.value['description'], 
                document.get('bot_name', '')),
            'status': SubscriptionStatus.active.value
        })

    return json.dumps(result)

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_active_strategy_names():
    names = list(ApiAccess.get_revenyou_db().signal_provider.find(
        {'stage': 'active'},
        {'_id': 1, 'name': 1}))
    
    for name in names:
        name['_id'] = str(name['_id'])
    return json.dumps(names)

@_celery.task
def change_bot_stage(name):
    payload = {
        'provider': name,
    }
    response = requests.post('{}/v1/skynet/provider/switchToProduction'.format(ApiAccess._revenyou_test_api_base), json=payload)
    return response.json()