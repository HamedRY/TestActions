import json
import requests
from enums import Role
from celery import Celery
from urllib import parse
import pycountry
import pycountry_convert
from bson.objectid import ObjectId
import sys
sys.path.extend(['..', 'tests'])
import math
from api_access import ApiAccess
from service_manager import ServiceManager
from datetime import datetime, timedelta
from enums import OrderStatus, TransactionType, TimelineSteps, DepositStage
import decimal
import utils
from cache_manager import CacheManger
from generator import TestGenerator
from collections import Counter
import pandas as pd

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
def get_deposits(user_id, size, page, sort, order, begin, end, name):
    query = {'user_id': user_id} if user_id else {}

    if name:
        query.update({'$or': 
            [{'bank_name': {'$regex': '.*{}.*'.format(name), '$options': 'i'}}, 
            {'strategy_name': {'$regex': '.*{}.*'.format(name), '$options': 'i'}},
            {'user_id': {'$regex': '.*{}.*'.format(name), '$options': 'i'}}]})

    if begin and end: 
        query['date_added'] = {'$gte': begin, '$lte': end}
    elif begin:
        query['date_added'] = {'$gte': begin}
    elif end:
        query['date_added'] = {'$lte': end}
        
    total_elements = ApiAccess.get_revenyou_db().deposit.find(query).count()
    cursor = ApiAccess.get_revenyou_db().deposit.\
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

    return json.dumps(result)


@_celery.task
@_test_generator.create()
def get_withdraws(user_id, size, page, sort, order, begin, end, name, role, failed):
    query = {'user_id': user_id} if user_id else {}

    if name:
        query.update({'$or': 
            [{'holder_name': {'$regex': '.*{}.*'.format(name), '$options': 'i'}}, 
            {'account_number': {'$regex': '.*{}.*'.format(name), '$options': 'i'}},
            {'user_id': {'$regex': '.*{}.*'.format(name), '$options': 'i'}}]})
            
    if begin and end: 
        query['date_added'] = {'$gte': begin, '$lte': end}
    elif begin:
        query['date_added'] = {'$gte': begin}
    elif end:
        query['date_added'] = {'$lte': end}
        
    db = ApiAccess.get_revenyou_db().withdrawal if not failed \
        else ApiAccess.get_skynet_db().failed_withdrawals
    total_elements = db.find(query).count()
    cursor = db.\
                find(query).\
                skip(page * size).\
                limit(size).\
                collation({'locale': 'en'})

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
        if role and role.upper() == Role.support.value:
            result['content'][-1]['account_number'] = ''.join(['*' if idx in list(range(8, 14)) else elem\
                 for idx, elem in enumerate(document.get('account_number', ''))])

    return json.dumps(result)

_short_cache_timeout = 60 #1min

@_celery.task
@_test_generator.create()
@_internal_cache.cached(expiration_s=_short_cache_timeout)
def get_withdraw(withdraw_id):
    cursor = list(ApiAccess.get_revenyou_db().withdrawal.\
                find({'_id': ObjectId(withdraw_id)}))

    if len(cursor) == 0:
        return {}

    withdrawal = cursor[0] 
    withdrawal['_id'] = str(withdrawal['_id'])
    withdrawal['date_added'] = utils.utc_ts_to_local_str(withdrawal.get('date_added', None))
    withdrawal['date_modified'] = utils.utc_ts_to_local_str(withdrawal.get('date_modified', None))

    return withdrawal

@_celery.task
@_test_generator.create()
@_internal_cache.cached(expiration_s=_short_cache_timeout)
def get_deposit(deposit_id):
    cursor = list(ApiAccess.get_revenyou_db().deposit.\
                find({'_id': ObjectId(deposit_id)}))

    if len(cursor) == 0:
        return {}

    deposit = cursor[0] 
    deposit['_id'] = str(deposit['_id'])
    deposit['date_added'] = utils.utc_ts_to_local_str(deposit.get('date_added', None))
    deposit['date_modified'] = utils.utc_ts_to_local_str(deposit.get('date_modified', None))

    transaction_id = deposit.get('transaction_id', None)
    if transaction_id is None:
        return deposit
        
    cursor = list(ApiAccess.get_revenyou_db().transaction.\
                find({'_id': ObjectId(transaction_id)}))

    if len(cursor) != 0:
        transaction = cursor[0] 
        transaction['ext_trans_id'] = transaction.get('transaction_id', '')
        del transaction['_id']
        del transaction['user_id']
        deposit.update(transaction)

    return deposit

_max_limit = 1000
@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_funded_accounts(begin=None, end=None):
    new_payed_pipeline = [{'$match': {'order_status': OrderStatus.approved.value}}]
    old_payed_pipeline = new_payed_pipeline.copy()

    if begin and end:
        old_payed_pipeline.append({'$match': {'date_added': {'$lt': begin}}})
        new_payed_pipeline.append({'$match': {'date_added': {'$gte': begin, '$lte': end}}})
    elif begin:
        old_payed_pipeline.append({'$match': {'date_added': {'$lt': begin}}})
        new_payed_pipeline.append({'$match': {'date_added': {'$gte': begin}}})
    elif end:
        new_payed_pipeline.append({'$match': {'date_added': {'$lte': end}}})

    new_payed_pipeline.extend([
        {'$skip': 0}, 
        {'$limit': _max_limit}, 
        {'$group': {'_id': '$user_id'}}])

    skip_index = next((index for (index, stage) in enumerate(new_payed_pipeline) if '$skip' in stage), None)
    if skip_index is None:
        raise KeyError('No $skip provided')
    new_payed_users = []
    while True:
        partial_result = list(ApiAccess.get_revenyou_db().deposit.aggregate(new_payed_pipeline))
        if len(partial_result) == 0:
            break
        new_payed_pipeline[skip_index]['$skip'] += _max_limit
        new_payed_users.extend(partial_result)

    if len(old_payed_pipeline) == 0:
        return {
            'total_elements': len(new_payed_users),
            'content': new_payed_users
        }

    old_payed_pipeline.extend([
        {'$match': {'user_id': {'$in': [doc['_id'] for doc in new_payed_users if '_id' in doc]}}},
        {'$skip': 0}, 
        {'$limit': _max_limit},
        {'$group': {'_id': '$user_id'}}
    ])
    skip_index = next((index for (index, stage) in enumerate(old_payed_pipeline) if '$skip' in stage), None)
    if skip_index is None:
        raise KeyError('No $skip provided')
    
    old_payed_users = []
    while True:
        partial_result = list(ApiAccess.get_revenyou_db().deposit.aggregate(old_payed_pipeline))
        if len(partial_result) == 0:
            break
        old_payed_pipeline[skip_index]['$skip'] += _max_limit
        old_payed_users.extend(partial_result)

    funded_users = utils.subtract_datasets(new_payed_users, old_payed_users)
    funded_users = sorted(funded_users, key=lambda k: k['_id'], reverse=True)

    return {
        'total_elements': len(funded_users),
        'content': funded_users
    }

_min_balance = 15
@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_funded_accounts_count(begin=None, end=None):
    if begin and end: 
        pipeline = [{'$match': {'date_added': {'$gte': begin, '$lte': end}}}]
    elif begin:
        pipeline = [{'$match': {'date_added': {'$gte': begin}}}]
    elif end:
        pipeline = [{'$match': {'date_added': {'$lte': end}}}]
    else:
        pipeline = []

    pipeline.extend([
        {'$sort': {'_id': 1}},
        {'$match': {'bank': TransactionType.external.value}},
        {'$limit': _max_limit}, 
        {'$group': {'_id': '$user_id',
                    'deposit_sum': {'$sum': {'$toDouble': '$quantity'}},
                    'withdrawals': {'$last': '$withdrawals'},
                    'traverse_id': {'$last': '$_id'}
        }},
        {'$unwind': {
            'path': '$withdrawals', 
            'preserveNullAndEmptyArrays': True
        }},
        {'$group': {'_id': '$_id',
            'deposit_sum': {'$last': '$deposit_sum'},
            'withdrawal_sum': {'$sum': {'$toDouble': '$withdrawals.quantity_in_fiat'}},
            'traverse_id': {'$last': '$traverse_id'}
        }},
        {'$addFields': {
            'asset': {'$subtract': ['$deposit_sum', '$withdrawal_sum']}
        }},
        {'$sort': {'traverse_id': 1}},
    ])

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

    docs = (pd.DataFrame(docs)\
        .groupby('_id', as_index=False)['asset'].sum()\
        .to_dict('records'))

    result = [
        {'_id': 'Funded accounts', 'count': sum(1 for doc in docs if doc['asset'] >= _min_balance)}, 
        {'_id': 'Zero accounts', 'count': sum(1 for doc in docs if doc['asset'] < _min_balance)}
    ]
    return result

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_lost_accounts_count(begin=None, end=None):
    result = [{'_id': 'Lost accounts', 'count': 0}]
    if begin and end: 
        period_begin = (datetime.fromtimestamp(begin) - timedelta(days=30)).timestamp()
        period_end = (datetime.fromtimestamp(end) - timedelta(days=30)).timestamp()
        time_pipe = {'$gte': period_begin, '$lte': period_end}
    elif begin:
        period_begin = (datetime.fromtimestamp(begin) - timedelta(days=30)).timestamp()
        period_end = (datetime.now() - timedelta(days=30)).timestamp()
        time_pipe = {'$gte': period_begin, '$lte': period_end}
    elif end:
        period_end = (datetime.fromtimestamp(end) - timedelta(days=30)).timestamp()
        time_pipe = {'$lte': period_end}
    else:
        period_end = (datetime.now() - timedelta(days=30)).timestamp()
        time_pipe = {'$lte': period_end}

    pipeline = [
        {'$skip': 0}, 
        {'$limit': _max_limit}, 
        {'$match': {'date_added': time_pipe}},
        {'$sort': {'_id': 1}},
        {'$group': {'_id': '$user_id',
                    'withdrawal_sum': {'$sum': {'$toDouble': '$quantity_in_fiat'}},
                    'deposits': {'$last': '$deposits'}
        }},
        {'$unwind': '$deposits'},
        {'$match': {'deposits.bank': TransactionType.external.value}},
        {'$group': {'_id': '$_id',
                    'withdrawal_sum': {'$last': '$withdrawal_sum'},
                    'deposit_sum': {'$sum': {'$toDouble': '$deposits.quantity'}}
        }},
        {'$addFields': {
            'asset': {'$subtract': ['$deposit_sum', '$withdrawal_sum']}
        }},
        {'$match': {'asset': {'$lt': _min_balance}}},
        {'$count': 'count'}
    ]

    skip_index = next((index for (index, stage) in enumerate(pipeline) if '$skip' in stage), None)
    if skip_index is None:
        raise KeyError('No $skip provided')

    while True:
        partial_result = list(ApiAccess.get_skynet_db().withdrawal_deposit.aggregate(pipeline))
        if len(partial_result) == 0:
            break
        pipeline[skip_index]['$skip'] += _max_limit
        result[0]['count'] += partial_result[0].get('count', 0)

    return result

_long_cache_timeout = 28800 #8hours

@_celery.task
@_internal_cache.cached(expiration_s=_long_cache_timeout)
@_test_generator.create()
def get_deposits_amount(begin=None, end=None):
    global _max_limit
    pipeline = []

    if begin and end: 
        pipeline.append({'$match': {'date_added': {'$gte': begin, '$lte': end}}})
    elif begin:
        pipeline.append({'$match': {'date_added': {'$gte': begin}}})
    elif end:
        pipeline.append({'$match': {'date_added': {'$lte': end}}})

    match_index = next((index for (index, stage) in enumerate(pipeline) if '$match' in stage), None)
    if match_index is None:
        pipeline.append({'$match': {
            'order_status': OrderStatus.approved.value,
            'bank': TransactionType.external.value}})
        match_index = len(pipeline) - 1
    else:
        pipeline[match_index]['$match'].update({
            'order_status': OrderStatus.approved.value,
            'bank': TransactionType.external.value})

    pipeline.extend([
        {'$skip': 0},
        {'$limit': _max_limit}
    ])

    deposit_count = ApiAccess.get_revenyou_db().deposit.\
        find(pipeline[match_index]['$match']).count()

    if deposit_count == 0:
        return {'deposit_count': 0, 'deposit_total': 0, 'deposit_fee': 0}

    pipeline.extend([
        {'$group': {
            '_id': '',
            'deposit_count': {'$sum': 1},
            'deposit_total': {'$sum': {'$toDouble': '$quantity'}},
            'deposit_fee': {'$sum': 
                {'$multiply': [{'$toDouble': '$quantity'}, 
                {'$divide': [{'$toDouble': '$fee'}, 100]}]}}
        }},
        {'$project': {'_id': 0, 'deposit_count': 1, 'deposit_total': 1, 'deposit_fee': 1}}
    ])

    skip_index = next((index for (index, stage) in enumerate(pipeline) if '$skip' in stage), None)
    if skip_index is None:
        raise KeyError('No $skip provided')

    result = Counter()
    while True:
        partial_result = list(ApiAccess.get_revenyou_db().deposit.aggregate(pipeline))
        pipeline[skip_index]['$skip'] += _max_limit
        if len(partial_result) == 0:
            continue
        result.update(partial_result[0])
        if result.get('deposit_count', 0) >= deposit_count:
            break
    
    return dict(result) if len(result) > 0 \
        else {'deposit_count': 0, 'deposit_total': 0, 'deposit_fee': 0}

@_celery.task
@_internal_cache.cached(expiration_s=_long_cache_timeout)
@_test_generator.create()
def get_withdrawals_amount(begin=None, end=None):
    global _max_limit
    pipeline = []
    if begin and end: 
        pipeline.append({'$match': {'date_added': {'$gte': begin, '$lte': end}}})
    elif begin:
        pipeline.append({'$match': {'date_added': {'$gte': begin}}})
    elif end:
        pipeline.append({'$match': {'date_added': {'$lte': end}}})

    match_index = next((index for (index, stage) in enumerate(pipeline) if '$match' in stage), None)
    if match_index is None:
        pipeline.append({'$match': {
            'withdrawal_status': OrderStatus.approved.value}})
        match_index = len(pipeline) - 1
    else:
        pipeline[match_index]['$match'].update({
            'withdrawal_status': OrderStatus.approved.value})

    pipeline.extend([
        {'$skip': 0},
        {'$limit': _max_limit}
    ])
    
    withdrawal_count = ApiAccess.get_revenyou_db().withdrawal.\
        find(pipeline[match_index]['$match']).count()

    if withdrawal_count == 0:
        return {'withdrawal_count': 0, 'withdrawal_total': 0, 'withdrawal_fee': 0}

    pipeline.extend([
        {'$group': {
            '_id': '',
            'withdrawal_count': {'$sum': 1},
            'withdrawal_total': {'$sum': {'$toDouble': '$quantity_in_fiat'}},
            'withdrawal_fee': {'$sum': 
                {'$multiply': [{'$toDouble': '$quantity_in_fiat'}, 
                {'$divide': [{'$toDouble': '$fee_percentage'}, 100]}]}}
        }},
        {'$project': {'_id': 0, 'withdrawal_count': 1, 'withdrawal_total': 1, 'withdrawal_fee': 1}}
    ])

    skip_index = next((index for (index, stage) in enumerate(pipeline) if '$skip' in stage), None)
    if skip_index is None:
        raise KeyError('No $skip provided')

    result = Counter()
    while True:
        partial_result = list(ApiAccess.get_revenyou_db().withdrawal.aggregate(pipeline))
        pipeline[skip_index]['$skip'] += _max_limit
        if len(partial_result) == 0:
            break
        result.update(partial_result[0])
        if result.get('withdrawal_count', 0) >= withdrawal_count:
            break

    return dict(result) if len(result) > 0 \
        else {'withdrawal_count': 0, 'withdrawal_total': 0, 'withdrawal_fee': 0}

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_transaction_info(transaction_id):
    return ApiAccess.get_revenyou_db().transaction.find_one({'_id': ObjectId(transaction_id)}, {'_id': False})

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_user_deposit_amount(user_id):
    data = list(ApiAccess.get_revenyou_db().deposit.aggregate([
        {'$match': {
            'user_id': user_id, 
            'order_status': OrderStatus.approved.value, 
            'bank': TransactionType.external.value}},
        {'$group': {'_id': '', 'deposit_amount': {'$sum': {'$toDouble': '$quantity'}}}}
    ]))
    return data[0] if len(data) > 0 else {'deposit_amount': 0}

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_user_withdraw_amount(user_id):
    data = list(ApiAccess.get_revenyou_db().withdrawal.aggregate([
        {'$match': {
            'user_id': user_id, 
            'withdrawal_status': OrderStatus.approved.value}},
        {'$group': {'_id': '', 'withdrawal_amount': {'$sum': {'$toDouble': '$quantity_in_fiat'}}}}
    ]))
    return data[0] if len(data) > 0 else {'withdrawal_amount': 0}

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_last_transaction_id(user_id):
    deposit_data = list(ApiAccess.get_revenyou_db().deposit.find(
        {'user_id': user_id, 'bank': TransactionType.external.value},
        {'_id': 0, 'transaction_ideal_id': 1, 'order_status': 1}).\
        sort('_id', -1).\
        limit(1))
    if len(deposit_data) == 0:
        return {'last_transaction': '-'}
    try:
        transaction_data = list(ApiAccess.get_revenyou_db().transactions.find(
            {'_id': ObjectId(deposit_data[0]['transaction_ideal_id'])}, 
            {'_id': 0, 'transaction_id': 1}))
        transaction_id = transaction_data[0]['transaction_id']\
            if len(transaction_data) > 0 else 'Id not recorded'
    except KeyError:
        return {'last_transaction': '-'}

    return {'last_transaction': '{} ({})'.format(
        transaction_id, deposit_data[0].get('order_status', 'unknown'))}

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_withdrawal_bank_account(user_id):
    data = list(ApiAccess.get_revenyou_db().withdrawal.find(
        {'user_id': user_id}, {'_id': 0, 'account_number': 1}).\
        sort('_id', -1).\
        limit(1))
    
    return {'bank_account': (data[0]['account_number']\
        if len(data) and 'account_number' in data[0]  else 'Not registered')}

@_celery.task
@_test_generator.create()
def clear_withdrawal(order_id):
    payload = {
        'ip': '89.220.232.107',
        'orders': order_id,
        'token': ApiAccess._revenyou_api_key_cleared
    }
    response = requests.post('{}/exchange/confirm/withdrawal'.format(ApiAccess._revenyou_api_base), json=payload)
    return response.json()

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_customer_deposit_snapshot(user_id):
    result = []
    deposit_cursor = ApiAccess.get_revenyou_db().deposit.find(
        {'user_id': user_id},
        {'_id': 0, 'quantity': 1, 'bank': 1, 'order_status':1, 'date_added': 1})
    for document in deposit_cursor:
        date_time = utils.utc_ts_to_local_str(document.get('date_added', None)).split()
        result.append({
            'date' : date_time[0],
            'time' : (date_time[1] if len(date_time) > 1 else ''),
            'operation': TimelineSteps.deposit.value['operation'],
            'description': '{}: {} ({})'.format(
                TimelineSteps.deposit.value['description'], 
                document.get('quantity', ''),
                document.get('bank', '')),
            'status': document.get('order_status', OrderStatus.unknown.value)
        })

    return json.dumps(result)

@_celery.task
@_test_generator.create()
def get_customer_withdrawal_snapshot(user_id):
    result = []
    withdrawal_cursor = ApiAccess.get_revenyou_db().withdrawal.find(
        {'user_id': user_id},
        {'_id': 0, 'withdrawal_amount_fiat': 1, 'withdrawal_status':1, 'date_added': 1})
    for document in withdrawal_cursor:
        date_time = utils.utc_ts_to_local_str(document.get('date_added', None)).split()
        result.append({
            'date' : date_time[0],
            'time' : (date_time[1] if len(date_time) > 1 else ''),
            'operation': TimelineSteps.withdrawal.value['operation'],
            'description': '{}: {}'.format(
                TimelineSteps.withdrawal.value['description'], 
                document.get('withdrawal_amount_fiat', '')),
            'status': document.get('withdrawal_status', OrderStatus.unknown.value)
        })

    return json.dumps(result)

#TODO: this function serves as a safty backdoor for now, when new procedure of withdrawals confirmed on production this one will adopt
@_celery.task
@_test_generator.create()
def export_withdrawals_csv(): 
    pipeline = [{'$match': {
        'cleared': False, 
        'confirmed_by_user': True, 
        'dump_withdrawal': {'$exists': True, '$ne': []}}
    }]
    data = list(ApiAccess.get_revenyou_db().withdrawal.aggregate(pipeline))
    return json.dumps(pd.DataFrame(data).filter([
        'user_id', 'description', 
        'withdrawal_amount_fiat', 
        'holder_name', 'account_number']).to_dict('records'))

@_celery.task
@_test_generator.create()
def export_withdrawals_xml():
    data_clear = list(ApiAccess.get_skynet_db().clear_withdrawals.find({}))
    return create_xml(data_clear)

@_celery.task
@_test_generator.create()
def any_failed_withdrawal():
    return {'any': ApiAccess.get_skynet_db().failed_withdrawals.find({}).count() != 0}

@_celery.task
@_test_generator.create()
def get_failed_withdrawal_xml(ids=None):
    data_failed = list(ApiAccess.get_skynet_db().failed_withdrawals.find({}))
    return create_xml(data_failed)

@_celery.task
@_test_generator.create()
def get_withdrawable_amount(unsub_value, user_id):
    pipeline = [
        {'$match': {'user_id': user_id, 'cleared': True}},
        {'$group': {
            '_id': '',
            'amount': {'$sum': {'$toDouble': '$quantity_in_fiat'}}
        }},
        {'$project': {'_id': 0, 'amount': 1}}
    ]
    result = list(ApiAccess.get_revenyou_db().withdrawal.aggregate(pipeline))
    withdrawn_amount = result[0].get('amount', 0) if len(result) > 0 else 0
    return {'withdrawable_amount': unsub_value - withdrawn_amount}

@_celery.task
@_test_generator.create()
def get_withdrawable_amount_dump(user_id):
    dumps = list(ApiAccess.get_revenyou_db().withdrawal.find({
        'user_id': user_id,
        'cleared': False, 
        'confirmed_by_user': True,
        'dump_withdrawal': {'$exists': True, '$ne': []}}, 
        {'_id': 0, 'dump_withdrawal': 1}))
    
    final_amount = 0
    for dump in next(iter(dumps or []), {}).get('dump_withdrawal', []):
        final_amount += sum(float(item.get('amount_fiat', 0)) for item in dump)
    return {'withdrawable_amount_dump': final_amount}

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_top_deposit_info(ids):
    if ids is None or len(ids.get('content', [])) == 0:
        return {'content': []}

    info = list(ApiAccess.get_revenyou_db().deposit.aggregate([
        {'$match': {
            'user_id': {'$in': [doc['_id'] for doc in ids.get('content', []) if '_id' in doc]},
            'bank': TransactionType.external.value,
            'order_status': OrderStatus.approved.value
        }},
        {'$addFields': {'double_quantity': {'$toDouble': '$quantity'}}},
        {'$group': {
            '_id': '$user_id',
            'sum': {'$sum': '$double_quantity'},
            'count': {'$sum': 1},
            'max': {'$max': '$double_quantity'},
            'min': {'$min': '$double_quantity'}
        }}
    ]))

    return {'content': info}

@_celery.task
@_internal_cache.cached(expiration_s=_long_cache_timeout)
@_test_generator.create()
def get_first_deposit_data():
    pipeline = [
        {'$skip': 0},
        {'$limit': _max_limit},
        {'$group': {
            '_id': {
                '$dateToString': {
                    'format': '%Y-%m-%d',
                    'date': {'$toDate': { '$multiply': [1000, '$date_added'] }}
                }
            },
            'count': {'$sum': 1},
            'value': {'$avg': '$quantity'}
        }},
        {'$project': {'_id': 0, 'date': '$_id', 'count': 1, 'value': 1}}
    ]

    skip_index = next((index for (index, stage) in enumerate(pipeline) if '$skip' in stage), None)
    if skip_index is None:
        raise KeyError('No $skip provided')

    daily = []
    while True:
        partial_result = list(ApiAccess.get_skynet_db().first_deposit.aggregate(pipeline))
        if len(partial_result) == 0:
            break
        pipeline[skip_index]['$skip'] += _max_limit
        daily.extend(partial_result)
    daily = sorted(daily, key=lambda k: k['date'], reverse=True)
    df_daily = pd.DataFrame(daily).groupby('date', as_index=False).agg({'count': 'sum', 'value': 'mean'})

    value_trendline = utils.get_trendline(df_daily['value'].to_list())
    count_trendline = utils.get_trendline(df_daily['count'].to_list())
    daily = df_daily.to_dict('records')

    for index, item in enumerate(daily):
        item.update({
            'value_trend': value_trendline[index],
            'count_trend': count_trendline[index]
        })

    return {'content': daily}


import uuid
from io import BytesIO
from xml.etree import ElementTree as ET

_chunk_size = 100 #BUNQ limit
def create_xml(data, is_failed=False):
    result = {'content': []}

    chunks = utils.chunks(data, _chunk_size)
    for index, chunk in enumerate(chunks):
        count = str(len(chunk))
        amount = '{:.2f}'.format(sum(float(item['withdrawal_amount_fiat']) for item in chunk))
        Document = ET.Element('Document')
        Document.set('xmlns', 'urn:iso:std:iso:20022:tech:xsd:pain.001.001.03')
        Document.set('xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance')
        CstmrCdtTrfInitn = ET.SubElement(Document, 'CstmrCdtTrfInitn')
        GrpHdr = ET.SubElement(CstmrCdtTrfInitn, 'GrpHdr')
        ET.SubElement(GrpHdr, 'MsgId').text = str(uuid.uuid4())[:-1]
        ET.SubElement(GrpHdr, 'CreDtTm').text = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        ET.SubElement(GrpHdr, 'NbOfTxs').text = count
        ET.SubElement(GrpHdr, 'CtrlSum').text = amount
        InitgPty = ET.SubElement(GrpHdr, 'InitgPty')
        ET.SubElement(InitgPty, 'Nm').text = 'REVENYOU (CMS BEHEER B.V.)'
        PmtInf = ET.SubElement(CstmrCdtTrfInitn, 'PmtInf')
        ET.SubElement(PmtInf, 'PmtInfId').text = str(uuid.uuid4())[:-1]
        ET.SubElement(PmtInf, 'PmtMtd').text = 'TRF'
        ET.SubElement(PmtInf, 'BtchBookg').text = 'true'
        ET.SubElement(PmtInf, 'NbOfTxs').text = count
        ET.SubElement(PmtInf, 'CtrlSum').text = amount
        PmtTpInf = ET.SubElement(PmtInf, 'PmtTpInf')
        ET.SubElement(PmtTpInf, 'InstrPrty').text = 'NORM'
        SvcLvl = ET.SubElement(PmtTpInf, 'SvcLvl')
        ET.SubElement(SvcLvl, 'Cd').text = 'SEPA'
        ET.SubElement(PmtInf, 'ReqdExctnDt').text = datetime.now().strftime('%Y-%m-%d')
        Dbtr = ET.SubElement(PmtInf, 'Dbtr')
        ET.SubElement(Dbtr, 'Nm').text = 'RevenYOU (CMS Beheer B.V.)'
        DbtrAcct = ET.SubElement(PmtInf, 'DbtrAcct')
        Id = ET.SubElement(DbtrAcct, 'Id')
        ET.SubElement(Id, 'IBAN').text = 'NL43BUNQ2040933417'
        DbtrAgt = ET.SubElement(PmtInf, 'DbtrAgt')
        FinInstnId = ET.SubElement(DbtrAgt, 'FinInstnId')
        ET.SubElement(FinInstnId, 'BIC').text = 'BUNQNL2A'

        for item in chunk:
            CdtTrfTxInf = ET.SubElement(PmtInf, 'CdtTrfTxInf')
            PmtId = ET.SubElement(CdtTrfTxInf, 'PmtId')
            ET.SubElement(PmtId, 'EndToEndId').text = item.get('description', '').replace('#', '')
            Amt = ET.SubElement(CdtTrfTxInf, 'Amt')
            InstdAmt = ET.SubElement(Amt, 'InstdAmt')
            InstdAmt.set('Ccy', item.get('fiat', 'EUR'))
            InstdAmt.text = item.get('withdrawal_amount_fiat', '0.00')
            Cdtr = ET.SubElement(CdtTrfTxInf, 'Cdtr')
            ET.SubElement(Cdtr, 'Nm').text = item.get('holder_name', '')
            CdtrAcct = ET.SubElement(CdtTrfTxInf, 'CdtrAcct')
            Id2 = ET.SubElement(CdtrAcct, 'Id')
            ET.SubElement(Id2, 'IBAN').text = item.get('account_number', '')
            RmtInf = ET.SubElement(CdtTrfTxInf, 'RmtInf')
            ET.SubElement(RmtInf, 'Ustrd').text = item.get('description', '').replace('#', '')

        f = BytesIO()
        ET.ElementTree(Document).write(f, encoding='utf-8', xml_declaration=True)

        result['content'].append({
            'fname': '{}{}_{}.xml'.format(
                datetime.now().strftime('%Y-%m-%d'), 
                '_failed' if is_failed else '',
                index),
            'xml': f.getvalue().decode('utf-8')})

    return result

def get_periodic_pipeline(type_):
    amount_field = 'quantity' if type_ == 'deposit' else 'quantity_in_fiat'
    status_field = 'order_status' if type_ == 'deposit' else 'withdrawal_status'

    pipeline = [{'$match': {status_field: OrderStatus.approved.value}}]
    if type_ == 'deposit':
        pipeline[0]['$match'].update({'bank': TransactionType.external.value})

    pipeline.extend([
        {'$skip': 0},
        {'$limit': _max_limit},
        {'$group': {
            '_id': {
                '$dateToString': {
                    'format': '%Y-%m-%d',
                    'date': {'$toDate': {'$multiply': [1000, '$date_added']}}
                }
            },
            'value': {'$sum': {'$toDouble': '${}'.format(amount_field)}},
            'count': {'$sum': 1}
        }},
        {'$project': {'_id': 0, 'date': '$_id', 'value': 1, 'count': 1}}
    ])

    return pipeline

@_celery.task
@_internal_cache.cached(expiration_s=_long_cache_timeout)
@_test_generator.create()
def get_periodical_transaction_amount(type_):
    global _max_limit
    pipeline = get_periodic_pipeline(type_)

    skip_index = next((index for (index, stage) in enumerate(pipeline) if '$skip' in stage), None)
    if skip_index is None:
        raise KeyError('No $skip provided')

    daily = []
    db = ApiAccess.get_revenyou_db().deposit if type_ == 'deposit' else ApiAccess.get_revenyou_db().withdrawal
    while True:
        partial_result = list(db.aggregate(pipeline))
        if len(partial_result) == 0:
            break
        pipeline[skip_index]['$skip'] += _max_limit
        daily.extend(partial_result)

    daily = sorted(daily, key=lambda k: k['date'], reverse=True) 
    df_daily = pd.DataFrame(daily).groupby('date', as_index=False).sum()
    daily = df_daily.to_dict('records')
    df_daily.date = pd.to_datetime(df_daily.date)

    df_weekly = df_daily.resample('W-Mon', on='date').sum().reset_index().sort_values(by='date')
    df_weekly['date'] = df_weekly['date'].dt.strftime('%Y-%m-%d')
    weekly = df_weekly.to_dict('records')

    df_monthly = df_daily[(df_daily['date'] > '{}-01-01'.format(datetime.now().year))].\
        groupby(df_daily['date'].dt.strftime('%B')).sum().sort_values(by='date')
    monthly = pd.DataFrame({'date': df_monthly.index, 'value': df_monthly['value'], 'count': df_monthly['count']}).to_dict('records')

    df_yearly = df_daily.groupby(df_daily['date'].dt.strftime('%Y')).sum().sort_values(by='date')
    yearly = pd.DataFrame({'date': df_yearly.index, 'value': df_yearly['value'], 'count': df_yearly['count']}).to_dict('records')

    return { 
        'daily': daily,
        'weekly': weekly,
        'monthly': monthly,
        'yearly': yearly }


@_celery.task
@_internal_cache.cached(expiration_s=_long_cache_timeout)
@_test_generator.create()
def join_periodic(deposit_data, withdrawal_data):
    result = {}
    for period in deposit_data.keys():
        df_deposits = pd.DataFrame(deposit_data[period])
        df_withdrawals = pd.DataFrame(withdrawal_data[period])

        joined_df = pd.merge(
            df_deposits.set_index('date'), 
            df_withdrawals.set_index('date'), 
            on='date', 
            how='outer', 
            suffixes=('_deposit', '_withdrawal')).fillna(0)

        result[period] = pd.DataFrame({
            'date':joined_df.index, 
            'value_2': joined_df['value_deposit'], 
            'value_1': joined_df['value_withdrawal'],
            'count_2': joined_df['count_deposit'],
            'count_1': joined_df['count_withdrawal']}).to_dict('records')

    return result

@_celery.task
@_internal_cache.cached(expiration_s=_long_cache_timeout)
@_test_generator.create()
def get_deposit_categories(stage, begin=None, end=None):
    if begin and end: 
        pipeline = [{'$match': {'date_added': {'$gte': begin, '$lte': end}}}]
    elif begin:
        pipeline = [{'$match': {'date_added': {'$gte': begin}}}]
    elif end:
        pipeline = [{'$match': {'date_added': {'$lte': end}}}]
    else:
        pipeline = []

    boundaries = [0, 50, 100, 200, 500, 1000, 1500, 5000, 10000, sys.maxsize]
    if stage == DepositStage.all.value:
        pipeline.append({'$match': {
            'order_status': OrderStatus.approved.value,
            'bank': TransactionType.external.value}})

    pipeline.extend([
        {'$skip': 0},
        {'$limit': _max_limit},
        {'$bucket': {
            'groupBy': {'$toDouble': '$quantity'},
            'boundaries': boundaries,
            'default': -1,
            'output': {
                'count': {'$sum': 1 },
                'amount': {'$sum': {'$toDouble': '$quantity'}}
            }
        }}
    ])

    skip_index = next((index for (index, stage) in enumerate(pipeline) if '$skip' in stage), None)
    if skip_index is None:
        raise KeyError('No $skip provided')

    collection = ApiAccess.get_skynet_db().first_deposit if stage == DepositStage.first.value else ApiAccess.get_revenyou_db().deposit
    buckets = []
    while True:
        partial_result = list(collection.aggregate(pipeline))
        if len(partial_result) == 0:
            break
        pipeline[skip_index]['$skip'] += _max_limit
        buckets.extend(partial_result)

    if len(buckets) == 0:
        return {
            'content': [],
            'max': 0
        }

    df_buckets = pd.DataFrame(buckets)
    df_buckets = df_buckets.groupby('_id', as_index=False).sum()
    df_buckets['avg'] = df_buckets['amount'] / df_buckets['count']
    df_buckets['share'] = df_buckets['count'] / df_buckets['count'].sum() * 100

    result = df_buckets.to_dict('records')
    for item in result:
        index = boundaries.index(item['_id'])
        item['_id'] = '{}-{}'.format(item['_id'], 
            'MAX' if index == len(boundaries) - 2 
            else boundaries[index + 1])
    
    max_bucket = max(result, key=lambda x: x.get('count', 0))
    total_amount = sum(item.get('amount', 0) for item in result)
    total_count = sum(item.get('count', 0) for item in result)
    return {
        'content': result,
        'max': max_bucket.get('count', 0) if max_bucket is not None else 0,
        'total_amount': total_amount,
        'total_count': total_count,
        'total_avg': total_amount / total_count,
        'total_share': sum(item.get('share', 0) for item in result)
    }