import json
import requests
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
from datetime import datetime, timedelta, date
from enums import SubscriptionStatus, TimelineSteps, UserStatus, CustomerType, CustomerGender
import utils
import pandas as pd
from cache_manager import CacheManger
from generator import TestGenerator
import cloudinary

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
def get_csv_exportable(max, cols, begin, end):
    query = {}
    if begin and end: 
        query['date_added'] = {'$gte': begin, '$lte': end}
    elif begin:
        query['date_added'] = {'$gte': begin}
    elif end:
        query['date_added'] = {'$lte': end}

    cursor = list(ApiAccess.get_revenyou_db().sign_up.\
                find(query, {'_id': 0}).\
                limit(max).\
                collation({'locale': 'en'}))

    if 'phone_number' in cols:
        carriers = ApiAccess.get_revenyou_db().carrier.\
        find({'_id': \
            {'$in' : [ObjectId(doc.get('carrier_id', '')) for doc in cursor]}},
            {'non_formated_phone_number': 1}).\
        limit(max)

        phone_numbers = {}
        for item in carriers:
            phone_numbers[str(item['_id'])] = item.get('non_formated_phone_number', '')

    result = []
    for document in cursor:
        result.append({})        
        for col in cols:
            if col == 'phone_number':
                result[-1]['phone_number'] = phone_numbers.get(document.get('carrier_id', ''), '')
                continue
            if col == 'full_name':
                result[-1]['full_name'] = '{} {}'.format(document.get('first_name', ''), document.get('last_name', ''))
                continue
            if col =='date_added':
                result[-1]['date_added'] = utils.utc_ts_to_local_str(document.get('date_added', None))
                continue

            result[-1][col] = document.get(col, '')

    return json.dumps(result)

def get_advance_filters(advanced):
    filters = []
    if 'kyc' in advanced and advanced['kyc'] != CustomerType.all.value:
        filters.append({'$match': {'swiftdil_customer_id': {'$exists': advanced['kyc'] == CustomerType.kyc.value}}})
    if 'gender' in advanced and advanced['gender'] != CustomerGender.all.value:
        filters.append({'$match': {'gender': advanced['gender']}})
    if 'status' in advanced:
        filters.append({'$match': {'status': {'$in': advanced['status']}}})
    if 'dob' in advanced and len(advanced['dob']) == 2:
        filters.append({'$match': {'dob': {'$gte': advanced['dob'][0], '$lte': advanced['dob'][1]}}})
    if 'type' in advanced:
        if 'private' in advanced['type']:
            filters.append({'$match': {'$or': [{'type': {'$exists': False}}, {'type': {'$in': advanced['type']}}]}})
        else:
            filters.append({'$match': {'type': {'$in': advanced['type']}}})
    
    return filters

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_customers_internal(size, page, sort, order, begin, end, name, advanced):
    pipeline = [{'$addFields': {'full_name': {'$concat': ['$first_name', ' ' ,'$last_name']}}}]

    if len(advanced) != 0:
        pipeline.extend(get_advance_filters(json.loads(advanced)))

    if begin and end: 
        pipeline.append({'$match': {'date_added': {'$gte': begin, '$lte': end}}})
    elif begin:
        pipeline.append({'$match': {'date_added': {'$gte': begin}}})
    elif end:
        pipeline.append({'$match': {'date_added': {'$lte': end}}})
    
    if name:
        initials = name.split()
        full_name_regex = '.*{}.*'.format(initials[0])
        for initial in initials[1:]:
            full_name_regex += ' .*{}.*'.format(initial)

        pipeline.append(
            {'$match': {'$or': [ 
                {'full_name': {'$regex': full_name_regex, '$options': 'i'}},
                {'email': {'$regex': '.*{}.*'.format(name), '$options': 'i'}},
                {'dob': {'$regex': '.*{}.*'.format(name), '$options': 'i'}}
            ]}})
    
    count_pipline = pipeline.copy()
    count_pipline.append({'$count': 'count'})

    maybe_count = list(ApiAccess.get_revenyou_db().sign_up.aggregate(count_pipline))
    total_elements = maybe_count[0]['count'] if maybe_count else 0

    pipeline.extend([
        {'$addFields': {'full_name_cap': {'$toLower': '$full_name'}}},
        {'$sort': {sort: (-1 if order == 'desc' else 1)}},
        {'$limit': size + (page * size) },
        {'$skip': (page * size) }
    ])

    cursor = list(ApiAccess.get_revenyou_db().sign_up.aggregate(pipeline))

    result = {
        'total_elements': total_elements,
        'total_pages': math.ceil(float(total_elements)/size),
        'current_page': page,
        'content': []}

    carriers = ApiAccess.get_revenyou_db().carrier.\
        find({'_id': \
            {'$in' : [ObjectId(doc.get('carrier_id', '')) for doc in cursor]}}, 
            {'non_formated_phone_number': 1}).\
        limit(size)

    phone_numbers = {}
    for item in carriers:
        phone_numbers[str(item['_id'])] = item.get('non_formated_phone_number', '')

    for document in cursor:
        result['content'].append(document)
        result['content'][-1]['_id'] = str(result['content'][-1]['_id'])
        result['content'][-1]['date_added'] = utils.utc_ts_to_local_str(document.get('date_added', None))
        result['content'][-1]['date_modified'] = utils.utc_ts_to_local_str(document.get('date_modified', None))
        result['content'][-1]['mobile'] = phone_numbers.get(document.get('carrier_id', ''), '')
        
    return result

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_customers_internal_from_phone(size, begin, end, name):
    query = {'non_formated_phone_number': {'$regex': '.*{}.*'.format(name), '$options': 'i'}}

    if begin and end: 
        query['date_added'] = {'$gte': begin, '$lte': end}
    elif begin:
        query['date_added'] = {'$gte': begin}
    elif end:
        query['date_added'] = {'$lte': end}
    
    total_elements = ApiAccess.get_revenyou_db().carrier.find(query).count()
    carriers = list(ApiAccess.get_revenyou_db().carrier.\
        find(query, {'non_formated_phone_number': 1}).\
        sort('_id', -1).\
        limit(size))

    phone_numbers = {}
    for item in carriers:
        phone_numbers[str(item['_id'])] = item.get('non_formated_phone_number', '')

    pipeline = [
        {'$match': {'carrier_id': {'$in': list(phone_numbers.keys())}}},
        {'$addFields': {'full_name': {'$concat': ['$first_name', ' ' ,'$last_name']}}},
        {'$addFields': {'full_name_cap': {'$toLower': '$full_name'}}},
        {'$sort': {'_id': -1 }},
        {'$limit': size}]

    cursor = list(ApiAccess.get_revenyou_db().sign_up.aggregate(pipeline))

    result = {
        'total_elements': total_elements,
        'total_pages': 1,
        'current_page': 0,
        'content': []}

    for document in cursor:
        result['content'].append(document)
        result['content'][-1]['_id'] = str(result['content'][-1]['_id'])
        result['content'][-1]['date_added'] = utils.utc_ts_to_local_str(document.get('date_added', None))
        result['content'][-1]['date_modified'] = utils.utc_ts_to_local_str(document.get('date_modified', None))
        result['content'][-1]['mobile'] = phone_numbers.get(document.get('carrier_id', ''), '')

    return result

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_total_customer_count(begin=None, end=None):
    query = {}
    if begin and end: 
        query['date_added'] = {'$gte': begin, '$lte': end}
    elif begin:
        query['date_added'] = {'$gte': begin}
    elif end:
        query['date_added'] = {'$lte': end}
    
    count = ApiAccess.get_revenyou_db().user.find(query).count()

    return [{'_id': 'Total users', 'count': count}]

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_kyc_customer_count(begin=None, end=None):
    query = {}
    if begin and end: 
        query['date_added'] = {'$gte': begin, '$lte': end}
    elif begin:
        query['date_added'] = {'$gte': begin}
    elif end:
        query['date_added'] = {'$lte': end}
    
    count = ApiAccess.get_revenyou_db().sign_up.find(query).count()

    return [{'_id': 'KYC users', 'count': count}]

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_full_kyc_customer_count(begin=None, end=None):
    query = {'$and': 
        [{'swiftdil_customer_id': {'$exists': True}},
        {'swiftdil_customer_id': {'$ne': ''}},
        {'swiftdil_customer_id': {'$ne': '-'}}]}

    if begin and end: 
        query['date_added'] = {'$gte': begin, '$lte': end}
    elif begin:
        query['date_added'] = {'$gte': begin}
    elif end:
        query['date_added'] = {'$lte': end}

    count = ApiAccess.get_revenyou_db().sign_up.find(query).count()

    return [{'_id': 'Full KYC users', 'count': count}]

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_subscribed_customer_count(begin=None, end=None):
    if begin and end: 
        pipeline = [{'$match': {'date_added': {'$gte': begin, '$lte': end}}}]
    elif begin:
        pipeline = [{'$match': {'date_added': {'$gte': begin}}}]
    elif end:
        pipeline = [{'$match': {'date_added': {'$lte': end}}}]
    else:
        pipeline = []
    
    pipeline.extend([
        {'$match': {'status': SubscriptionStatus.active.value}},
        {'$group': {'_id': '$user_id'}},
        {'$count': 'count'}
    ])
    cursor = ApiAccess.get_revenyou_db().subscribed.aggregate(pipeline)
    
    result = [{'_id': 'Subscribed users', 'count': 0}]
    for pair in cursor:
        result[0].update(pair)

    return result

@_celery.task
@utils.deprecated
def get_customers(args):
    headers = {
        'Authorization': 'Bearer {}'.format(ApiAccess.get_swift_access_token())
    }

    args_clean = {k: v for k, v in args.items() if v is not None}
    response = requests.get(
        '{}/customers?{}'.format(ApiAccess._swift_api_base,
                                 parse.urlencode(args_clean)),
        headers=headers)
    response.raise_for_status()
    return response.json()


@_celery.task
@utils.deprecated
def get_customers_count(args):
    headers = {
        'Authorization': 'Bearer {}'.format(ApiAccess.get_swift_access_token())
    }

    args['size'] = 1
    args_clean = {k: v for k, v in args.items() if v is not None}
    response = requests.get(
        '{}/customers?{}'.format(ApiAccess._swift_api_base,
                                 parse.urlencode(args_clean)),
        headers=headers)
    response.raise_for_status()
    return response.json().get('total_elements', None)

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_customer_internal(user_id, is_corporate):
    db = ApiAccess.get_revenyou_db().corporate_account if is_corporate \
        else ApiAccess.get_revenyou_db().sign_up
    cursor = list(db.find({'user_id': user_id}))
    if len(cursor) == 0:
        return {}

    user = cursor[0]
    carrier_id = user.get('carrier_id', None)
    country = user.get('country', None)
    carrier = ApiAccess.get_revenyou_db().carrier.find_one({'_id': ObjectId(carrier_id)}) if carrier_id is not None else {}
    country = pycountry.countries.get(alpha_3=country.upper()) if country is not None else None
    user['mobile'] = carrier.get('non_formated_phone_number', '') if carrier else ''
    user['_id'] = str(user['_id'])
    user['country'] = country.name if country is not None else 'Unknown'
    user['date_added'] = utils.utc_ts_to_local_str(user.get('date_added', None))
    user['date_modified'] = utils.utc_ts_to_local_str(user.get('date_modified', None))
    user['full_name'] = '{} {}'.format(user.get('first_name', ''), user.get('last_name', ''))

    try:
        if 'document_front_url' in user:
            user['document_front_url'] = cloudinary.CloudinaryImage(user['document_front_url']).build_url()
        if 'document_back_url' in user:
            user['document_back_url'] = cloudinary.CloudinaryImage(user['document_back_url']).build_url()
    except Exception:
        pass
    
    return user

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_customer(customer_id):
    headers = {
        'Authorization': 'Bearer {}'.format(ApiAccess.get_swift_access_token())
    }

    response = requests.get(
        '{}/customers/{}'.format(ApiAccess._swift_api_base, 
                                customer_id), headers=headers)
    response.raise_for_status()
    
    response_body = response.json()
    countries = pycountry_convert.map_country_alpha3_to_country_name()

    if 'nationality' in response_body:
        response_body['nationality'] = countries[response_body['nationality']]
    if 'birth_country' in response_body:
        response_body['birth_country'] = countries[response_body['birth_country']]

    return response_body 

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_customer_risk(customer_id):
    headers = {
        'Authorization': 'Bearer {}'.format(ApiAccess.get_swift_access_token())
    }

    response = requests.get(
        '{}/customers/{}/risk_profile'.format(ApiAccess._swift_api_base, 
                                            customer_id), headers=headers)
    response.raise_for_status()
    return response.json()

@_celery.task
@_test_generator.create()
def board_customer(token, args):
    pass

@_celery.task
@_test_generator.create()
def get_first_deposits(args):
    pass

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_swift_id(user_id):
    cursor = ApiAccess.get_revenyou_db().sign_up.find(
        {'user_id': user_id},
        {'swiftdil_customer_id': 1, '_id': 0})

    return json.dumps(cursor[0] if cursor.count() > 0 else '')


@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_customer_snapshot(user_id):
    cursor = ApiAccess.get_revenyou_db().sign_up.find(
        {'user_id': user_id},
        {'_id': 0, 'youhex_name': 1, 'status': 1, 'date_added': 1})

    if cursor.count() == 0:
        return {}

    
    date_time = utils.utc_ts_to_local_str(cursor[0].get('date_added', None)).split()
    result = {
        'date' : date_time[0],
        'time' : (date_time[1] if len(date_time) > 1 else ''),
        'operation': TimelineSteps.user_creation.value['operation'],
        'description': '{}: {}'.format(
            TimelineSteps.user_creation.value['description'], 
            cursor[0].get('youhex_name', '')),
        'status': cursor[0].get('status', UserStatus.unknown.value)
    }
    return result

_max_limit = 1000
_long_cache_timeout = 28800 #8hours

@_celery.task
@_internal_cache.cached(expiration_s=_long_cache_timeout)
@_test_generator.create()
def get_geographic_customer_count():
    global _max_limit
    pipeline = [
        {'$skip': 0},
        {'$limit': _max_limit},
        {'$group': {'_id': '$country_code', 'value': {'$sum': 1}}},
        {'$project': {'_id': 0, 'id': "$_id", 'value': 1}},
    ]

    skip_index = next((index for (index, stage) in enumerate(pipeline) if '$skip' in stage), None)
    if skip_index is None:
        raise KeyError('No $skip provided')

    data = []
    while True:
        partial_result = list(ApiAccess.get_revenyou_db().carrier.aggregate(pipeline))
        if len(partial_result) == 0:
            break
        pipeline[skip_index]['$skip'] += _max_limit
        data.extend(partial_result)
    data = sorted(data, key=lambda k: k['value'], reverse=True)

    result = (pd.DataFrame(data)\
        .groupby('id', as_index=False)\
        .value.sum()\
        .sort_values('value')\
        .to_dict('records'))
    return {
        'min': result[-1].get('value', 0) if len(result) > 0 else 0,
        'max': result[0].get('value', 0) if len(result) > 0 else 0,
        'content': result
    }

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_customers_by_id(ids, size, page, sort, order, name=None):
    if ids is None or len(ids.get('content', [])) == 0:
        return {
            'total_elements': 0,
            'total_pages': 0,
            'current_page': 0,
            'content': []}
    
    ids_raw = [doc['_id'] for doc in ids.get('content', []) if '_id' in doc]
    pipeline = [
        {'$addFields': {'full_name': {'$concat': ['$first_name', ' ' ,'$last_name']}}},
        {'$match': {'user_id': {'$in': ids_raw}}}]

    if name:
        initials = name.split()
        full_name_regex = '.*{}.*'.format(initials[0])
        for initial in initials[1:]:
            full_name_regex += ' .*{}.*'.format(initial)

        pipeline.append(
            {'$match': {'$or': [ 
                {'full_name': {'$regex': full_name_regex, '$options': 'i'}},
                {'email': {'$regex': '.*{}.*'.format(name), '$options': 'i'}},
                {'dob': {'$regex': '.*{}.*'.format(name), '$options': 'i'}}
            ]}})

    count_pipline = pipeline.copy()
    count_pipline.append({'$count': 'count'})

    maybe_count = list(ApiAccess.get_revenyou_db().sign_up.aggregate(count_pipline))
    total_elements = maybe_count[0]['count'] if maybe_count else 0

    pipeline.append({'$addFields': {'full_name_cap': {'$toLower': '$full_name'}}})

    if sort == 'external':
        pipeline.extend([
            {'$addFields': {'__order': {'$indexOfArray' : [ids_raw, '$user_id' ]}}},
            {'$sort': {'__order': (-1 if order == 'desc' else 1)}}])
    else:
        pipeline.append({'$sort': {sort: (-1 if order == 'desc' else 1)}})
    
    pipeline.extend([
        {'$limit': size + (page * size)},
        {'$skip': (page * size)}
    ])

    cursor = list(ApiAccess.get_revenyou_db().sign_up.aggregate(pipeline))

    result = {
        'total_elements': total_elements,
        'total_pages': math.ceil(float(total_elements)/size),
        'current_page': page,
        'content': []}

    carriers = ApiAccess.get_revenyou_db().carrier.\
        find({'_id': {'$in' : [ObjectId(doc.get('carrier_id', '')) for doc in cursor]}}, {'non_formated_phone_number': 1})
    phone_numbers = {}
    for item in carriers:
        phone_numbers[str(item['_id'])] = item.get('non_formated_phone_number', '')

    for document in cursor:
        result['content'].append(document)
        result['content'][-1]['_id'] = str(result['content'][-1]['_id'])
        result['content'][-1]['date_added'] = utils.utc_ts_to_local_str(document.get('date_added', None))
        result['content'][-1]['date_modified'] = utils.utc_ts_to_local_str(document.get('date_modified', None))
        result['content'][-1]['mobile'] = phone_numbers.get(document.get('carrier_id', ''), '')
        
    return result

@_celery.task
@_internal_cache.cached(expiration_s=_long_cache_timeout)
@_test_generator.create()
def get_periodical_customer_count():
    global _max_limit
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
            'value': {'$sum': 1}
        }},
        {'$project': {'_id': 0, 'date': "$_id", 'value': 1}}
    ]
    skip_index = next((index for (index, stage) in enumerate(pipeline) if '$skip' in stage), None)
    if skip_index is None:
        raise KeyError('No $skip provided')

    daily = []
    while True:
        partial_result = list(ApiAccess.get_revenyou_db().sign_up.aggregate(pipeline))
        if len(partial_result) == 0:
            break
        pipeline[skip_index]['$skip'] += _max_limit
        daily.extend(partial_result)

    daily = sorted(daily, key=lambda k: k['date'], reverse=True) 
    df_daily = pd.DataFrame(daily).groupby('date', as_index=False).sum()
    daily = df_daily.to_dict('records')
    df_daily.date = pd.to_datetime(df_daily.date)

    df_monthly = df_daily[(df_daily['date'] > '{}-01-01'.format(datetime.now().year))].\
        groupby(df_daily['date'].dt.strftime('%B'))['value'].sum().sort_values()
    monthly = pd.DataFrame({'month':df_monthly.index, 'value':df_monthly.values}).to_dict('records')

    df_yearly = df_daily.groupby(df_daily['date'].dt.strftime('%Y'))['value'].sum().sort_values()
    yearly = pd.DataFrame({'year':df_yearly.index, 'value':df_yearly.values}).to_dict('records')

    return { 
        'daily': daily,
        'monthly': monthly,
        'yearly': yearly }

@_celery.task
@_test_generator.create()
def save_note(user_id, creator, title, note):
    payload = {
        'identifier': ApiAccess._revenyou_api_key_add_note,
        'creator': creator,
        'title': title,
        'note': note,
        'user_id': user_id
    }
    response = requests.post(
        '{}/v1/skynet/notes/create'.format(ApiAccess._revenyou_api_base), json=payload)

    return response.json()

@_celery.task
@_test_generator.create()
def get_notes(user_id):
    notes = list(ApiAccess.get_revenyou_db().notes.\
        find({'user_id': user_id}, {'_id': False}).\
        sort('date_added', -1).\
        limit(10))

    for note in notes:
        note.update((k, utils.utc_ts_to_local_str(v)) for k, v in note.items() if k == 'date_added')
    
    return {'notes': notes}

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_corporates_internal(size, page, sort, order, begin, end, name):
    pipeline = [{'$addFields': {'full_name': {'$concat': ['$first_name', ' ' ,'$middle_name', ' ' ,'$last_name']}}}]

    if begin and end: 
        pipeline.append({'$match': {'date_added': {'$gte': begin, '$lte': end}}})
    elif begin:
        pipeline.append({'$match': {'date_added': {'$gte': begin}}})
    elif end:
        pipeline.append({'$match': {'date_added': {'$lte': end}}})
    
    if name:
        initials = name.split()
        full_name_regex = '.*{}.*'.format(initials[0])
        for initial in initials[1:]:
            full_name_regex += ' .*{}.*'.format(initial)

        pipeline.append(
            {'$match': {'$or': [ 
                {'full_name': {'$regex': full_name_regex, '$options': 'i'}},
                {'email': {'$regex': '.*{}.*'.format(name), '$options': 'i'}}
            ]}})
    
    count_pipline = pipeline.copy()
    count_pipline.append({'$count': 'count'})

    maybe_count = list(ApiAccess.get_revenyou_db().corporate_account.aggregate(count_pipline))
    total_elements = maybe_count[0]['count'] if maybe_count else 0

    pipeline.extend([
        {'$addFields': {'full_name_cap': {'$toLower': '$full_name'}}},
        {'$sort': {sort: (-1 if order == 'desc' else 1)}},
        {'$limit': size + (page * size) },
        {'$skip': (page * size) }
    ])

    cursor = list(ApiAccess.get_revenyou_db().corporate_account.aggregate(pipeline))

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
        
    return result
