import json
import requests
from celery import Celery
from urllib import parse
import pycountry
import pycountry_convert
from bson.objectid import ObjectId
import sys
sys.path.extend(['..', 'tests'])
from api_access import ApiAccess
from service_manager import ServiceManager
from datetime import datetime, timedelta
import decimal
from generator import TestGenerator

_test_generator = TestGenerator(__name__)

service_name = ServiceManager.get_service_name(__name__)
_celery = Celery(
    service_name,
    backend=ServiceManager.get_backend(service_name),
    broker=ServiceManager.get_broker(service_name))

def get_historical_price(symbol, begin, end, interval='30m', is_eur=True):
    payload = {
        'code': ApiAccess._revenyou_api_key_quotes, 
        'symbols': symbol,
        'convert_to': 'TUSD',
        'interval': interval,
        'limit': '10',
        'time_start': begin,
        'time_end': end,
        'is_eur': is_eur
    }

    response = requests.post('{}/exchange/quotes/historical'.format(ApiAccess._revenyou_api_base), json=payload)
    try:
        parsed_response = json.loads(response.text)
    except json.JSONDecodeError:
        return 'No price'

    quotes = parsed_response[0]['quotes']
    price_avg = sum(float(quote['price']) for quote in quotes) / len(quotes)
    return price_avg

@_celery.task
@_test_generator.create()
def get_trade_data(begin=None, end=None):
    begin = begin * 1000 if begin else None
    end = end * 1000 if end else None
    price_begin = str(end\
         if end else int(datetime.now().timestamp()))
    price_end = str(int((datetime.fromtimestamp(end / 1000) - timedelta(hours=12)).timestamp())\
        if end else int((datetime.now() - timedelta(hours=12)).timestamp()))
        
    if begin and end: 
        pipeline = [{'$match': {'createdTsMsec': {'$gte': begin, '$lte': end}}}]
    elif begin:
        pipeline = [{'$match': {'createdTsMsec': {'$gte': begin}}}]
    elif end:
        pipeline = [{'$match': {'createdTsMsec': {'$lte': end}}}]
    else:
        pipeline = []

    pipeline.extend([
        {'$match': {'trades': {'$exists': True, '$not': {'$size': 0}}}},
        {'$addFields': {'traded_quantity': {'$sum': '$trades.quantity'}}},
        {'$group': {
            '_id': '$marketAsset',
            'traded': {'$sum': '$traded_quantity'},
            'fee': {'$sum': {'$multiply': ['$traded_quantity', '$dynamicFee']}},
            'date': {'$min': '$createdTs'}
        }}
    ])

    cache = {}
    docs = list(ApiAccess.get_revenyou_db().order_v1.aggregate(pipeline))
    for doc in docs:
        doc['date'] = doc['date'].strftime('%m-%d-%Y')
        doc['traded'] = doc['traded'].to_decimal()
        fee_decimal = doc['fee'].to_decimal()

        if doc['_id'] in cache:
            doc['value'] = fee_decimal * cache[doc['_id']] if type(cache[doc['_id']]) != str else cache[doc['_id']]
            continue

        coin_value = get_historical_price(doc['_id'], price_begin, price_end)
        doc['value'] = doc['traded'] * decimal.Decimal(coin_value) if type(coin_value) == float else coin_value
        doc['fee_value'] = fee_decimal * decimal.Decimal(coin_value) if type(coin_value) == float else coin_value
        doc['fee'] = fee_decimal
        cache[doc['_id']] = decimal.Decimal(coin_value) if type(coin_value) == float else coin_value

    total_value = decimal.Decimal(.0)
    total_fee_value = decimal.Decimal(.0)
    for doc in docs:
        if doc['value'] == 'No price' or doc['fee_value'] == 'No price':
            continue
        total_value += decimal.Decimal(doc['value'])
        total_fee_value += decimal.Decimal(doc['fee_value'])
        
    docs.append({'_id': 'total', 'fee': total_fee_value, 'value': total_value, 'fee_value': total_fee_value, 'traded': total_value})

    return docs