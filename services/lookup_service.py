from celery import Celery
from celery.schedules import crontab
from bson.objectid import ObjectId
from service_manager import ServiceManager
from api_access import ApiAccess
from enums import OrderStatus
import time
import sys
sys.path.extend(['..', 'tests'])
from generator import TestGenerator

_test_generator = TestGenerator(__name__)

service_name = ServiceManager.get_service_name(__name__)
_celery = Celery(
    service_name,
    backend=ServiceManager.get_backend(service_name),
    broker=ServiceManager.get_broker(service_name))

_default_partition_size = 500 #records
_default_cool_off_timeout = 3 #seconds

def get_last_id(db):
    maybe_id = list(db.find({}, {'_id': 1}).\
                        sort([('_id', -1)]).\
                        limit(1))
    if len(maybe_id) > 0:
        return maybe_id[0]['_id']

@_celery.task
@_test_generator.create()
def get_withdrawal_deposit_lookup():
    while True:
        pipeline = [
            {'$match': {'withdrawal_status': OrderStatus.approved.value}},
            {'$sort': {'_id': 1}},
            {'$limit': _default_partition_size},
            {'$lookup':{
                'from': 'deposit',
                'let': {'user_id': '$user_id', 'status': '$withdrawal_status'},
                'pipeline':
                    [{'$match': 
                        {'$expr': {'$and':[
                            {'$eq': [ '$user_id',  '$$user_id' ]}, 
                            {'$eq': [ '$order_status', '$$status']}]
                        }}
                    }],
                'as': 'deposits'
            }}
        ]
        maybe_id = get_last_id(ApiAccess.get_skynet_db().withdrawal_deposit)
        if maybe_id:
            match_index = next((index for (index, stage) in enumerate(pipeline) if '$match' in stage), None)
            if match_index is None:
                raise KeyError('No $match provided')
            pipeline[match_index]['$match'].update({'_id': {'$gt': maybe_id}})

        data = list(ApiAccess.get_revenyou_db().withdrawal.aggregate(pipeline))
        if len(data) == 0:
            break
        ApiAccess.get_skynet_db().withdrawal_deposit.insert_many(data)
        time.sleep(_default_cool_off_timeout)


@_celery.task
@_test_generator.create()
def get_deposit_withdrawal_lookup():
    while True:
        pipeline = [
            {'$match': {'order_status': OrderStatus.approved.value}},
            {'$sort': {'_id': 1}},
            {'$limit': _default_partition_size},
            {'$lookup':{
                'from': 'withdrawal',
                'let': {'user_id': '$user_id', 'status': '$order_status'},
                'pipeline':
                    [{'$match': 
                        {'$expr': {'$and':[
                            {'$eq': [ '$user_id',  '$$user_id' ]}, 
                            {'$eq': [ '$withdrawal_status', '$$status']}]
                        }}
                    }],
                'as': 'withdrawals'
            }}
        ]
        maybe_id = get_last_id(ApiAccess.get_skynet_db().deposit_withdrawal)
        if maybe_id:
            match_index = next((index for (index, stage) in enumerate(pipeline) if '$match' in stage), None)
            if match_index is None:
                raise KeyError('No $match provided')
            pipeline[match_index]['$match'].update({'_id': {'$gt': maybe_id}})

        data = list(ApiAccess.get_revenyou_db().deposit.aggregate(pipeline))
        if len(data) == 0:
            break
        ApiAccess.get_skynet_db().deposit_withdrawal.insert_many(data)
        time.sleep(_default_cool_off_timeout)

    
_celery.conf.beat_schedule = {
    'get_withdrawal_deposit_lookup_task': {
        'task': '{}.get_withdrawal_deposit_lookup'.format(__name__),
        'options': {'queue' : 'lookup'},
        'schedule': crontab(minute=0, hour='*/5'),
    },
    'get_deposit_withdrawal_lookup_task': {
        'task': '{}.get_deposit_withdrawal_lookup'.format(__name__),
        'options': {'queue' : 'lookup'},
        'schedule': crontab(minute=0, hour='*/5')
    }
}