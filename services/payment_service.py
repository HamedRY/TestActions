import json
import requests
from celery import Celery
from celery.schedules import crontab
from bson.objectid import ObjectId
import sys
sys.path.extend(['..', 'tests'])
from api_access import ApiAccess
from service_manager import ServiceManager
from datetime import datetime, timedelta
from enums import OrderStatus, TransactionType
from pymongo import UpdateOne
from generator import TestGenerator

_test_generator = TestGenerator(__name__)

service_name = ServiceManager.get_service_name(__name__)
_celery = Celery(
    service_name,
    backend=ServiceManager.get_backend(service_name),
    broker=ServiceManager.get_broker(service_name))

_margin = 15

def get_user_deposit_amount(user_id):
    data = list(ApiAccess.get_revenyou_db().deposit.aggregate([
        {'$match': {
            'user_id': user_id, 
            'order_status': OrderStatus.approved.value, 
            'bank': TransactionType.external.value}},
        {'$group': {'_id': '', 'deposit_amount': {'$sum': {'$toDouble': '$quantity'}}}}
    ]))
    
    return data[0] if len(data) > 0 else {'deposit_amount': 0}

def get_user_withdraw_amount(user_id):
    data = list(ApiAccess.get_revenyou_db().withdrawal.aggregate([
        {'$match': {
            'user_id': user_id, 
            'withdrawal_status': OrderStatus.approved.value}},
        {'$group': {'_id': '', 'withdrawal_amount': {'$sum': {'$toDouble': '$quantity_in_fiat'}}}}
    ]))

    return data[0] if len(data) > 0 else {'withdrawal_amount': 0}

def check_withdrawal_amount(withdrawal_info):
    user_id = withdrawal_info['user_id']
    amount = float(withdrawal_info['withdrawal_amount_fiat'])
    
    deposit_amount = get_user_deposit_amount(user_id)['deposit_amount']
    withdrawal_amount = get_user_withdraw_amount(user_id)['withdrawal_amount']
    assumed_balance = deposit_amount - withdrawal_amount
    if assumed_balance == 0:
        withdrawal_info['reason'] = '0 Balance'
        return False

    relative_amount = (amount / assumed_balance - 1) * 100
    if assumed_balance > 0 and relative_amount <= _margin:
        return True
        
    withdrawal_info['reason'] = 'Amount mismached'
    return False

def get_customer_screenings(customer_id):
    response = requests.get(
        '{}/customers/{}/screenings'.format(ApiAccess._swift_api_base,customer_id),
        headers={'Authorization': 'Bearer {}'.format(ApiAccess.get_swift_access_token())})
    return response.json()

def check_withdrawal_screening(withdrawal_info):
    cursor = list(ApiAccess.get_revenyou_db().sign_up.find(
        {'user_id': withdrawal_info['user_id']},
        {'swiftdil_customer_id': 1, '_id': 0}))
    maybe_swift_id = cursor[0]['swiftdil_customer_id'] \
        if len(cursor) > 0 and 'swiftdil_customer_id' in cursor[0] \
        else None
    if not maybe_swift_id:
        return True
    
    screenings = get_customer_screenings(maybe_swift_id).get('content', [])
    for scr in screenings:
        if 'outcome' not in scr:
            continue
        if any(status not in {'CLEAR', 'DISMISSED'} \
            for status in scr['outcome'].values()):
                withdrawal_info['reason'] = 'Screening failed'
                return False

    return True

def upsert_collection(raw_data, collection):
    any_stored = ApiAccess.get_skynet_db()[collection].count() != 0
    paid_ids = []
    if any_stored:
        diffs = list(ApiAccess.get_skynet_db()[collection].aggregate([
            {'$group': {'_id': '', 'ids': {'$addToSet': '$_id'}}}, 
            {'$project' : {
                'paid_ids': {'$setDifference': ['$ids', [user['_id'] for user in raw_data]]}, '_id': 0
            }}
        ]))
        paid_ids = diffs[0].get('paid_ids', []) if len(diffs) > 0 else {}
        if len(paid_ids) > 0:
            ApiAccess.get_skynet_db()[collection].remove({'_id': {'$in': paid_ids}})

    updates = [UpdateOne({'_id': item['_id']}, {'$set': item}, upsert=True) for item in raw_data]
    if (len(updates) > 0):
        ApiAccess.get_skynet_db()[collection].bulk_write(updates)
    
@_celery.task
@_test_generator.create()
def store_unpaid_users():
    unpaid_users = list(ApiAccess.get_revenyou_db().withdrawal.aggregate([
        {'$match': {
            'cleared': False, 
            'confirmed_by_user': True, 
            'dump_withdrawal': {'$exists': True, '$ne': []}
        }}]))
    data_clear, data_fail = [], []
    for item in unpaid_users:
        (data_fail, data_clear)[
            check_withdrawal_screening(item) and \
            check_withdrawal_amount(item)].append(item)

    upsert_collection(data_clear, 'clear_withdrawals')
    upsert_collection(data_fail, 'failed_withdrawals')


_celery.conf.beat_schedule = {
    'store_unpaid_users_task': {
        'task': '{}.store_unpaid_users'.format(__name__),
        'options': {'queue' : 'payment'},
        'schedule': crontab(minute=0, hour='*/1'),
    }
}