import bcrypt
from celery import Celery
import sys
sys.path.extend(['..', 'tests'])
from api_access import ApiAccess
from pymongo import MongoClient
from service_manager import ServiceManager
import json
from bson import json_util
from bson.objectid import ObjectId
import base64
import onetimepass
import os
from enums import Mode, LoginStates
from datetime import datetime, timedelta
import utils
from generator import TestGenerator

_test_generator = TestGenerator(__name__)

service_name = ServiceManager.get_service_name(__name__)
_celery = Celery(
    service_name,
    backend=ServiceManager.get_backend(service_name),
    broker=ServiceManager.get_broker(service_name))

@_celery.task
@_test_generator.create()
def create_user(username, password, role, permissions):
    existing_user = ApiAccess.get_skynet_db().users.find_one({'username': username.upper()})

    if existing_user is None:
        ApiAccess.get_skynet_db().users.insert_one({'username': username.upper(), 'password': bcrypt.hashpw(
            password.encode('utf-8'), bcrypt.gensalt()), 'role': role, 'permissions': permissions})
        return username.upper()

    return None

def check_2fa(data, token):
    if 'secret' not in data:
        secret = base64.b32encode(os.urandom(10)).decode('utf-8')
        ApiAccess.get_skynet_db().users.update_one(
            {'username': data['username'].upper()},
            {'$set': {'secret': secret, '2fa_active': False}})
        return {
            'status': LoginStates.active_2fa.value,
            'qr_code': 'otpauth://totp/Skynet:{}?secret={}&issuer=Skynet' \
            .format(data['username'], secret)}

    is_token_valid = onetimepass.valid_totp(token, data['secret'])
    if not is_token_valid: 
        return {
            'status': LoginStates.active_2fa.value,
            'qr_code': 'otpauth://totp/Skynet:{}?secret={}&issuer=Skynet' \
            .format(data['username'], data['secret'])} if not data['2fa_active'] \
            else {'status': LoginStates.fail.value}

    return None

def update_2fa_activation(data):
    if not data['2fa_active']:
        ApiAccess.get_skynet_db().users.update_one(
            {'username': data['username'].upper()},
            {'$set': {'2fa_active': True}})

_fail_retry_max = 3
_fail_retry_timeout = 5 #minutes
def update_fail_retry(data):
    fail_count = data.get('fail_count', 0) + 1
    ApiAccess.get_skynet_db().users.update_one(
            {'username': data['username'].upper()},
            {'$set': {'fail_count': fail_count}})
    if fail_count == _fail_retry_max:
        timeout_until = int((datetime.now() + timedelta(minutes=_fail_retry_timeout)).timestamp())
        ApiAccess.get_skynet_db().users.update_one(
            {'username': data['username'].upper()},
            {'$set': {'fail_count': 0, 'timeout_until': timeout_until}})
        return {
            'status': LoginStates.retry_fail.value,
            'timeout': utils.utc_ts_to_local_str(timeout_until)}
    elif fail_count > _fail_retry_max:
        ApiAccess.get_skynet_db().users.update_one(
            {'username': data['username'].upper()},
            {'$set': {'fail_count': 0}})
        return {
            'status': LoginStates.retry_fail.value,
            'timeout': utils.utc_ts_to_local_str(data.get('timeout_until', 0))}

    return {'status': LoginStates.fail.value}

def check_fail_retry(data):
    timeout = data.get('timeout_until', 0)
    if timeout > int(datetime.now().timestamp()):
        return {
            'status': LoginStates.retry_fail.value,
            'timeout': utils.utc_ts_to_local_str(timeout)}
    return None

def successfull_login(data):
    del data['password']
    data['status'] = LoginStates.success.value
    ApiAccess.get_skynet_db().users.update(
        {'username': data['username'].upper()},
        {'$unset': {'fail_count': 1, 'timeout_until': 1}})
    return data

@_celery.task
@_test_generator.create()
def login(username, password, token):
    login_user = ApiAccess.get_skynet_db().users.find_one(
        {'username': username.upper()})
        
    if login_user is None:
        return None
    
    result_retry_fail = check_fail_retry(login_user)
    if result_retry_fail is not None:
        return result_retry_fail

    login_user['_id'] = str(login_user['_id'])
    if bcrypt.hashpw(password.encode('utf-8'), login_user['password']) != login_user['password']:
        return update_fail_retry(login_user)

    if os.environ['APP_RUN_ENV'] == Mode.development.value: #skip 2FA on dev
        return successfull_login(login_user)

    result_2fa = check_2fa(login_user, token)
    if result_2fa is not None:
        return result_2fa

    update_2fa_activation(login_user)

    return successfull_login(login_user)

@_celery.task
@_test_generator.create()
def change_password(user_id, new_password):
    ApiAccess.get_skynet_db().users.update_one(
        {'_id': ObjectId(user_id)},
        {'$set': {'password': bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())}})


@_celery.task
@_test_generator.create()
def reset_2fa_secret(user_id):
    ApiAccess.get_skynet_db().users.update(
        {'_id': ObjectId(user_id)},
        {'$unset': {'secret': 1, '2fa_active': 1}})

@_celery.task
@_test_generator.create()
def delete_user(user_id):
    ApiAccess.get_skynet_db().users.delete_one({'_id': ObjectId(user_id)})


@_celery.task
@_test_generator.create()
def find_user(username):
    user = ApiAccess.get_skynet_db().users.find_one({'username': username})
    return user

@_celery.task
@_test_generator.create()
def get_users():
    cursor = ApiAccess.get_skynet_db().users.find({}, {'username': 1, 'role': 1, 'permissions': 1})
    result = {'content': []}
    for document in cursor:
        document['_id'] = str(document['_id'])
        result['content'].append(document)

    return result

@_celery.task
@_test_generator.create()
def modify_user(user_id, permissions, role):
    ApiAccess.get_skynet_db().users.update_one(
        {'_id': ObjectId(user_id)},
        {'$set': {'permissions': permissions, 'role': role}})

@_celery.task
@_test_generator.create()
def suspend_user(user_id):
    permissions = ApiAccess.get_skynet_db().users.find_one({'_id': ObjectId(user_id)}, {'_id': 0, 'permissions': 1})['permissions']
    if 'SUSPENDED' in permissions:
        return

    ApiAccess.get_skynet_db().users.update_one(
        {'_id': ObjectId(user_id)},
        {'$push': {'permissions': 'SUSPENDED'}})

@_celery.task
@_test_generator.create()
def unsuspend_user(user_id):
    ApiAccess.get_skynet_db().users.update_one(
        {'_id': ObjectId(user_id)},
        {'$pull': {'permissions': 'SUSPENDED'}})