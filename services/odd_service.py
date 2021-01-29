import json
import requests
from celery import Celery
from urllib import parse
import sys
sys.path.extend(['..', 'tests'])
from api_access import ApiAccess
from service_manager import ServiceManager
from cache_manager import CacheManger
from generator import TestGenerator

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
def get_odds(customer_id, args):
    headers = {
        'Authorization': 'Bearer {}'.format(ApiAccess.get_swift_access_token())
    }

    args_clean = {k: v for k, v in args.items() if v is not None}
    response = requests.get(
        '{}/customers/{}/odd?{}'.format(ApiAccess._swift_api_base,
                                        customer_id,
                                        parse.urlencode(args_clean)),
        headers=headers)
    response.raise_for_status()
    return response.json()


@_celery.task
@_test_generator.create()
def create_odd(customer_id, args):
    headers = {
        'Authorization': 'Bearer {}'.format(ApiAccess.get_swift_access_token()),
        'content-type': 'application/json'
    }

    payload = {'scope' : json.loads(args['scope']), 'frequency' : args['frequency']}
    response = requests.post(
        '{}/customers/{}/odd'.format(
            ApiAccess._swift_api_base,
            customer_id),
        data=json.dumps(payload),
        headers=headers)
    response.raise_for_status()
    return response.json()


@_celery.task
@_test_generator.create()
def delete_odd(customer_id, odd_id):
    headers = {
        'Authorization': 'Bearer {}'.format(ApiAccess.get_swift_access_token())
    }

    response = requests.delete(
        '{}/customers/{}/odd/{}'.format(
            ApiAccess._swift_api_base,
            customer_id,
            odd_id),
        headers=headers)
    response.raise_for_status()
    return response.json()


@_celery.task
@_test_generator.create()
def edit_odd(customer_id, odd_id, args):
    headers = {
        'Authorization': 'Bearer {}'.format(ApiAccess.get_swift_access_token()),
        'content-type': 'application/json'
    }

    args_clean = {k: v for k, v in args.items() if v is not None}
    response = requests.put(
        '{}/customers/{}/odd/{}'.format(
            ApiAccess._swift_api_base,
            customer_id,
            odd_id),
        data=json.dumps(args_clean),
        headers=headers)
    response.raise_for_status()
    return response.json()
