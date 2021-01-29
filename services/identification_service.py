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
@_internal_cache.cached()
@_test_generator.create()
def get_identifications(args):
    headers = {
        'Authorization': 'Bearer {}'.format(ApiAccess.get_swift_access_token())
    }

    args_clean = {k: v for k, v in args.items() if v is not None}
    response = requests.get('{}/search/identifications?{}'.format(ApiAccess._swift_api_base,
                                                                  parse.urlencode(args_clean)),
                            headers=headers)
    response.raise_for_status()
    return response.json()


@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_identifications_count(args):
    headers = {
        'Authorization': 'Bearer {}'.format(ApiAccess.get_swift_access_token())
    }

    args['size'] = 1
    args_clean = {k: v for k, v in args.items() if v is not None}
    response = requests.get(
        '{}/search/identifications?{}'.format(ApiAccess._swift_api_base,
                                 parse.urlencode(args_clean)),
        headers=headers)
    response.raise_for_status()
    return response.json().get('total_elements', None)


@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def find_identification(customer_id, identification_id):
    headers = {
        'Authorization': 'Bearer {}'.format(ApiAccess.get_swift_access_token())
    }

    response = requests.get(
        '{}/customers/{}/identifications/{}'.format(
            ApiAccess._swift_api_base,
            customer_id,
            identification_id),
        headers=headers)
    response.raise_for_status()
    return response.json()


@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def find_latest_identification(customer_id):
    headers = {
        'Authorization': 'Bearer {}'.format(ApiAccess.get_swift_access_token())
    }

    response = requests.get(
        '{}/customers/{}/identifications'.format(
            ApiAccess._swift_api_base,
            customer_id),
        headers=headers)
    response.raise_for_status()

    identifications = response.json()['content']

    if len(identifications) > 0:
        latest_identification = max(
            identifications,
            key=lambda item: item['created_at'])

        return latest_identification

    return ''
