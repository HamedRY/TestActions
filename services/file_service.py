import json
import requests
import os
from celery import Celery
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
def get_documents(customer_id):
    headers = {
        'Authorization': 'Bearer {}'.format(ApiAccess.get_swift_access_token())
    }

    response = requests.get(
        '{}/customers/{}/documents'.format(ApiAccess._swift_api_base, customer_id), headers=headers)
    response.raise_for_status()
    return response.json() 


@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def download_file(file_id):
    headers = {
        'Authorization': 'Bearer {}'.format(ApiAccess.get_swift_access_token())
    }

    response = requests.get(
        '{}/files/{}?output=BASE64'.format(ApiAccess._swift_api_base, file_id), headers=headers)
    response.raise_for_status()
    return response.json()

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def download_report(report_id):
    headers = {
        'Authorization': 'Bearer {}'.format(ApiAccess.get_swift_access_token())
    }
    response = requests.get('{}/reports/{}/pdf/download'.format(ApiAccess._swift_api_base, report_id), headers=headers)
    response.raise_for_status()
    with open("static/assets/Report" + report_id + ".pdf","wb") as Report:
        Report.write(response.content)
    os.remove("static/assets/Report" + report_id + ".pdf")
    return response.json()

@_celery.task
@_internal_cache.cached()
@_test_generator.create()
def get_reports():
    headers = {
        'Authorization': 'Bearer {}'.format(ApiAccess.get_swift_access_token())
    }

    response = requests.get(
        '{}/reports'.format(ApiAccess._swift_api_base), headers=headers)
    response.raise_for_status()
    return response.json()