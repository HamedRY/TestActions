import json
import http.client
from celery import Celery
from celery.schedules import crontab
from bson.objectid import ObjectId
import sys
sys.path.extend(['..', 'tests'])
from api_access import ApiAccess
from service_manager import ServiceManager
from datetime import datetime, timedelta
import time
from generator import TestGenerator

_test_generator = TestGenerator(__name__)

service_name = ServiceManager.get_service_name(__name__)
_celery = Celery(
    service_name,
    backend=ServiceManager.get_backend(service_name),
    broker=ServiceManager.get_broker(service_name))

def get_last_id():
    maybe_id = list(ApiAccess.get_skynet_db().cached_contacts.find({}, {'_id': 1}).\
                        sort([('_id', -1)]).\
                        limit(1))
    if len(maybe_id) > 0:
        return maybe_id[0]['_id']

def send_contacts(contacts):
    connection = http.client.HTTPSConnection(ApiAccess._sendgrid_api_base)
    headers = {
        'authorization': 'Bearer {}'.format(ApiAccess._sendgrid_api_key),
        'content-type': "application/json"
    }
    payload = {
        'list_ids': [
            ApiAccess._sendgrid_api_list_id
        ],
        'contacts': contacts
    }

    for retry in range(1, 4):
        connection.request('PUT', '/v3/marketing/contacts', json.dumps(payload), headers)
        response = connection.getresponse()
        if response.status >= 200 and response.status < 300:
            break
        time.sleep(retry)

    connection.close()

_max_limit = 1000
@_celery.task
@_test_generator.create()
def get_new_contact_list():
    maybe_id = get_last_id()
    #Last day or from last saved
    query = {'date_added': {'$gte': int((datetime.now() - timedelta(days=1)).timestamp())}} \
        if maybe_id is None else {'_id': {'$gt': maybe_id}}
    query.update({'email': {'$exists': True, '$ne': ''}})

    contacts = []
    skip_ = 0
    while True:
        partial = list(ApiAccess.get_revenyou_db().sign_up.\
            find(query, {'email': 1, 'first_name': 1, 'last_name': 1, 'nationality': 1}).\
            sort('_id', -1).\
            skip(skip_).\
            limit(_max_limit))
        skip_ += _max_limit
        if len(partial) == 0:
            break
        contacts.extend(partial)
    
    if len(contacts) == 0:
        return

    ApiAccess.get_skynet_db().cached_contacts.drop()
    ApiAccess.get_skynet_db().cached_contacts.insert_many(contacts)

    for contact in contacts:
        contact['country'] = contact.pop('nationality', '').upper()
        del contact['_id']
    send_contacts(contacts)


_celery.conf.beat_schedule = {
    'get_new_contact_list_task': {
        'task': '{}.get_new_contact_list'.format(__name__),
        'options': {'queue' : 'sendgrid'},
        'schedule': crontab(minute=0, hour='*/1'),
    }
}