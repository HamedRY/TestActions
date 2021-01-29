import json
import requests
from requests.auth import HTTPBasicAuth
from pymongo import MongoClient
import os
from enums import Mode
import utils

def get_secret(secret_name):
    path = '/run/secrets/{}'.format(secret_name)
    if not os.path.exists(path):
        return None

    with open(path, 'r') as secret:
        return str(secret.read().strip())

    return None

class ApiAccess:
    if os.environ['APP_RUN_ENV'] == Mode.development.value:
        _mongo_client = MongoClient('mongodb://localhost:27017', 
        connect=False)
        _skynet_db = _mongo_client['Skynet']
        _swift_api_base = _skynet_db.api.find_one()['swift_api_base']
        _swift_client_id = _skynet_db.api.find_one()['swift_client_id']
        _swift_client_key = _skynet_db.api.find_one()['swift_client_key']
        _revenyou_api_base = _skynet_db.api.find_one()['revenyou_api_base']
        _revenyou_test_api_base = _skynet_db.api.find_one()['revenyou_test_api_base']
        _revenyou_api_key_quotes = _skynet_db.api.find_one()['revenyou_api_key']
        _revenyou_api_key_cleared = _skynet_db.api.find_one()['revenyou_api_key_2']
        _revenyou_api_key_bot_desc = _skynet_db.api.find_one()['revenyou_api_key_3']
        _revenyou_api_key_add_note = _skynet_db.api.find_one()['revenyou_api_key_4']
        _revenyou_api_key_change_article = _skynet_db.api.find_one()['revenyou_api_key_5']
        _sendgrid_api_base = _skynet_db.api.find_one()['sendgrid_api_base']
        _sendgrid_api_key = _skynet_db.api.find_one()['sendgrid_api_key']
        _sendgrid_api_list_id = _skynet_db.api.find_one()['sendgrid_api_list_id']
    elif os.environ['APP_RUN_ENV'] == Mode.production.value:
        _mongo_client = MongoClient('mongodb://skynet-mongodb:27017', 
            connect=False,
            username=get_secret('MONGO_ROOT_USERNAME'), 
            password=get_secret('MONGO_ROOT_PASSWORD'))
        _skynet_db = _mongo_client[os.environ['MONGO_INITDB_DATABASE']]
        _swift_api_base = os.environ['SWIFT_API_ADDRESS']
        _swift_client_id = get_secret('SWIFT_CLIENT_ID')
        _swift_client_key = get_secret('SWIFT_CLIENT_KEY')
        _revenyou_api_base = os.environ['REVENYOU_API_BASE']
        _revenyou_test_api_base = os.environ['REVENYOU_TEST_API_BASE']
        _revenyou_api_key_quotes = get_secret('REVENYOU_API_KEY')
        _revenyou_api_key_cleared = get_secret('REVENYOU_API_KEY_2')
        _revenyou_api_key_bot_desc = get_secret('REVENYOU_API_KEY_3')
        _revenyou_api_key_add_note = get_secret('REVENYOU_API_KEY_4')
        _revenyou_api_key_change_article = get_secret('REVENYOU_API_KEY_5')
        _sendgrid_api_base = os.environ['SENDGRID_API_ADDRESS']
        _sendgrid_api_key = get_secret('SENDGRID_API_KEY')
        _sendgrid_api_list_id = get_secret('SENDGRID_API_LIST_ID')
    else:
        raise ValueError('Indicate run envinroment (DEV/PROD)')

    @staticmethod
    @utils.static_vars(client=None)
    def get_skynet_db():
        db_name = os.environ.get('MONGO_INITDB_DATABASE', 'Skynet')
        if ApiAccess.get_skynet_db.client:
            return ApiAccess.get_skynet_db.client[db_name]

        if os.environ['APP_RUN_ENV'] == Mode.development.value:
            ApiAccess.get_skynet_db.client = MongoClient('mongodb://localhost:27017', connect=False)
        elif os.environ['APP_RUN_ENV'] == Mode.production.value:
            ApiAccess.get_skynet_db.client = MongoClient('mongodb://skynet-mongodb:27017', connect=False,
                username=get_secret('MONGO_ROOT_USERNAME'), 
                password=get_secret('MONGO_ROOT_PASSWORD'))
        else:
            raise ValueError('Indicate run envinroment (DEV/PROD)')

        return ApiAccess.get_skynet_db.client[db_name]


    @staticmethod
    @utils.static_vars(client=None)
    def get_revenyou_db():
        db_name = 'revenyou'
        if ApiAccess.get_revenyou_db.client:
            return ApiAccess.get_revenyou_db.client[db_name]

        if os.environ['APP_RUN_ENV'] == Mode.development.value:
            db_creds = ApiAccess.get_skynet_db().revenyou_db.find_one({'dev': True})
            ApiAccess.get_revenyou_db.client = MongoClient('mongodb://{}'.format(db_creds['address']), connect=False,
                readPreference='secondary')
        elif os.environ['APP_RUN_ENV'] == Mode.production.value:
            ApiAccess.get_revenyou_db.client = MongoClient('mongodb+srv://{}'.format(os.environ['REVENYOU_DB_ADDRESS']), connect=False,
                username=get_secret('REVENYOU_DB_USERNAME'), 
                password=get_secret('REVENYOU_DB_PASSWORD'),
                readPreference='secondary')
        else:
            raise ValueError('Indicate run envinroment (DEV/PROD)')

        return ApiAccess.get_revenyou_db.client[db_name]

    @staticmethod
    def get_swift_access_token():
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        response = requests.post(
            ApiAccess._swift_api_base +
            '/oauth2/token',
            headers=headers,
            auth=HTTPBasicAuth(
                ApiAccess._swift_client_id,
                ApiAccess._swift_client_key))
        response.raise_for_status()
        return response.json()['access_token']
