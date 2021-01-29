from flask import jsonify
from enums import Mode
from api_access import ApiAccess
from datetime import datetime
import traceback
import os

class InvalidUsage(Exception):
    _status_code = 400
    _default_error_msg = {
        400: 'Invalid request',
        401: 'Permission denied',
        403: 'Account suspended',
        422: 'Database key error',
        500: 'Internal generic error',
        503: 'Service down'
    }

    def __init__(self, code=None, message=None, payload=None):
        Exception.__init__(self)
        self.stack_trace = traceback.format_exc()
        self.payload = None
        self.message = message
        if code:
            self._status_code = code
        if not message:
            self.message = self._default_error_msg[self._status_code]
        if payload:
            self.create_payload(payload)

        ApiAccess._skynet_db.backend_errors.insert({
            'time': int(datetime.now().timestamp()),
            'error_code': (self._status_code or '-'),
            'message': (self.message or '-'),
            'payload': (self.payload or '-'),
            'stack_trace': (self.stack_trace or '-')
        })

    def create_payload(self, payload):
        if os.environ['APP_RUN_ENV'] != Mode.development.value:
            return None
       
        if self._status_code == 400:
            multi_param = isinstance(payload, list)
            self.payload = {'payload': '{} parameter{} {} mandatory'.format(
                payload, 
                's' if multi_param else '', 
                'are' if multi_param else 'is')}
            return
        
        self.payload = {'payload': payload}

        
    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv