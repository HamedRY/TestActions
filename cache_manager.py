import redis
import json
import os
from enums import Mode
from functools import wraps
import time

class CacheManger:
    def __init__(self):
        self.address = 'localhost' if os.environ['APP_RUN_ENV'] == Mode.development.value else 'skynet-redis'
        self.client = redis.Redis(
            host=self.address,
            port=6379, 
            password='')

    def get_permissions(self, key):
        return self.client.get('permissions:{}'.format(key))

    def set_permissions(self, key, permissions, expiration_s):
        self.client.set('permissions:{}'.format(key), json.dumps(permissions), ex=expiration_s)

    def get_username(self, key):
        return self.client.get('username:{}'.format(key))

    def set_username(self, key, username, expiration_s):
        self.client.set('username:{}'.format(key), username, ex=expiration_s)
    
    def revoke_permissions(self, key):
        self.client.delete('permissions:{}'.format(key))

    def get_token(self, user_id):
        return self.client.get('token:{}'.format(user_id))

    def set_token(self, user_id, token, expiration_s):
        self.client.set('token:{}'.format(user_id), token, ex=expiration_s)
    
    def set_new_gold_members(self, ids, expiration_s):
        self.client.set('new_gold', ids, ex=expiration_s)

    def get_new_gold_members(self):
        return self.client.get('new_gold')

    def set_lost_gold_members(self, ids, expiration_s):
        self.client.set('lost_gold', ids, ex=expiration_s)

    def get_lost_gold_members(self):
        return self.client.get('lost_gold')

    def cached(self, expiration_s=300): #5min default
        def wrap(f, *args, **kwargs):
            @wraps(f)
            def wrapped_f(*args, **kwargs):
                args_key = '{}{}'.format(''.join(map(str, args)), ''.join(map(str, kwargs.values())))
                now_ts = int(time.time())

                cached = self.client.hget(f.__name__, args_key)
                cached = cached if cached is None else json.loads(cached)
                if cached is not None and \
                    now_ts - cached['ts'] <= expiration_s:
                    return cached['res']

                result = f(*args, **kwargs)
                self.client.hset(f.__name__, args_key, \
                    json.dumps({'res': result, 'ts': now_ts}))
                return result
            return wrapped_f
        return wrap