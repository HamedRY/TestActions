from gevent import monkey; monkey.patch_all()
from flask import Flask, session, request, abort, jsonify
from flask_caching import Cache
from flask_cors import CORS, cross_origin
from task_manager import TaskManager
from internal_task_manager import InternalTaskManager
from cache_manager import CacheManger
from error_handler import InvalidUsage

from functools import wraps
from datetime import datetime, timedelta
from uuid import uuid4
from functools import wraps
from flask_jwt_extended import (
    JWTManager, jwt_required, create_access_token,
    jwt_refresh_token_required, create_refresh_token,
    get_jwt_identity)
from service_manager import ServiceManager
import json
import os
from enums import Mode, LoginStates, CustomerType, GoldType, DepositStage

_core = Flask(__name__)
_core.secret_key = os.urandom(24)
_core.config['CORS_HEADERS'] = 'Content-Type'
_cache = Cache(config={'CACHE_TYPE': 'filesystem', 'CACHE_DIR': '/tmp'}) #filesystem catching instead of redis since redis catch clear might truncate the whole data
_cache.init_app(_core)
_default_cache_timeout = 300 #5mins
_short_cache_timeout = 60 #1min
_long_cache_timeout = 28800 #8hours

_allowed_origin = 'http://localhost:4200' if os.environ['APP_RUN_ENV'] == Mode.development.value else 'https://backpack.revenyou.io'
CORS(_core, resources={r"/v2/api": {"origins": _allowed_origin}})

_jwt = JWTManager(_core)

_task_manager = TaskManager()
_internal_task_manager = InternalTaskManager()
_internal_cache = CacheManger()

def permission_required(access):
    def wrap(f, *args, **kwargs):
        @wraps(f)
        def wrapped_f(*args, **kwargs):
            key = request.headers.environ['HTTP_AUTHORIZATION'].split(' ')[1]
            json_perms = _internal_cache.get_permissions(key)
            if not json_perms:
                raise InvalidUsage(403)

            permissions = json.loads(json_perms)
            if 'SUSPENDED' in permissions:
                raise InvalidUsage(403)
            if set(access) <= set(permissions):
                _internal_task_manager.record_action(
                    f.__name__,
                    _internal_cache.get_username(key).decode("utf-8"))
                return f(*args, **kwargs)
            raise InvalidUsage(401)
        return wrapped_f
    return wrap

@_core.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error._status_code
    return response

@_core.route('/v2/api/internal/customers/notes/add/<user_id>', methods=['POST'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
def save_note(user_id):
    try:
        title = request.json.get('title', '')
        note = request.json.get('note', '')
        creator = _internal_cache.get_username(
            key = request.headers.environ['HTTP_AUTHORIZATION'].split(' ')[1]
        ).decode("utf-8")
        response = _internal_task_manager.save_note(user_id, creator, title, note)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))


    return json.dumps(response)

@_core.route('/v2/api/internal/customers/notes/<user_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_notes(user_id):
    try:
        notes = _internal_task_manager.get_notes(user_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return notes

@_core.route('/v2/api/internal/customers', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_customers_internal():
    try:
        begin = int(request.args['begin']) if request.args.get('begin', '').isnumeric() else None
        end = int(request.args['end']) if request.args.get('end', '').isnumeric() else None
        customers = _internal_task_manager.get_customers(
            size=request.args.get('size', 20),
            page=request.args.get('page', 0),
            sort=request.args.get('sort', 'date_added'),
            order=request.args.get('order', 'desc'),
            begin=begin,
            end=end,
            name=request.args.get('name', None),
            advanced=request.args.get('advanced', ''))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return customers

@_core.route('/v2/api/internal/customers/corporate', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_corporates_internal():
    try:
        begin = int(request.args['begin']) if request.args.get('begin', '').isnumeric() else None
        end = int(request.args['end']) if request.args.get('end', '').isnumeric() else None
        corporates = _internal_task_manager.get_corporates(
            size=request.args.get('size', 20),
            page=request.args.get('page', 0),
            sort=request.args.get('sort', 'date_added'),
            order=request.args.get('order', 'desc'),
            begin=begin,
            end=end,
            name=request.args.get('name', None))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return corporates

@_core.route('/v2/api/internal/customers/funded/new', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_funded_accounts():
    try:
        begin = int(request.args['begin']) if request.args.get('begin', '').isnumeric() else None
        end = int(request.args['end']) if request.args.get('end', '').isnumeric() else None
        customers = _internal_task_manager.get_funded_accounts(
            size=request.args.get('size', 20),
            page=request.args.get('page', 0),
            sort=request.args.get('sort', 'date_added'),
            order=request.args.get('order', 'desc'),
            begin=begin,
            end=end)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return customers

@_core.route('/v2/api/internal/customers/gold', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_gold_members():
    try:
        begin = int(request.args['begin']) if request.args.get('begin', '').isnumeric() else None
        end = int(request.args['end']) if request.args.get('end', '').isnumeric() else None
        gold_members = _internal_task_manager.get_gold_members(
            size=request.args.get('size', 20),
            page=request.args.get('page', 0),
            sort=request.args.get('sort', 'date_added'),
            order=request.args.get('order', 'desc'),
            begin=begin,
            end=end,
            name=request.args.get('name', None),
            type_=request.args.get('type_', GoldType.current.value))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return gold_members


@_core.route('/v2/api/internal/customers/top', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_top_customers():
    try:
        top_customers = _internal_task_manager.get_top_customers()
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return top_customers

@_core.route('/v2/api/internal/customers/geographic/count', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_long_cache_timeout, query_string=True)
def get_geographic_customer_count():
    try:
        result = _internal_task_manager.get_geographic_customer_count()
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return result

@_core.route('/v2/api/internal/customers/periodic/count', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_long_cache_timeout, query_string=True)
def get_periodical_customer_count():
    try:
        result = _internal_task_manager.get_periodical_customer_count()
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return result

@_core.route('/v2/api/internal/transactions/periodic', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_long_cache_timeout, query_string=True)
def get_periodical_transaction_amount():
    try:
        result = _internal_task_manager.get_periodical_transaction_amount()
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return result

@_core.route('/v2/api/internal/transactions/deposits/categories', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_long_cache_timeout, query_string=True)
def get_deposit_categories():
    try:
        begin = int(request.args['begin']) if request.args.get('begin', '').isnumeric() else None
        end = int(request.args['end']) if request.args.get('end', '').isnumeric() else None
        result = _internal_task_manager.get_deposit_categories(
            stage=request.args.get('stage', DepositStage.all.value),
            begin=begin, 
            end=end)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return result

@_core.route('/v2/api/internal/feedbacks', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_feedbacks():
    try:
        feedbacks = _internal_task_manager.get_feedbacks(
            size=request.args.get('size', 20),
            page=request.args.get('page', 0),
            sort=request.args.get('sort', 'version'),
            order=request.args.get('order', 'desc'),
            name=request.args.get('name', None))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return feedbacks

@_core.route('/v2/api/internal/customers/timeline/<user_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)  
def get_user_timeline(user_id):
    try:
        result = _internal_task_manager.get_user_timeline(user_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return result


@_core.route('/v2/api/internal/customers/csv', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_customers_csv_exportable():
    if 'cols' not in request.args or 'max' not in request.args:
        raise InvalidUsage(code=400, payload=['cols', 'max'])
    try:
        cols = request.args.get('cols', '').split(',')
        if len(cols) == 0:
            raise InvalidUsage(code=400, message='Columns cannot be empty')

        begin = int(request.args['begin']) if request.args.get('begin', '').isnumeric() else None
        end = int(request.args['end']) if request.args.get('end', '').isnumeric() else None
        customers = _internal_task_manager.get_customers_csv_exportable(
            max=request.args.get('max', 10),
            cols=cols,
            begin=begin,
            end=end)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return customers

@_core.route('/v2/api/internal/strategies/csv', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_strategies_csv_exportable():
    if 'cols' not in request.args:
        raise InvalidUsage(code=400, payload='cols')
    try:
        cols = request.args.get('cols', '').split(',')
        if len(cols) == 0:
            raise InvalidUsage(code=400, message='Columns cannot be empty')

        strategies = _internal_task_manager.get_strategies_csv_exportable(cols=cols)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return strategies

@_core.route('/v2/api/internal/customers/swift_id/<user_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_swift_id(user_id):
    try:
        swift_id = _internal_task_manager.get_swift_id(user_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return swift_id

@_core.route('/v2/api/internal/customers/<user_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_customer_internal(user_id):
    try:
        customer = _internal_task_manager.get_customer(user_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return customer

@_core.route('/v2/api/internal/customers/corporate/<user_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_corporte_internal(user_id):
    try:
        customer = _internal_task_manager.get_customer(user_id, True)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return customer

@_core.route('/v2/api/internal/customers/users/stats', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_user_count():
    try:
        begin = int(request.args['begin']) if request.args.get('begin', '').isnumeric() else None
        end = int(request.args['end']) if request.args.get('end', '').isnumeric() else None
        stats = _internal_task_manager.get_user_stats(begin, end)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return stats


@_core.route('/v2/api/customers', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_customers():
    try:
        customers = _task_manager.get_customers(
            size=request.args.get('size', 20),
            page=request.args.get('page', 0),
            sort=request.args.get('sort', 'createdDate'),
            order=request.args.get('order', 'desc'),
            begin=request.args.get('begin', None),
            end=request.args.get('end', None),
            name=request.args.get('name', None))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return customers


@_core.route('/v2/api/customers/count', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_customers_count():
    try:
        count = _task_manager.get_customers_count(
            request.args.get('country', None))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(count)

@_core.route('/v2/api/customers/<customer_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_customer(customer_id):
    try:
        customer = _task_manager.get_customer(customer_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return customer

@_core.route('/v2/api/customers/risk/<customer_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_customer_risk(customer_id):
    try:
        risk = _task_manager.get_customer_risk(customer_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return risk

@_core.route('/v2/api/customers/onboard/<customer_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ', 'WRITE'])
def board_customer(customer_id):
    if 'state' not in request.args:
        raise InvalidUsage(code=400, payload='state')

    try:
        #to be implemented
        result = _task_manager.board_customer(
            '',
            customer_id,
            request.args.get("state"))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return result


@_core.route('/v2/api/customers/first_deposits', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_first_deposits():
    try:
        deposits = _task_manager.get_first_deposits(
            request.args.get('begin_year', None), request.args.get('begin_month', None))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return deposits


@_core.route('/v2/api/screenings/count', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_screenings_count():
    try:
        count = _task_manager.get_screening_count(
            status=request.args.get('status', 'DONE'),
            scope=request.args.get('scope', None))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(count)


@_core.route('/v2/api/screenings/scopes/counts', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_screening_all_scopes():
    try:
        counts = _task_manager.get_screening_all_scopes(
            request.args.get('status', 'DONE'))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(counts)


@_core.route('/v2/api/screenings/create/<customer_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ', 'WRITE'])
def create_screening(customer_id):
    if 'scope' not in request.args:
        raise InvalidUsage(code=400, payload='scope')

    try:
        result = _task_manager.create_screening(
            customer_id, request.args.get("scope"))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(result)


@_core.route('/v2/api/screenings/<customer_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_screenings(customer_id):
    try:
        result = _task_manager.get_screenings(customer_id,
            size=request.args.get('size', 20),
            page=request.args.get('page', 0),
            sort=request.args.get('sort', 'createdDate'),
            order=request.args.get('order', 'desc'),
            begin=request.args.get('begin', None),
            end=request.args.get('end', None))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(result)

@_core.route('/v2/api/screenings', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def search_screenings():
    try:
        result = _task_manager.search_screenings(
            size=request.args.get('size', 20),
            page=request.args.get('page', 0),
            sort=request.args.get('sort', 'createdDate'),
            order=request.args.get('order', 'desc'),
            begin=request.args.get('begin', None),
            end=request.args.get('end', None),
            name=request.args.get('name', None),
            scope=request.args.get('scope', None))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(result)

@_core.route('/v2/api/screenings/find/<customer_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def find_screening(customer_id):
    if 'screening_id' not in request.args:
        raise InvalidUsage(code=400, payload='screening_id')
    try:
        screening = _task_manager.find_screening(
            customer_id, request.args.get('screening_id'))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))


    return screening


@_core.route('/v2/api/odd/<customer_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_odds(customer_id):
    try:
        result = _task_manager.get_odds(
            customer_id,
            request.args.get('size', 7),
            request.args.get('scope', None),
            request.args.get('begin', None),
            request.args.get('end', None))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))


    return result


@_core.route('/v2/api/odd/create/<customer_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ', 'WRITE'])
def create_odd(customer_id):
    if 'scope' not in request.args or 'frequency' not in request.args:
        raise InvalidUsage(code=400, payload=['screening_id', 'frequency'])

    try:
        result = _task_manager.create_odd(
            customer_id,
            request.args.get('scope'),
            request.args.get('frequency'))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return result


@_core.route('/v2/api/odd/delete/<customer_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ', 'WRITE', 'DELETE'])
def delete_odd(customer_id):
    if 'id' not in request.args:
        raise InvalidUsage(code=400, payload='id')

    try:
        result = _task_manager.delete_odd(customer_id, request.args.get('id'))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return result


@_core.route('/v2/api/odd/edit/<customer_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ', 'WRITE'])
def edit_odd(customer_id):
    if 'id' not in request.args or \
            ('scope' and 'frequency' not in request.args):
        raise InvalidUsage(code=400, payload=['id', 'scope', 'frequency'])

    try:
        result = _task_manager.edit_odd(
            customer_id,
            request.args.get('id'),
            request.args.get('scope', None),
            request.args.get('frequency', None))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return result


@_core.route('/v2/api/documents/<customer_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_documents(customer_id):
    try:
        documents = _task_manager.get_documents(customer_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(documents)


@_core.route('/v2/api/documents/download/<document_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def download_document(document_id):
    try:
        documents = _task_manager.download_document(document_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return documents


@_core.route('/v2/api/documents/reports/download/<report_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def download_report(report_id):
    try:
        report = _task_manager.download_report(report_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(report)


@_core.route('/v2/api/match/<customer_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_matches(customer_id):
    if 'screening_id' not in request.args:
        raise InvalidUsage(code=400, payload='screening_id')

    try:
        matches = _task_manager.get_matches(
            customer_id,
            request.args.get('screening_id'),
            request.args.get('match_tyoe', None),
            request.args.get('result', None))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(matches)


@_core.route('/v2/api/match/confirm/<customer_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ', 'WRITE'])
def confirm_match(customer_id):
    if 'screening_id' or 'match_id' not in request.args:
        raise InvalidUsage(code=400, payload=['screening_id', 'match_id'])

    try:
        result = _task_manager.confirm_match(
            customer_id,
            request.args.get('match_id'),
            request.args.get('screening_id'))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return result


@_core.route('/v2/api/match/dissmis/<customer_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ', 'WRITE'])
def dismiss_match(customer_id):
    if 'screening_id' or 'match_id' not in request.args:
        raise InvalidUsage(code=400, payload=['screening_id', 'match_id'])

    try:
        result = _task_manager.dismiss_match(
            customer_id,
            request.args.get('screening_id'),
            request.args.get('match_id'))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return result


@_core.route('/v2/api/identifications', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_identifications():
    try:
        identifications = _task_manager.get_identifications(
            size=request.args.get('size', 20),
            page=request.args.get('page', 0),
            sort=request.args.get('sort', 'status'),
            order=request.args.get('order', 'desc'),
            status=request.args.get('status', None),
            begin=request.args.get('begin', None),
            end=request.args.get('end', None),
            name=request.args.get('name', None))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return identifications

@_core.route('/v2/api/identifications/count', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_identifiactions_count():
    try:
        count = _task_manager.get_identifications_count(
            request.args.get('status', None))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(count)

@_core.route('/v2/api/identifications/<customer_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def find_identification(customer_id):
    try:
        identification = _task_manager.find_identification(
            customer_id, request.args.get('identification_id', ''))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return identification


@_core.route('/v2/api/identifications_and_docs/<customer_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_customer_idetifications_and_docs(customer_id):
    try:
        result = _task_manager.get_customer_idetifications_and_docs(customer_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(result)


@_core.route('/v2/api/internal/strategies/subscribed/<user_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_user_subscribed_strategies(user_id):
    try:
        begin = int(request.args['begin']) if request.args.get('begin', '').isnumeric() else None
        end = int(request.args['end']) if request.args.get('end', '').isnumeric() else None
        strategies = _internal_task_manager.get_user_subscribed_strategies(
            user_id=user_id,
            size=request.args.get('size', 10),
            page=request.args.get('page', 0),
            sort=request.args.get('sort', 'date_added'),
            order=request.args.get('order', 'desc'),
            begin=begin,
            end=end)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(strategies)


@_core.route('/v2/api/internal/strategies/unsubscribed/<user_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_user_unsubscribed_strategies(user_id):
    try:
        begin = int(request.args['begin']) if request.args.get('begin', '').isnumeric() else None
        end = int(request.args['end']) if request.args.get('end', '').isnumeric() else None
        strategies = _internal_task_manager.get_user_unsubscribed_strategies(
            user_id=user_id,
            size=request.args.get('size', 10),
            page=request.args.get('page', 0),
            sort=request.args.get('sort', 'date_added'),
            order=request.args.get('order', 'desc'),
            begin=begin,
            end=end)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(strategies)


@_core.route('/v2/api/strategies/orders/<customer_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_strategy_orders(customer_id):
    if 'strategy_id' not in request.args:
        raise InvalidUsage(code=400, payload='strategy_id')
    try:
        result = _task_manager.get_strategy_orders(
            customer_id, request.args.get('strategy_id'))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(result)


@_core.route('/v2/api/authentication/login', methods=['POST'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
def login():
    if 'username' not in request.json or 'password' not in request.json:
        raise InvalidUsage(code=400, payload=['username', 'password'])
    try:
        response = _task_manager.login(
            request.json.get('username'),
            request.json.get('password'),
            request.json.get('token', ''))
        if response is None or \
            response['status'] != LoginStates.success.value:
            return response
        
        delta = timedelta(hours=10)
        token = create_access_token(response['username'], expires_delta=delta)
        _internal_cache.set_token(response['_id'], token, int(delta.total_seconds()))
        _internal_cache.set_permissions(token, response['permissions'], int(delta.total_seconds()))
        _internal_cache.set_username(token, response['username'], int(delta.total_seconds()))
        return json.dumps({
            'status': response['status'],
            'access_token': token,
            'expires_at': delta.total_seconds(),
            'role': response['role']
        })
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    raise InvalidUsage(401)

@_core.route('/v2/api/strategies/description', methods=['POST'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
def change_bot_description():
    if 'name' not in request.json or 'description' not in request.json:
        raise InvalidUsage(code=400, payload=['name', 'description'])
    try:
        response = _internal_task_manager.change_bot_description(
            request.json.get('name'),
            request.json.get('description'),
            request.json.get('language', 'en'))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(response)

@_core.route('/v2/api/strategies/stage', methods=['POST'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
def change_bot_stage():
    if 'name' not in request.json:
        raise InvalidUsage(code=400, payload='name')
    try:
        response = _internal_task_manager.change_bot_stage(request.json.get('name'))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(response)

@_core.route('/v2/api/strategies/description/import', methods=['POST'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
def import_bot_description():
    if 'data' not in request.json:
        raise InvalidUsage(code=400, payload='data')
    try:
        response = _internal_task_manager.import_bot_description(request.json.get('data'))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(os))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return response

# @_core.route('/v2/api/bot', methods=['GET'])
# @cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
# @jwt_required
# @permission_required(['READ'])
# @_cache.cached(timeout=_default_cache_timeout)
# def get_bots():
#     try:
#         bots = _task_manager.get_bots(request.args.get(time, 'alltime'))
#     except LookupError as le:
#         raise InvalidUsage(code=422, payload=str(le))
#     except OSError as oe:
#         raise InvalidUsage(code=503, payload=str(oe))
#     except Exception as e:
#         raise InvalidUsage(code=500, payload=str(e))

#     return bots


@_core.route('/v2/api/bot/<bot_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def find_bot(bot_id):
    try:
        bot = _task_manager.find_bot(bot_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return bot


@_core.route('/v2/api/bot/subscriptions/<bot_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_bot_subscrtiptios(bot_id):
    try:
        subscriptions = _task_manager.get_bot_subscrtiptios(
            bot_id, request.args.get('status', ''))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return subscriptions


# @_core.route('/v2/api/bot/creators', methods=['GET'])
# @cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
# @jwt_required
# @permission_required(['READ'])
# @_cache.cached(timeout=_default_cache_timeout, query_string=True)
# def get_creators():
#     try:
#         creators = _task_manager.get_creators(request.args.get(time, 'alltime'))
#     except LookupError as le:
#         raise InvalidUsage(code=422, payload=str(le))
#     except OSError as oe:
#         raise InvalidUsage(code=503, payload=str(oe))
#     except Exception as e:
#         raise InvalidUsage(code=500, payload=str(e))

#     return creators


@_core.route('/v2/api/bot/assets_growth', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout)
def get_assets_growth():
    try:
        growth = _task_manager.get_assets_growth()
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return growth


@_core.route('/v2/api/bot/trade_volumes', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout)
def get_trade_volumes():
    try:
        volumes = _task_manager.get_trade_volumes()
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return volumes


@_core.route('/v2/api/bot/trade_volumes/bot_id', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def find_trade_volume(bot_id):
    if 'percentage' not in request.args:
        raise InvalidUsage(code=400, payload='percentage')
    try:
        volumes = _task_manager.find_trade_volume(
            bot_id, request.args.get('percentage'))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return volumes


@_core.route('/v2/api/verifications', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_verifications():
    try:
        verifications = _task_manager.get_verifications(
            size=request.args.get('size', 20),
            page=request.args.get('page', 0),
            sort=request.args.get('sort', 'status'),
            order=request.args.get('order', 'desc'),
            type=request.args.get('type', None),
            status=request.args.get('status', None),
            begin=request.args.get('begin', None),
            end=request.args.get('end', None),
            name=request.args.get('name', None))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return verifications


@_core.route('/v2/api/verifications/<customer_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def find_verification(customer_id):
    if 'verification_id' not in request.args:
        raise InvalidUsage(code=400, payload='verification_id')
    try:
        verification = _task_manager.find_verification(
            customer_id, request.args.get('verification_id'))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(verification)

@_core.route('/v2/api/internal/transactions/deposits/<user_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_user_deposits(user_id):
    try:
        begin = int(request.args['begin']) if request.args.get('begin', '').isnumeric() else None
        end = int(request.args['end']) if request.args.get('end', '').isnumeric() else None
        deposits = _internal_task_manager.get_deposits(
            user_id=user_id,
            size=request.args.get('size', 10),
            page=request.args.get('page', 0),
            sort=request.args.get('sort', 'date_added'),
            order=request.args.get('order', 'desc'),
            begin=begin,
            end=end)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return deposits


@_core.route('/v2/api/internal/transactions/withdraws/clear', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ', 'WRITE'])
def clear_withdrawal():
    if 'order_id' not in request.args:
            raise InvalidUsage(code=400, payload='order_id')
    try:
        response = _internal_task_manager.clear_withdrawal(request.args['order_id'])
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return response

@_core.route('/v2/api/internal/transactions/withdraws/export', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ', 'WRITE'])
def export_withdrawals():
    try:
        xml = _internal_task_manager.export_withdrawals(
            request.args.get('format', 'xml'))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return xml

@_core.route('/v2/api/internal/transactions/withdraws/xml/failed', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ', 'WRITE'])
def get_failed_withdrawal_xml():
    try:
        ids = request.args.get('ids', None)
        xml = _internal_task_manager.get_failed_withdrawal_xml(ids.split(',') if ids else ids)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return xml

@_core.route('/v2/api/internal/transactions/withdraws/failed/any', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ', 'WRITE'])
def any_failed_withdrawal():
    try:
        any_fialed = _internal_task_manager.any_failed_withdrawal()
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return any_fialed

@_core.route('/v2/api/internal/transactions/withdraws', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
def get_withdraws():
    try:
        if 'role' not in request.args:
            raise InvalidUsage(code=400, payload='role')
        begin = int(request.args['begin']) if request.args.get('begin', '').isnumeric() else None
        end = int(request.args['end']) if request.args.get('end', '').isnumeric() else None
        withdraws = _internal_task_manager.get_withdraws(
            size=request.args.get('size', 20),
            page=request.args.get('page', 0),
            sort=request.args.get('sort', 'date_added'),
            order=request.args.get('order', 'desc'),
            begin=begin,
            end=end,
            name=request.args.get('name', None), 
            role=request.args['role'],
            failed=request.args.get('failed', None))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return withdraws

@_core.route('/v2/api/internal/transactions/deposits', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
def get_deposits():
    try:
        begin = int(request.args['begin']) if request.args.get('begin', '').isnumeric() else None
        end = int(request.args['end']) if request.args.get('end', '').isnumeric() else None
        withdraws = _internal_task_manager.get_deposits(
            size=request.args.get('size', 20),
            page=request.args.get('page', 0),
            sort=request.args.get('sort', 'date_added'),
            order=request.args.get('order', 'desc'),
            begin=begin,
            end=end,
            name=request.args.get('name', None))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return withdraws

@_core.route('/v2/api/internal/transactions/withdraw/<withdraw_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_short_cache_timeout, query_string=True)
def get_withdraw(withdraw_id):
    try:
        withdraw = _internal_task_manager.get_withdraw(withdraw_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return withdraw

@_core.route('/v2/api/internal/transactions/deposit/<deposit_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_short_cache_timeout, query_string=True)
def get_deposit(deposit_id):
    try:
        withdraw = _internal_task_manager.get_deposit(deposit_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return withdraw

@_core.route('/v2/api/internal/transactions/withdraws/<user_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_user_withdraws(user_id):
    try:
        begin = int(request.args['begin']) if request.args.get('begin', '').isnumeric() else None
        end = int(request.args['end']) if request.args.get('end', '').isnumeric() else None
        withdraws = _internal_task_manager.get_withdraws(
            user_id=user_id,
            size=request.args.get('size', 10),
            page=request.args.get('page', 0),
            sort=request.args.get('sort', 'date_added'),
            order=request.args.get('order', 'desc'),
            begin=begin,
            end=end)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return withdraws

@_core.route('/v2/api/internal/finance/general', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_general_finance():
    try:
        begin = int(request.args['begin']) if request.args.get('begin', '').isnumeric() else None
        end = int(request.args['end']) if request.args.get('end', '').isnumeric() else None
        result = _internal_task_manager.get_general_finance(begin, end)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return result

@_core.route('/v2/api/internal/strategies', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)  
def get_strategies_info():
    try:
        begin = int(request.args['begin']) if request.args.get('begin', '').isnumeric() else None
        end = int(request.args['end']) if request.args.get('end', '').isnumeric() else None
        strategies = _internal_task_manager.get_strategies_info(
            size=request.args.get('size', 20),
            page=request.args.get('page', 0),
            sort=request.args.get('sort', 'date_added'),
            order=request.args.get('order', 'desc'),
            begin=begin,
            end=end,
            name=request.args.get('name', None))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return strategies

@_core.route('/v2/api/internal/articles', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)  
def get_articles():
    try:
        articles = _internal_task_manager.get_articles(
            size=request.args.get('size', 20),
            page=request.args.get('page', 0),
            sort=request.args.get('sort', 'article'),
            order=request.args.get('order', 'desc'),
            name=request.args.get('name', None))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(os))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return articles

@_core.route('/v2/api/internal/articles/<article_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)  
def get_article(article_id):
    try:
        articles = _internal_task_manager.get_articles(article_id=article_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(os))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return articles


@_core.route('/v2/api/internal/articles/delete', methods=['POST'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
def delete_article():
    if 'name' not in request.json or 'language' not in request.json:
        raise InvalidUsage(code=400, payload=['name', 'language'])
    try:
        response = _internal_task_manager.delete_article(
            request.json.get('name'),
            request.json.get('language'))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(os))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(response)

@_core.route('/v2/api/internal/articles/update', methods=['POST'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
def update_article():
    if 'name' not in request.json or 'language' not in request.json:
        raise InvalidUsage(code=400, payload=['name', 'language'])
    try:
        response = _internal_task_manager.update_article(
            request.json.get('name'),
            request.json.get('language'), 
            request.json.get('image', ''), 
            request.json.get('type_', ''), 
            request.json.get('title', ''), 
            request.json.get('intro', ''), 
            request.json.get('body', ''),
            request.json.get('bots', ''))

    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(os))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return json.dumps(response)

@_core.route('/v2/api/internal/transactions/<transaction_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)  
def get_transaction_info(transaction_id):
    try:
        info = _internal_task_manager.get_transaction_info(transaction_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return info

@_core.route('/v2/api/internal/strategies/<strategy_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)  
def get_strategy_info(strategy_id):
    try:
        info = _internal_task_manager.get_strategy_info(strategy_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return info

@_core.route('/v2/api/internal/strategies/names', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)  
def get_active_strategy_names():
    try:
        names = _internal_task_manager.get_active_strategy_names()
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(os))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return names

@_core.route('/v2/api/internal/strategies/full/<strategy_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_short_cache_timeout, query_string=True) 
def get_strategy_full_info(strategy_id):
    try:
        info = _internal_task_manager.get_strategies_info(strategy_id=strategy_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return info

@_core.route('/v2/api/internal/strategies/grouped/team', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_long_cache_timeout)
def get_team_grouped_strategies():
    try:
        data = _internal_task_manager.get_team_grouped_strategies()
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return data


@_core.route('/v2/api/internal/strategies/archived/<archive_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_default_cache_timeout, query_string=True)
def get_unsub_strategy_info(archive_id):
    try:
        info = _internal_task_manager.get_unsub_strategy_info(archive_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except ArithmeticError as ae:
        raise InvalidUsage(code=500, message='Arithmatic error', payload=str(ae))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))
    
    return info


@_core.route('/v2/api/internal/skynet/users', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['ADMIN'])
def get_skynet_users():
    try:
        users = _internal_task_manager.get_skynet_users()
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return users

@_core.route('/v2/api/internal/skynet/users/delete/<user_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['ADMIN'])
def delete_skynet_user(user_id):
    try:
        _internal_task_manager.delete_skynet_user(user_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return jsonify(success=True)

@_core.route('/v2/api/internal/skynet/users/reset/<user_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['ADMIN'])
def reset_skynet_user_2fa_secret(user_id):
    try:
        _internal_task_manager.reset_skynet_user_2fa_secret(user_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return jsonify(success=True)

@_core.route('/v2/api/internal/skynet/users/modify/<user_id>', methods=['POST'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['ADMIN'])
def modify_skynet_user(user_id):
    try:
        permissions = request.json.get('permissions', [])
        role = request.json.get('role', '')
        password = request.json.get('password', None)
        _internal_task_manager.modify_skynet_user(user_id, permissions, role)
        if password:
            _internal_task_manager.change_skynet_user_password(user_id, password)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return jsonify(success=True)


@_core.route('/v2/api/internal/skynet/users/create', methods=['POST'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['ADMIN'])
def create_skynet_user():
    if 'username' not in request.json\
        or 'password' not in request.json:
        raise InvalidUsage(code=400, payload=['username', 'password'])

    try:
        permissions = request.json.get('permissions', [])
        role = request.json.get('role', '')
        username = _internal_task_manager.create_skynet_user(
            request.json.get('username'),
            request.json.get('password'),
            permissions,
            role)
        if username:
            return jsonify(success=True)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    raise InvalidUsage(500)


@_core.route('/v2/api/internal/skynet/users/suspend/<user_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['ADMIN'])
def suspend_skynet_user(user_id):
    try:
        token = _internal_cache.get_token(user_id).decode("utf-8") 
        if token:
            _internal_cache.revoke_permissions(token)
        _internal_task_manager.suspend_skynet_user(user_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return jsonify(success=True)


@_core.route('/v2/api/internal/skynet/users/unsuspend/<user_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['ADMIN'])
def unsuspend_skynet_user(user_id):
    try:
        _internal_task_manager.unsuspend_skynet_user(user_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return jsonify(success=True)

@_core.route('/v2/api/internal/skynet/diagnostics', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['ADMIN'])
def get_diagnostics():
    try:
        begin = int(request.args['begin']) if request.args.get('begin', '').isnumeric() else None
        end = int(request.args['end']) if request.args.get('end', '').isnumeric() else None
        diagnostics = _internal_task_manager.get_diagnostics(begin, end)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return diagnostics

@_core.route('/v2/api/internal/skynet/audits', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['ADMIN'])
def get_audits():
    try:
        begin = int(request.args['begin']) if request.args.get('begin', '').isnumeric() else None
        end = int(request.args['end']) if request.args.get('end', '').isnumeric() else None
        audits = _internal_task_manager.get_audits(
            begin, 
            end,
            request.args.get('user', ''),
            request.args.get('max', 10))
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return audits

@_core.route('/v2/api/internal/transactions/withdrawable/<user_id>', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
def get_withdrawable_amount(user_id):
    try:
        amount = _internal_task_manager.get_withdrawable_amount(user_id)
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return amount

@_core.route('/v2/api/internal/transactions/deposit/first/daily', methods=['GET'])
@cross_origin(origin=_allowed_origin, headers=['Content- Type', 'Authorization'])
@jwt_required
@permission_required(['READ'])
@_cache.cached(timeout=_long_cache_timeout, query_string=True)  
def get_first_deposit_data():
    try:
        data = _internal_task_manager.get_first_deposit_data()
    except LookupError as le:
        raise InvalidUsage(code=422, payload=str(le))
    except OSError as oe:
        raise InvalidUsage(code=503, payload=str(oe))
    except Exception as e:
        raise InvalidUsage(code=500, payload=str(e))

    return data


from gevent.pywsgi import WSGIServer

if __name__ == "__main__":
    ServiceManager.kill_all() #in case any daemon is stalled
    
    ServiceManager.boot_all()
    try:
        if os.environ['APP_RUN_ENV'] == Mode.development.value:
            _core.run()
        elif os.environ['APP_RUN_ENV'] == Mode.production.value:
            https_server = WSGIServer(('0.0.0.0', 5000), _core, keyfile='key.pem', certfile='cert.pem')
            https_server.serve_forever()
        else:
            raise ValueError('NO_MODE')
    except ValueError:
        print('Indicate run envinroment (DEV/PROD)')
    except KeyboardInterrupt:
        print('Finished by keyboard intruption')
    finally:
        _cache.clear()
        ServiceManager.kill_all()
        