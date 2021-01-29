from services import (
    customers_service,
    screening_service,
    odd_service,
    file_service,
    match_service,
    identification_service,
    strategies_service,
    authentication_service,
    bot_service,
    verification_service,
    transaction_service,
    exchange_service)

from celery import group, chain
import pycountry
import pycountry_convert
from datetime import datetime
import dateutil.relativedelta
import ast

class TaskManager:
    def get_customers(self, size=20, page=0, sort='createdDate',
                      order='desc', begin=None, end=None, name=None):
        parameter_pack = lambda **args:\
            customers_service.get_customers.apply_async(
                args=(args,), queue='customers')

        sort_arg = sort + ',{}'.format(order.upper())
        return parameter_pack(size=size, page=page, sort=sort_arg,
                              created_after=begin, created_before=end, entity_name=name).get()

    def get_customers_count(self, country=None):
        parameter_pack = lambda **args:\
            customers_service.get_customers_count.apply_async(
                args=(args,), queue='customers')

        country_code = None if country is None \
            else pycountry_convert.country_name_to_country_alpha3(country)

        return parameter_pack(country=country_code).get()

    def get_customer(self, customer_id):
        return customers_service.get_customer.apply_async(
            args=(customer_id,), queue='customers').get()

    def get_customer_risk(self, customer_id):
        return customers_service.get_customer_risk.apply_async(
            args=(customer_id,), queue='customers').get()
            
    def board_customer(self, token, customer_id, state):
        parameter_pack = lambda token, **args:\
            customers_service.board_customer.apply_async(
                args=(token, args), queue='customers')

        return parameter_pack(
            token=token, swiftdil_id=customer_id, state=state).get()

    def get_first_deposits(self, begin_year=None, begin_month=None):
        parameter_pack = lambda token, **args:\
            customers_service.get_first_deposits.apply_async(
                args=(args,), queue='customers')

        if begin_year and begin_month:
            begin = datetime(year=begin_year, month=begin_month, day=1)
            end = datetime.now().replace(
                day=calendar.monthrange(begin.year, begin.month)[1],
                hour=0, minute=0, second=0, microsecond=0)
        else:
            begin = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            end = begin + dateutil.relativedelta.relativedelta(months=-1)

        return parameter_pack(
            startDate=begin.timestamp(), endDate=end.timestamp()).get()

    def get_screening_count(self, status='DONE', scope=None):
        parameter_pack = lambda **args:\
            screening_service.get_screening_count.apply_async(
                args=(args,), queue='screenings')

        return parameter_pack(status=status.upper(), scope=scope).get()

    def get_screening_all_scopes(self, status='DONE'):
        parameter_pack = lambda **args:\
            screening_service.get_screening_count.s(
                args).set(queue='screenings', countdown=.25)

        scopes = ['PEP', 'ADVERSE_MEDIA', 'DISQUALIFIED_ENTITIES', 'WATCHLIST']
        all_scopes = group(
            parameter_pack(
                status=status.upper(),
                scope=scope) for scope in scopes)().get()

        result = {}
        for i, count in enumerate(all_scopes):
            result[scopes[i]] = count
        return result

    def create_screening(self, customer_id, scope=''):
        scopes = '["PEP","WATCHLIST","ADVERSE_MEDIA","DISQUALIFIED_ENTITIES"]' \
            if scope == '' or scope.upper() == 'TOTAL' \
            else '["{}"]'.format(scope.upper())

        return screening_service.create_screening.apply_async(
            args=(customer_id, scopes), queue='screenings').get()

    def get_screenings(self, customer_id, size=20, page=0, sort='createdDate',
                      order='desc', begin=None, end=None):
        parameter_pack = lambda customer_id, **args:\
            screening_service.get_screenings.apply_async(
                args=(customer_id, args), queue='screenings')

        sort_arg = sort + ',{}'.format(order.upper())
        return parameter_pack(customer_id=customer_id, size=size, page=page, sort=sort_arg,
                              created_after=begin, created_before=end).get()

    def search_screenings(self, size=20, page=0, sort='createdDate',
                    order='desc', begin=None, end=None, name=None, scope=None):
        parameter_pack = lambda **args:\
            screening_service.search_screenings.apply_async(
                args=(args,), queue='screenings')

        sort_arg = sort + ',{}'.format(order.upper())
        return parameter_pack(size=size, page=page, sort=sort_arg,
                                created_after=begin, created_before=end, entity_name=name, scope=scope).get()

    def find_screening(self, customer_id, screening_id):
        return screening_service.find_screening.apply_async(
            args=(customer_id, screening_id), queue='screenings').get()

    def get_odds(self, customer_id, size=7,
                 scope=None, begin=None, end=None):
        parameter_pack = lambda customer_id, **args:\
            odd_service.get_odds.apply_async(
                args=(customer_id, args), queue='odd')

        return parameter_pack(
            customer_id=customer_id, size=size, scope=scope, created_after=begin,
            created_before=end).get()

    def create_odd(self, customer_id, scope, frequency):
        parameter_pack = lambda customer_id, **args:\
            odd_service.create_odd.apply_async(
                args=(customer_id, args), queue='odd')

        return parameter_pack(
            customer_id=customer_id, scope=scope, frequency=frequency).get()

    def delete_odd(self, customer_id, odd_id):
        return odd_service.delete_odd.apply_async(
            args=(customer_id, odd_id), queue='odd').get()

    def edit_odd(self, customer_id, odd_id, scope=None, frequency=None):
        parameter_pack = lambda customer_id, odd_id, **args:\
            odd_service.edit_odd.apply_async(
                args=(customer_id, odd_id, args), queue='odd')

        return parameter_pack(
            customer_id=customer_id, odd_id=odd_id, scope=ast.literal_eval(scope),
            frequency=frequency).get()

    def get_documents(self, customer_id):
        return file_service.get_documents.apply_async(
            args=(customer_id,), queue='file').get()

    def download_document(self, documet_id):
        return file_service.download_file.apply_async(
            args=(documet_id,), queue='file').get()

    def download_report(self, report_id):
        return file_service.download_report.apply_async(
            args=(report_id,), queue='file').get()

    def get_reports(self):
        return file_service.get_reports.apply_async(queue='file').get()

    def get_matches(self, customer_id, screening_id,
                    match_type=None, result=None):
        parameter_pack = lambda customer_id, screening_id, **args:\
            match_service.get_matches.apply_async(
                args=(customer_id, screening_id, args), queue='match')

        return parameter_pack(customer_id=customer_id, screening_id=screening_id,
                              match_type=match_type, validation_result=result).get()

    def confirm_match(self, customer_id, screening_id, match_id):
        return match_service.confirm_match.apply_async(
            args=(customer_id, screening_id, match_id), queue='match').get()

    def dismiss_match(self, customer_id, screening_id, match_id):
        return match_service.dismiss_match.apply_async(
            args=(customer_id, screening_id, match_id), queue='match').get()

    def get_identifications(self, size=20, page=0, sort='createdDate',
                            order='desc', status=None, begin=None, end=None, name=None):
        parameter_pack = lambda **args:\
            identification_service.get_identifications.apply_async(
                args=(args,), queue='identification')

        sort_arg = sort + ',{}'.format(order.upper())
        return parameter_pack(size=size, page=page, sort=sort_arg, status=status,
                              created_after=begin, created_before=end, entity_name=name).get()

    def get_identifications_count(self, status=None):
        parameter_pack = lambda **args:\
            identification_service.get_identifications_count.apply_async(
                args=(args,), queue='identification')

        return parameter_pack(status=status).get()

    def find_identification(self, customer_id, identification_id=''):
        return identification_service.find_identification.apply_async(
            args=(customer_id, identification_id), queue='identification').get()

    def get_customer_idetifications_and_docs(self, customer_id):
        result = ["" for x in range(4)]

        result[0] = identification_service.find_latest_identification.apply_async(
            args=(customer_id,), queue='identification').get()

        if 'selfie_id' in result[0]:
            result[1] = file_service.download_file.apply_async(
                args=(result[0]['selfie_id'],), queue='file').get()

        docs = file_service.get_documents.apply_async(
            args=(customer_id,), queue='file').get()['content']

        if 'front_side' in docs and 'back_side' in docs:
            temp = group([file_service.download_file.s(docs['front_side']).set(queue='file', countdown=.25),
                   file_service.download_file.s(docs['back_side']).set(queue='file', countdown=.25),])().get()
            result[2] = temp[0]
            result[3] = temp[1]

        return result

    def get_strategies(self, customer_id):
        return strategies_service.get_strategies.apply_async(
            args=(customer_id,), queue='strategies').get()

    def get_strategy_orders(self, customer_id, strategy_id):
        return strategies_service.get_strategy_orders.apply_async(
            args=(customer_id, strategy_id), queue='strategies').get()

    def login(self, username, password, token):
        return authentication_service.login.apply_async(
            args=(username, password, token), queue='authentication').get()

    def get_user(self, username):
        return authentication_service.get_user.apply_async(
            args=(username,), queue='authentication').get()

    def get_bots(self, time='alltime'):
        return bot_service.get_bots.apply_async(
            args=(time,), queue='bot').get()

    def find_bot(self, bot_id):
        return bot_service.find_bot.apply_async(
            args=(bot_id,), queue='bot').get()

    def get_bot_subscrtiptios(self, bot_id, status=''):
        return bot_service.get_bot_subscrtiptios.apply_async(
            args=(bot_id, status), queue='bot').get()

    def get_creators(self, time='alltime'):
        return bot_service.get_creators.apply_async(
            args=(time,), queue='bot').get()

    def get_assets_growth(self):
        return bot_service.get_assets_growth.apply_async(queue='bot').get()

    def get_trade_volumes(self):
        return bot_service.get_trade_volumes.apply_async(queue='bot').get()

    def find_trade_volume(self, bot_id, percentage):
        return bot_service.find_trade_volume.apply_async(
            args=(bot_id, percentage), queue='bot').get()

    def get_verifications(self, size=20, page=0, sort='createdDate',
                          order='desc', type=None, status=None, begin=None, end=None, name=None):
        parameter_pack = lambda **args:\
            verification_service.get_verifications.apply_async(
                args=(args,), queue='verification')

        sort_arg = sort + ',{}'.format(order.upper())
        return parameter_pack(size=size, page=page, sort=sort_arg,
                              created_after=begin, created_before=end, entityName=name).get()

    def find_verification(self, customer_id, verification_id):
        return verification_service.find_verification.apply_async(
            args=(customer_id, verification_id), queue='verification').get()
