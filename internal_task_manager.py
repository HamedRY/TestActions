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
    exchange_service,
    diagnostics_service,
    feedback_service,
    gold_service,
    bot_service)

from celery import group, chain
import pycountry
import pycountry_convert
from datetime import datetime
import dateutil.relativedelta
import ast
import json
from utils import filter_outliers
from enums import GoldType

class InternalTaskManager:
    def record_action(self, caller, username):
        diagnostics_service.record_action.apply_async(args=(caller, username), queue='diagnostics')

    def get_customers_csv_exportable(self, max=10, cols=[], begin=None, end=None):
        return customers_service.get_csv_exportable.apply_async(
            args=(int(max), cols, begin, end), queue='customers').get()

    def get_strategies_csv_exportable(self, cols=[]):
        return strategies_service.get_csv_exportable.apply_async(
            args=(cols,), queue='strategies').get()

    def get_customers(self, size=20, page=0, sort='date_added', 
                        order='desc', begin=None, end=None, name=None, advanced=''):
        if name is not None and name.startswith('#'): #search by phone number
            return customers_service.get_customers_internal_from_phone.apply_async(
                args=(int(size), begin, end, name[1:]), queue='customers').get()

        return customers_service.get_customers_internal.apply_async(
            args=(int(size), int(page), sort, order, begin, end, name, advanced), queue='customers').get()

    def get_articles(self, size=20, page=0, sort='date_added', 
                        order='desc', name=None, article_id=None):
        return bot_service.get_articles.apply_async(
            args=(int(size), int(page), sort, order, name, article_id), queue='bot').get()

    def delete_article(self, name, language):
        return bot_service.delete_article.apply_async(args=(name, language), queue='bot').get()

    def update_article(self, name, language, image, type_, title, intro, body, bots):
        return bot_service.update_article.apply_async(args=(name, language, image, type_, title, intro, body, bots), queue='bot').get()

    def get_active_strategy_names(self):
        return strategies_service.get_active_strategy_names.apply_async(queue='strategies').get()

    def get_corporates(self, size=20, page=0, sort='date_added', 
                        order='desc', begin=None, end=None, name=None):
        return customers_service.get_corporates_internal.apply_async(
            args=(int(size), int(page), sort, order, begin, end, name), queue='customers').get()

    def get_feedbacks(self, size=20, page=0, sort='version', order='desc', name=None):
        return feedback_service.get_feedbacks.apply_async(
            args=(int(size), int(page), sort, order, name), queue='feedbacks').get()

    def get_swift_id(self, customer_id):
        return customers_service.get_swift_id.apply_async(
            args=(customer_id,), queue='customers').get()

    def get_deposits(self, user_id=None, size=10, page=0, sort='date_added', 
                        order='desc', begin=None, end=None, name=None):
        return transaction_service.get_deposits.apply_async(
            args=(user_id, int(size), int(page), sort, order, begin, end, name), queue='transaction').get()

    def get_withdraws(self, user_id=None, size=10, page=0, sort='date_added', 
                        order='desc', begin=None, end=None, name=None, role=None, failed=None):
        return transaction_service.get_withdraws.apply_async(
            args=(user_id, int(size), int(page), sort, order, begin, end, name, role, failed), queue='transaction').get()

    def get_withdraw(self, withdraw_id):
        return transaction_service.get_withdraw.apply_async(
            args=(withdraw_id,), queue='transaction').get()

    def get_deposit(self, deposit_id):
        return transaction_service.get_deposit.apply_async(
            args=(deposit_id,), queue='transaction').get()

    def get_user_subscribed_strategies(self, user_id, size=10, page=0, sort='date_added', 
                        order='desc', begin=None, end=None):
        return strategies_service.get_user_subscribed_strategies.apply_async(
            args=(user_id, int(size), int(page), sort, order, begin, end), queue='strategies').get()

    def get_user_unsubscribed_strategies(self, user_id, size=10, page=0, sort='date_added', 
                        order='desc', begin=None, end=None):
        return strategies_service.get_user_unsubscribed_strategies.apply_async(
            args=(user_id, int(size), int(page), sort, order, begin, end), queue='strategies').get()

    def get_customer(self, user_id, is_corporate=False):
        general_info_task = customers_service.get_customer_internal.apply_async(
            args=(user_id, is_corporate), queue='customers')
        transaction_info_tasks = group([
            transaction_service.get_user_deposit_amount.s(user_id=user_id).set(queue='transaction'),
            transaction_service.get_user_withdraw_amount.s(user_id=user_id).set(queue='transaction'),
            transaction_service.get_last_transaction_id.s(user_id=user_id).set(queue='transaction'),
            transaction_service.get_withdrawal_bank_account.s(user_id=user_id).set(queue='transaction')
        ]).apply_async()
        user_data = general_info_task.get()
        for tr in transaction_info_tasks.get(): user_data.update(tr)
        return json.dumps(user_data)

    def get_user_stats(self, begin=None, end=None):
        customer_tasks = group([
            customers_service.get_total_customer_count.s(begin=begin, end=end).set(queue='customers'),
            customers_service.get_kyc_customer_count.s(begin=begin, end=end).set(queue='customers'),
            customers_service.get_full_kyc_customer_count.s(begin=begin, end=end).set(queue='customers'),
            customers_service.get_subscribed_customer_count.s(begin=begin, end=end).set(queue='customers')]).apply_async()
        transaction_tasks = group([
            transaction_service.get_funded_accounts_count.s(begin=begin, end=end).set(queue='transaction'),
            transaction_service.get_lost_accounts_count.s(begin=begin, end=end).set(queue='transaction')]).apply_async()

        
        content = []
        counts = []
        for tr in customer_tasks.get(): content.extend(tr)
        for tr in transaction_tasks.get(): content.extend(tr)
    
        counts = [item['count'] for item in content if 'count' in item]
        axis_breaks = filter_outliers(counts)
        max_axis_break = axis_breaks['max_axis_break'] if axis_breaks is not None else None
        min_axis_break = axis_breaks['min_axis_break'] if axis_breaks is not None else None           
        max_stat = max(content, key=lambda x:x.get('count', 0))
        return {
            'content': content,
            'max': max_stat.get('count', 0) if max_stat is not None else 0,
            'max_axis_break' : max_axis_break,
            'min_axis_break' : min_axis_break
        }

    def get_trade_data(self, begin=None, end=None):
        return exchange_service.get_trade_data.apply_async(args=(begin, end),queue='exchange').get()

    def get_general_finance(self, begin=None, end=None): #TODO: use group
        financial_data = group([
            transaction_service.get_deposits_amount.s(begin=begin, end=end).set(queue='transaction'),
            transaction_service.get_withdrawals_amount.s(begin=begin, end=end).set(queue='transaction')]).apply_async().get()

        return {k: v for data in financial_data for k, v in data.items()}

    def get_strategies_info(self, size=10, page=0, sort='date_added', 
                order='desc', begin=None, end=None, name=None, strategy_id=None):
        info = group([
            strategies_service.get_funded_strategies.s().set(queue='strategies'),
            strategies_service.get_strategies.s(int(size), int(page), sort, order, begin, end, name, strategy_id).set(queue='strategies')]).apply_async().get()

        for index, strategy in enumerate(info[1]['content']):
            stat = info[0].get(strategy['_id'], None)
            if stat:
                info[1]['content'][index] = {**strategy, **stat}

        return info[1]

    def get_transaction_info(self, transaction_id):
        return transaction_service.get_transaction_info.apply_async(args=(transaction_id,), queue='transaction').get()
        
    def get_strategy_info(self, strategy_id):
        return strategies_service.get_strategy_info.apply_async(args=(strategy_id,), queue='strategies').get()

    def get_unsub_strategy_info(self, archive_id):
        return (strategies_service.maybe_strategy_id.s(archive_id).set(queue='strategies') | \
            strategies_service.get_strategy_info.s().set(queue='strategies')).apply_async().get()

    def get_skynet_users(self):
        users = authentication_service.get_users.apply_async(queue='authentication').get()
        return users

    def delete_skynet_user(self, user_id):
        authentication_service.delete_user.apply_async(args=(user_id,), queue='authentication').get()

    def reset_skynet_user_2fa_secret(self, user_id):
        authentication_service.reset_2fa_secret.apply_async(args=(user_id,), queue='authentication').get()

    def change_skynet_user_password(self, user_id, password):
        authentication_service.change_password.apply_async(args=(user_id, password), queue='authentication').get()
    
    def modify_skynet_user(self, user_id, permissions, role):
        authentication_service.modify_user.apply_async(args=(user_id, permissions, role), queue='authentication').get()
    
    def create_skynet_user(self, username, password, permissions, role):
        return authentication_service.create_user.apply_async(
            args=(username, password, role, permissions), queue='authentication').get()

    def suspend_skynet_user(self, user_id):
        authentication_service.suspend_user.apply_async(args=(user_id,), queue='authentication').get()
    
    def unsuspend_skynet_user(self, user_id):
        authentication_service.unsuspend_user.apply_async(args=(user_id,), queue='authentication').get()

    def export_withdrawals(self, format):
        task = transaction_service.export_withdrawals_xml.s().set(queue='transaction') if format == 'xml' \
            else transaction_service.export_withdrawals_csv.s().set(queue='transaction')
        return task.apply_async().get()

    def get_failed_withdrawal_xml(self, ids):
        return transaction_service.get_failed_withdrawal_xml.apply_async(args=(ids,), queue='transaction').get()

    def clear_withdrawal(self, order_id):
        return transaction_service.clear_withdrawal.apply_async(args=(order_id,), queue='transaction').get()

    def any_failed_withdrawal(self):
        return transaction_service.any_failed_withdrawal.apply_async(queue='transaction').get()

    def change_bot_description(self, name, description, language):
        return strategies_service.change_bot_description.apply_async(args=(name, description, language), queue='strategies').get()

    def change_bot_stage(self, name):
        return strategies_service.change_bot_stage.apply_async(args=(name,), queue='strategies').get()

    def import_bot_description(self, data):
        return strategies_service.import_bot_description.apply_async(args=(data, ), queue='strategies').get()

    def get_user_timeline(self, user_id):
        user_task = customers_service.get_customer_snapshot.apply_async(
            args=(user_id,), queue='customers')

        transaction_tasks = group([
            transaction_service.get_customer_deposit_snapshot.s(user_id=user_id).set(queue='transaction'),
            transaction_service.get_customer_withdrawal_snapshot.s(user_id=user_id).set(queue='transaction')]).apply_async()

        subscription_tasks = group([
            strategies_service.get_customer_subscribed_snapshot.s(user_id=user_id).set(queue='strategies'),
            strategies_service.get_customer_unsubscribed_snapshot.s(user_id=user_id).set(queue='strategies')]).apply_async()

        result = []
        result.append(user_task.get())
        for tr in transaction_tasks.get(): result.extend(json.loads(tr))
        for sb in subscription_tasks.get(): result.extend(json.loads(sb))
        result.sort(key=lambda x: x.get('date', '') + x.get('time', ''), reverse=True)
            
        return json.dumps(result)

    def get_diagnostics(self, begin, end):
        return diagnostics_service.get_diagnostics.apply_async(args=(begin, end), queue='diagnostics').get()

    def get_audits(self, begin, end, user='', max=10):
        return diagnostics_service.get_audits.apply_async(args=(begin, end, user, max), queue='diagnostics').get()

    def get_geographic_customer_count(self):
        return customers_service.get_geographic_customer_count.apply_async(queue='customers').get()

    def get_periodical_customer_count(self):
        return customers_service.get_periodical_customer_count.apply_async(queue='customers').get()

    def get_funded_accounts(self, size=20, page=0, sort='date_added', order='desc', begin=None, end=None):
        return (transaction_service.get_funded_accounts.s(begin, end).set(queue='transaction') | \
            customers_service.get_customers_by_id.s(int(size), int(page), sort, order).set(queue='customers')).apply_async().get()

    def save_note(self, user_id, creator, title, note):
        return customers_service.save_note.apply_async(args=(user_id, creator, title, note), queue='customers').get()

    def get_notes(self, user_id):
        return customers_service.get_notes.apply_async(args=(user_id,), queue='customers').get()

    def get_withdrawable_amount(self, user_id):
        amount_task_dump = transaction_service.get_withdrawable_amount_dump.apply_async(args=(user_id,), queue='transaction')
        amount_task = (strategies_service.get_unsub_value.s(user_id).set(queue='strategies') | \
            transaction_service.get_withdrawable_amount.s(user_id).set(queue='transaction')).apply_async()
        return {**amount_task_dump.get(), **amount_task.get()}

    def get_periodical_transaction_amount(self):
        periodic_data = group([
            transaction_service.get_periodical_transaction_amount.s(type_='deposit').set(queue='transaction'),
            transaction_service.get_periodical_transaction_amount.s(type_='withdrwal').set(queue='transaction')]).apply_async().get()
        return transaction_service.join_periodic.apply_async(args=(periodic_data[0], periodic_data[1]), queue='transaction').get()

    def get_deposit_categories(self, stage, begin=None, end=None):
        return transaction_service.get_deposit_categories.apply_async(args=(stage, begin, end), queue='transaction').get()

    def get_gold_members(self, size=20, page=0, sort='date_added', order='desc', begin=None, end=None, name=None, type_='current'):
        if type_ == GoldType.current.value:
            gold_task = gold_service.get_gold_ids.s(begin, end, order).set(queue='gold')
        elif type_ == GoldType.new.value:
            gold_task = gold_service.get_new_gold_ids.s().set(queue='gold')
        elif type_ == GoldType.lost.value:
            gold_task = gold_service.get_lost_gold_ids.s().set(queue='gold')
        else:
            raise KeyError('Invalid type')

        return (gold_task | \
            customers_service.get_customers_by_id.s(int(size), int(page), sort, order, name).set(queue='customers')).apply_async().get()

    def get_top_customers(self):
        return (gold_service.get_top_ids.s().set(queue='gold') | \
            transaction_service.get_top_deposit_info.s().set(queue='transaction')).apply_async().get()

    def get_first_deposit_data(self):
        return transaction_service.get_first_deposit_data.apply_async(queue='transaction').get()

    def get_team_grouped_strategies(self):
        return (strategies_service.get_funded_strategies.s().set(queue='strategies') | \
            strategies_service.group_by_team.s().set(queue='strategies')).apply_async().get()