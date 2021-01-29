import subprocess
import time
from pymongo import MongoClient
from api_access import ApiAccess
from enums import Mode
import os

class ServiceManager():
    _services = []

    @staticmethod
    def get_broker(service_name):
        service_data = ApiAccess._skynet_db.services.find_one(
            {'app': service_name})
        return service_data['broker']

    @staticmethod
    def get_backend(service_name):
        service_data = ApiAccess._skynet_db.services.find_one(
            {'app': service_name})
        return service_data['backend']

    @staticmethod
    def get_services():
        cursor = ApiAccess._skynet_db.services.find({})
        services = []
        for document in cursor:
            services.append(document)
        return services

    @staticmethod
    def create_service_dev(options):
        service = []
        if 'type' in options and options['type'] == 'beat':
            service.append(subprocess.Popen(['celery',
                                            '-A', 'services.{}'.format(options['app']), 'beat',
                                            '--pidfile=',
                                            '-l', options['loglevel']]))

        service.append(subprocess.Popen(['celery',
                                        '-A', 'services.{}'.format(options['app']), 'worker',
                                        '-l', options['loglevel'],
                                        '-Q', options['queues'],
                                        '--concurrency={}'.format(options['concurrency']),
                                        '-n', 'wkr{}@%h'.format(options['id'])]))
        return service

    @staticmethod
    def create_service_prod(options):
        if 'type' in options and options['type'] == 'beat':
            os.system('celery multi start bt{}@%h -A services.{} --beat -l {} --pidfile=/var/run/celery/%n.pid'\
                .format(options['id'], options['app'], options['loglevel']))

        os.system ('celery multi start wkr{}@%h -A services.{} -l {} -Q {} --concurrency={}'\
            .format(options['id'], options['app'], 
            options['loglevel'], options['queues'], options['concurrency']))

    @staticmethod
    def boot_all():
        services = ServiceManager.get_services()
        mode = os.environ['APP_RUN_ENV']
        for service_data in services:
            if mode == Mode.development.value:
                ServiceManager._services.extend(
                    ServiceManager.create_service_dev(service_data))
                time.sleep(2)
            elif mode == Mode.production.value:
                ServiceManager.create_service_prod(service_data)

    @staticmethod
    def kill_all():
        mode = os.environ['APP_RUN_ENV']
        if mode == Mode.development.value:
            for service in ServiceManager._services:
                service.kill()
        elif mode == Mode.production.value:
            services = ServiceManager.get_services()
            for service_data in services:
                if 'type' in service_data and service_data['type'] == 'beat':
                    os.system('celery multi stop bt{}@%h'.format(service_data['id']))
                os.system('celery multi stop wkr{}@%h'.format(service_data['id']))

    @staticmethod
    def get_service_name(name):
        splited = name.split('.')
        return splited[1] if len(splited) > 1 else name