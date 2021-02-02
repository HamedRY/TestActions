#TODO: add overwrite mode (use database?)

import fileinput
import inspect
from functools import wraps
import json
import os

class TestGenerator:
    with open('tests/template.txt', 'r') as file:
        _template = file.read()
        _generate = bool(os.environ.get('TEST_GEN', 'false').lower() in ('true', '1'))

    def __init__(self, service):
        self.service = service[len('services')+1:].split('_')[0]
        self.test_file = 'tests/test_{}.py'.format(self.service)
        self.existing_tests = self.get_existing_tests()

    def get_existing_tests(self):
        result = {}
        with open(self.test_file, 'r') as test_file:
            for line in test_file:
                if 'def' in line and '(self)' in line and '__init__' not in line:
                    splited = line.split('_')
                    result['_'.join(map(str, splited[1:-2]))] = int(splited[-1].split('(')[0])

        return result

    def add_test(self, test_):
        for line in fileinput.input(self.test_file, inplace=1):
            if "if __name__ == '__main__':" in line:
                line=line.replace(line, '{}\n{}'.format(test_, line))
            
            print(line, end='')

    def create_test(self, tname_, tparams_, treturn_):
        num = self.existing_tests.get(tname_, -1) + 1
        test = TestGenerator._template
        for r in (
            ('SERVICE_NAME', self.service), ('TASK_NAME', tname_), 
            ('TASK_PARAMETERS', tparams_), ('TASK_RETURN', treturn_.\
                replace('true', 'True').replace('false', 'False').\
                replace('task.result, null', 'task.result, None')), 
            ('TEST_NUMBER', str(num))):
            test = test.replace(*r)
        self.existing_tests[tname_] = num

        self.add_test(test)

    def create(self):
        def wrap(f, *args, **kwargs):
            @wraps(f)
            def wrapped_f(*args, **kwargs):
                if not TestGenerator._generate:
                    return f(*args, **kwargs)

                task_name = f.__name__
                task_params = ''
                for value in args:
                    task_params += '{}, '.format('\'{}\''.format(value) if isinstance(value, str) else value)
                for key, value in kwargs.items():
                    task_params += '{}={}, '.format(key, '\'{}\''.format(value) if isinstance(value, str) else value)
                task_return = f(*args, **kwargs)
                self.create_test(task_name, task_params[:-2], json.dumps(task_return))

                return task_return
            return wrapped_f
        return wrap