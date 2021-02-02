import unittest
import sys, os
import json
sys.path.append(os.getcwd())
os.environ['APP_RUN_ENV'] = 'development'

from services import authentication_service

class AuthenticationTests(unittest.TestCase):
    _ = None
    def test_login_mutation_0(self):
        task = authentication_service.login.s(
            'admin', 'admin', ''
        ).apply()
        self.assertEqual(task.result, {"_id": "5ee7918979bb276948910fd3", "username": "ADMIN", "uid": 1.0, "permissions": ["READ", "WRITE", "ADMIN", "DELETE"], "role": "ADMIN", "2fa_active": True, "secret": "CWDFOMQOHCY4QMDE", "status": "success"})

    def test_get_users_mutation_0(self):
        task = authentication_service.get_users.s(
            
        ).apply()
        self.assertEqual(task.result, {"content": [{"_id": "5ee7918979bb276948910fd3", "username": "ADMIN", "permissions": ["READ", "WRITE", "ADMIN", "DELETE"], "role": "ADMIN"}, {"_id": "5f16a7bbb9ec781a4d85226a", "username": "MARKETING", "permissions": ["READ", "DELETE", "WRITE"], "role": "MARKETING"}, {"_id": "5f4785ab1c1ac908507639a6", "username": "SUPPORT", "permissions": ["READ"], "role": "SUPPORT"}, {"_id": "5f6c57b2abc917c039dcf0d6", "username": "OTHER", "role": "OTHER", "permissions": ["READ", "WRITE"]}, {"_id": "6006c24557bc03c3ebc96574", "username": "MAINTAINER", "role": "MAINTAINER", "permissions": ["READ", "WRITE", "DELETE", "ADMIN"]}]})

    def test_get_users_mutation_1(self):
        task = authentication_service.get_users.s(
            
        ).apply()
        self.assertEqual(task.result, {"content": [{"_id": "5ee7918979bb276948910fd3", "username": "ADMIN", "permissions": ["READ", "WRITE", "ADMIN", "DELETE"], "role": "ADMIN"}, {"_id": "5f16a7bbb9ec781a4d85226a", "username": "MARKETING", "permissions": ["READ", "DELETE", "WRITE"], "role": "MARKETING"}, {"_id": "5f4785ab1c1ac908507639a6", "username": "SUPPORT", "permissions": ["READ"], "role": "SUPPORT"}, {"_id": "5f6c57b2abc917c039dcf0d6", "username": "OTHER", "role": "OTHER", "permissions": ["READ", "WRITE"]}, {"_id": "6006c24557bc03c3ebc96574", "username": "MAINTAINER", "role": "MAINTAINER", "permissions": ["READ", "WRITE", "DELETE", "ADMIN"]}]})

    def test_create_user_mutation_0(self):
        task = authentication_service.create_user.s(
            'mamad', 'new@Notebook2017', 'SUPPORT', ['READ', 'WRITE', 'DELETE']
        ).apply()
        self.assertEqual(task.result, "MAMAD")

    def test_create_user_mutation_0(self):
        task = authentication_service.create_user.s(
            'mamad', 'new@Notebook2017', 'SUPPORT', ['READ', 'WRITE', 'DELETE']
        ).apply()
        self.assertEqual(task.result, "MAMAD")

    def test_get_users_mutation_2(self):
        task = authentication_service.get_users.s(
            
        ).apply()
        self.assertEqual(task.result, {"content": [{"_id": "5ee7918979bb276948910fd3", "username": "ADMIN", "permissions": ["READ", "WRITE", "ADMIN", "DELETE"], "role": "ADMIN"}, {"_id": "5f16a7bbb9ec781a4d85226a", "username": "MARKETING", "permissions": ["READ", "DELETE", "WRITE"], "role": "MARKETING"}, {"_id": "5f4785ab1c1ac908507639a6", "username": "SUPPORT", "permissions": ["READ"], "role": "SUPPORT"}, {"_id": "5f6c57b2abc917c039dcf0d6", "username": "OTHER", "role": "OTHER", "permissions": ["READ", "WRITE"]}, {"_id": "6006c24557bc03c3ebc96574", "username": "MAINTAINER", "role": "MAINTAINER", "permissions": ["READ", "WRITE", "DELETE", "ADMIN"]}, {"_id": "60191b47f7fa5d0767a4568e", "username": "MAMAD", "role": "SUPPORT", "permissions": ["READ", "WRITE", "DELETE"]}, {"_id": "60191b47f6dcc40402a4568e", "username": "MAMAD", "role": "SUPPORT", "permissions": ["READ", "WRITE", "DELETE"]}]})

    def test_get_users_mutation_3(self):
        task = authentication_service.get_users.s(
            
        ).apply()
        self.assertEqual(task.result, {"content": [{"_id": "5ee7918979bb276948910fd3", "username": "ADMIN", "permissions": ["READ", "WRITE", "ADMIN", "DELETE"], "role": "ADMIN"}, {"_id": "5f16a7bbb9ec781a4d85226a", "username": "MARKETING", "permissions": ["READ", "DELETE", "WRITE"], "role": "MARKETING"}, {"_id": "5f4785ab1c1ac908507639a6", "username": "SUPPORT", "permissions": ["READ"], "role": "SUPPORT"}, {"_id": "5f6c57b2abc917c039dcf0d6", "username": "OTHER", "role": "OTHER", "permissions": ["READ", "WRITE"]}, {"_id": "6006c24557bc03c3ebc96574", "username": "MAINTAINER", "role": "MAINTAINER", "permissions": ["READ", "WRITE", "DELETE", "ADMIN"]}, {"_id": "60191b47f7fa5d0767a4568e", "username": "MAMAD", "role": "SUPPORT", "permissions": ["READ", "WRITE", "DELETE"]}, {"_id": "60191b47f6dcc40402a4568e", "username": "MAMAD", "role": "SUPPORT", "permissions": ["READ", "WRITE", "DELETE"]}]})

    def test_get_users_mutation_4(self):
        task = authentication_service.get_users.s(
            
        ).apply()
        self.assertEqual(task.result, {"content": [{"_id": "5ee7918979bb276948910fd3", "username": "ADMIN", "permissions": ["READ", "WRITE", "ADMIN", "DELETE"], "role": "ADMIN"}, {"_id": "5f16a7bbb9ec781a4d85226a", "username": "MARKETING", "permissions": ["READ", "DELETE", "WRITE"], "role": "MARKETING"}, {"_id": "5f4785ab1c1ac908507639a6", "username": "SUPPORT", "permissions": ["READ"], "role": "SUPPORT"}, {"_id": "5f6c57b2abc917c039dcf0d6", "username": "OTHER", "role": "OTHER", "permissions": ["READ", "WRITE"]}, {"_id": "6006c24557bc03c3ebc96574", "username": "MAINTAINER", "role": "MAINTAINER", "permissions": ["READ", "WRITE", "DELETE", "ADMIN"]}, {"_id": "60191b47f7fa5d0767a4568e", "username": "MAMAD", "role": "SUPPORT", "permissions": ["READ", "WRITE", "DELETE"]}, {"_id": "60191b47f6dcc40402a4568e", "username": "MAMAD", "role": "SUPPORT", "permissions": ["READ", "WRITE", "DELETE"]}]})

    def test_get_users_mutation_5(self):
        task = authentication_service.get_users.s(
            
        ).apply()
        self.assertEqual(task.result, {"content": [{"_id": "5ee7918979bb276948910fd3", "username": "ADMIN", "permissions": ["READ", "WRITE", "ADMIN", "DELETE"], "role": "ADMIN"}, {"_id": "5f16a7bbb9ec781a4d85226a", "username": "MARKETING", "permissions": ["READ", "DELETE", "WRITE"], "role": "MARKETING"}, {"_id": "5f4785ab1c1ac908507639a6", "username": "SUPPORT", "permissions": ["READ"], "role": "SUPPORT"}, {"_id": "5f6c57b2abc917c039dcf0d6", "username": "OTHER", "role": "OTHER", "permissions": ["READ", "WRITE"]}, {"_id": "6006c24557bc03c3ebc96574", "username": "MAINTAINER", "role": "MAINTAINER", "permissions": ["READ", "WRITE", "DELETE", "ADMIN"]}, {"_id": "60191b47f7fa5d0767a4568e", "username": "MAMAD", "role": "SUPPORT", "permissions": ["READ", "WRITE", "DELETE"]}, {"_id": "60191b47f6dcc40402a4568e", "username": "MAMAD", "role": "SUPPORT", "permissions": ["READ", "WRITE", "DELETE"]}]})

    def test_delete_user_mutation_0(self):
        task = authentication_service.delete_user.s(
            '60191b47f6dcc40402a4568e'
        ).apply()
        self.assertEqual(task.result, None)

    def test_delete_user_mutation_1(self):
        task = authentication_service.delete_user.s(
            '60191b47f6dcc40402a4568e'
        ).apply()
        self.assertEqual(task.result, None)

    def test_get_users_mutation_6(self):
        task = authentication_service.get_users.s(
            
        ).apply()
        self.assertEqual(task.result, {"content": [{"_id": "5ee7918979bb276948910fd3", "username": "ADMIN", "permissions": ["READ", "WRITE", "ADMIN", "DELETE"], "role": "ADMIN"}, {"_id": "5f16a7bbb9ec781a4d85226a", "username": "MARKETING", "permissions": ["READ", "DELETE", "WRITE"], "role": "MARKETING"}, {"_id": "5f4785ab1c1ac908507639a6", "username": "SUPPORT", "permissions": ["READ"], "role": "SUPPORT"}, {"_id": "5f6c57b2abc917c039dcf0d6", "username": "OTHER", "role": "OTHER", "permissions": ["READ", "WRITE"]}, {"_id": "6006c24557bc03c3ebc96574", "username": "MAINTAINER", "role": "MAINTAINER", "permissions": ["READ", "WRITE", "DELETE", "ADMIN"]}, {"_id": "60191b47f7fa5d0767a4568e", "username": "MAMAD", "role": "SUPPORT", "permissions": ["READ", "WRITE", "DELETE"]}]})

    def test_get_users_mutation_7(self):
        task = authentication_service.get_users.s(
            
        ).apply()
        self.assertEqual(task.result, {"content": [{"_id": "5ee7918979bb276948910fd3", "username": "ADMIN", "permissions": ["READ", "WRITE", "ADMIN", "DELETE"], "role": "ADMIN"}, {"_id": "5f16a7bbb9ec781a4d85226a", "username": "MARKETING", "permissions": ["READ", "DELETE", "WRITE"], "role": "MARKETING"}, {"_id": "5f4785ab1c1ac908507639a6", "username": "SUPPORT", "permissions": ["READ"], "role": "SUPPORT"}, {"_id": "5f6c57b2abc917c039dcf0d6", "username": "OTHER", "role": "OTHER", "permissions": ["READ", "WRITE"]}, {"_id": "6006c24557bc03c3ebc96574", "username": "MAINTAINER", "role": "MAINTAINER", "permissions": ["READ", "WRITE", "DELETE", "ADMIN"]}, {"_id": "60191b47f7fa5d0767a4568e", "username": "MAMAD", "role": "SUPPORT", "permissions": ["READ", "WRITE", "DELETE"]}]})

    def test_login_mutation_1(self):
        task = authentication_service.login.s(
            'admin', 'admin', ''
        ).apply()
        self.assertEqual(task.result, {"_id": "5ee7918979bb276948910fd3", "username": "ADMIN", "uid": 1.0, "permissions": ["READ", "WRITE", "ADMIN", "DELETE"], "role": "ADMIN", "2fa_active": True, "secret": "CWDFOMQOHCY4QMDE", "status": "success"})

if __name__ == '__main__':
    unittest.main(verbosity=2)