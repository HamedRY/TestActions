import unittest
import sys, os
import json
sys.path.append(os.getcwd())
os.environ['APP_RUN_ENV'] = 'development'

from services import authentication_service

class AuthenticationTests(unittest.TestCase):
    # def __init__(self):
    #     pass
    def test_login_mutation_0(self):
        task = authentication_service.login.s(
            'admin', 'admin', '128950'
        ).apply()
        self.assertEqual(task.result, {"status": "fail"})

    def test_login_mutation_1(self):
        task = authentication_service.login.s(
            'admin', 'admin', '921346'
        ).apply()
        self.assertEqual(task.result, {"_id": "5f3d1835c222f53d2d579c65", "username": "ADMIN", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"], "2fa_active": True, "secret": "WNK4ID7JYAYS6H3A", "status": "success"})

    def test_get_users_mutation_0(self):
        task = authentication_service.get_users.s(
            
        ).apply()
        self.assertEqual(task.result, {"content": [{"_id": "5f3d1835c222f53d2d579c65", "username": "ADMIN", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d495e900f8d106922845e", "username": "MAMAD", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d4a807427f2825a08630c", "username": "AKBAR", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7c963054679d5f7f37ea", "username": "JAVAD", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7e0a861f9ade1e1484f9", "username": "SAJAD", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7ec042bdd941b2cadcae", "username": "SOGHRA", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7f0e3fec790b6e79ed81", "username": "TEST0", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7f6ee8db26f83c9a00a9", "username": "TEST1", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d807c6ba6c41d0a3a9b62", "username": "TEST2", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d80829fe3bd409bdfbb16", "username": "TEST3", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d83b0a3ceedc1288f778f", "username": "TEST4", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d85bb699d612eefcf96ce", "username": "ADMIN1", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d86bb37ebbd27cf144f21", "username": "HAMED", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d8bfd5cb0b454f48d866d", "username": "TEST1000", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}]})

    def test_get_users_mutation_1(self):
        task = authentication_service.get_users.s(
            
        ).apply()
        self.assertEqual(task.result, {"content": [{"_id": "5f3d1835c222f53d2d579c65", "username": "ADMIN", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d495e900f8d106922845e", "username": "MAMAD", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d4a807427f2825a08630c", "username": "AKBAR", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7c963054679d5f7f37ea", "username": "JAVAD", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7e0a861f9ade1e1484f9", "username": "SAJAD", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7ec042bdd941b2cadcae", "username": "SOGHRA", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7f0e3fec790b6e79ed81", "username": "TEST0", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7f6ee8db26f83c9a00a9", "username": "TEST1", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d807c6ba6c41d0a3a9b62", "username": "TEST2", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d80829fe3bd409bdfbb16", "username": "TEST3", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d83b0a3ceedc1288f778f", "username": "TEST4", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d85bb699d612eefcf96ce", "username": "ADMIN1", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d86bb37ebbd27cf144f21", "username": "HAMED", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d8bfd5cb0b454f48d866d", "username": "TEST1000", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}]})

    def test_modify_user_mutation_0(self):
        task = authentication_service.modify_user.s(
            '5f3d7f0e3fec790b6e79ed81', ['ADMIN', 'READ', 'WRITE', 'DELETE'], 'ADMIN'
        ).apply()
        self.assertEqual(task.result, None)

    def test_modify_user_mutation_1(self):
        task = authentication_service.modify_user.s(
            '5f3d7f0e3fec790b6e79ed81', ['ADMIN', 'READ', 'WRITE', 'DELETE'], 'ADMIN'
        ).apply()
        self.assertEqual(task.result, None)

    def test_get_users_mutation_2(self):
        task = authentication_service.get_users.s(
            
        ).apply()
        self.assertEqual(task.result, {"content": [{"_id": "5f3d1835c222f53d2d579c65", "username": "ADMIN", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d495e900f8d106922845e", "username": "MAMAD", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d4a807427f2825a08630c", "username": "AKBAR", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7c963054679d5f7f37ea", "username": "JAVAD", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7e0a861f9ade1e1484f9", "username": "SAJAD", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7ec042bdd941b2cadcae", "username": "SOGHRA", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7f0e3fec790b6e79ed81", "username": "TEST0", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7f6ee8db26f83c9a00a9", "username": "TEST1", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d807c6ba6c41d0a3a9b62", "username": "TEST2", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d80829fe3bd409bdfbb16", "username": "TEST3", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d83b0a3ceedc1288f778f", "username": "TEST4", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d85bb699d612eefcf96ce", "username": "ADMIN1", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d86bb37ebbd27cf144f21", "username": "HAMED", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d8bfd5cb0b454f48d866d", "username": "TEST1000", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}]})

    def test_get_users_mutation_3(self):
        task = authentication_service.get_users.s(
            
        ).apply()
        self.assertEqual(task.result, {"content": [{"_id": "5f3d1835c222f53d2d579c65", "username": "ADMIN", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d495e900f8d106922845e", "username": "MAMAD", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d4a807427f2825a08630c", "username": "AKBAR", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7c963054679d5f7f37ea", "username": "JAVAD", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7e0a861f9ade1e1484f9", "username": "SAJAD", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7ec042bdd941b2cadcae", "username": "SOGHRA", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7f0e3fec790b6e79ed81", "username": "TEST0", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7f6ee8db26f83c9a00a9", "username": "TEST1", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d807c6ba6c41d0a3a9b62", "username": "TEST2", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d80829fe3bd409bdfbb16", "username": "TEST3", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d83b0a3ceedc1288f778f", "username": "TEST4", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d85bb699d612eefcf96ce", "username": "ADMIN1", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d86bb37ebbd27cf144f21", "username": "HAMED", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d8bfd5cb0b454f48d866d", "username": "TEST1000", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}]})

    def test_create_user_mutation_0(self):
        task = authentication_service.create_user.s(
            'ADDDED', 'new@Notebook2017', 'SUPPORT', ['READ']
        ).apply()
        self.assertEqual(task.result, "ADDDED")

    def test_create_user_mutation_0(self):
        task = authentication_service.create_user.s(
            'ADDDED', 'new@Notebook2017', 'SUPPORT', ['READ']
        ).apply()
        self.assertEqual(task.result, None)

    def test_get_users_mutation_4(self):
        task = authentication_service.get_users.s(
            
        ).apply()
        self.assertEqual(task.result, {"content": [{"_id": "5f3d1835c222f53d2d579c65", "username": "ADMIN", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d495e900f8d106922845e", "username": "MAMAD", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d4a807427f2825a08630c", "username": "AKBAR", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7c963054679d5f7f37ea", "username": "JAVAD", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7e0a861f9ade1e1484f9", "username": "SAJAD", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7ec042bdd941b2cadcae", "username": "SOGHRA", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7f0e3fec790b6e79ed81", "username": "TEST0", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7f6ee8db26f83c9a00a9", "username": "TEST1", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d807c6ba6c41d0a3a9b62", "username": "TEST2", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d80829fe3bd409bdfbb16", "username": "TEST3", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d83b0a3ceedc1288f778f", "username": "TEST4", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d85bb699d612eefcf96ce", "username": "ADMIN1", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d86bb37ebbd27cf144f21", "username": "HAMED", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d8bfd5cb0b454f48d866d", "username": "TEST1000", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "6013fbe73dba60d126f40d59", "username": "ADDDED", "role": "SUPPORT", "permissions": ["READ"]}]})

    def test_get_users_mutation_5(self):
        task = authentication_service.get_users.s(
            
        ).apply()
        self.assertEqual(task.result, {"content": [{"_id": "5f3d1835c222f53d2d579c65", "username": "ADMIN", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d495e900f8d106922845e", "username": "MAMAD", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d4a807427f2825a08630c", "username": "AKBAR", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7c963054679d5f7f37ea", "username": "JAVAD", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7e0a861f9ade1e1484f9", "username": "SAJAD", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7ec042bdd941b2cadcae", "username": "SOGHRA", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7f0e3fec790b6e79ed81", "username": "TEST0", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d7f6ee8db26f83c9a00a9", "username": "TEST1", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d807c6ba6c41d0a3a9b62", "username": "TEST2", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d80829fe3bd409bdfbb16", "username": "TEST3", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d83b0a3ceedc1288f778f", "username": "TEST4", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d85bb699d612eefcf96ce", "username": "ADMIN1", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d86bb37ebbd27cf144f21", "username": "HAMED", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "5f3d8bfd5cb0b454f48d866d", "username": "TEST1000", "role": "ADMIN", "permissions": ["ADMIN", "READ", "WRITE", "DELETE"]}, {"_id": "6013fbe73dba60d126f40d59", "username": "ADDDED", "role": "SUPPORT", "permissions": ["READ"]}]})

if __name__ == '__main__':
    unittest.main(verbosity=2)