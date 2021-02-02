import unittest
import sys, os
import json
sys.path.append(os.getcwd())
os.environ['APP_RUN_ENV'] = 'development'

from services import payment_service

class PaymentTests(unittest.TestCase):
    _ = None
    def test_store_unpaid_users_mutation_0(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_store_unpaid_users_mutation_1(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_store_unpaid_users_mutation_2(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_store_unpaid_users_mutation_3(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_store_unpaid_users_mutation_4(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_store_unpaid_users_mutation_5(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_store_unpaid_users_mutation_6(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_store_unpaid_users_mutation_7(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_store_unpaid_users_mutation_8(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_store_unpaid_users_mutation_9(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_store_unpaid_users_mutation_10(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_store_unpaid_users_mutation_11(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_store_unpaid_users_mutation_12(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_store_unpaid_users_mutation_13(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_store_unpaid_users_mutation_14(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_store_unpaid_users_mutation_15(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_store_unpaid_users_mutation_16(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_store_unpaid_users_mutation_17(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_store_unpaid_users_mutation_18(self):
        task = payment_service.store_unpaid_users.s(
            
        ).apply()
        self.assertEqual(task.result, null)

if __name__ == '__main__':
    unittest.main(verbosity=2)
