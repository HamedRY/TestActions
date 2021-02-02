import unittest
import sys, os
import json
sys.path.append(os.getcwd())
os.environ['APP_RUN_ENV'] = 'development'

from services import lookup_service

class LookupTests(unittest.TestCase):
    _ = None
    def test_get_withdrawal_deposit_lookup_mutation_0(self):
        task = lookup_service.get_withdrawal_deposit_lookup.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_deposit_withdrawal_lookup_mutation_0(self):
        task = lookup_service.get_deposit_withdrawal_lookup.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_deposit_withdrawal_lookup_mutation_0(self):
        task = lookup_service.get_deposit_withdrawal_lookup.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_withdrawal_deposit_lookup_mutation_0(self):
        task = lookup_service.get_withdrawal_deposit_lookup.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_withdrawal_deposit_lookup_mutation_1(self):
        task = lookup_service.get_withdrawal_deposit_lookup.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_deposit_withdrawal_lookup_mutation_1(self):
        task = lookup_service.get_deposit_withdrawal_lookup.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_withdrawal_deposit_lookup_mutation_1(self):
        task = lookup_service.get_withdrawal_deposit_lookup.s(
            
        ).apply()
        self.assertEqual(task.result, null)

if __name__ == '__main__':
    unittest.main(verbosity=2)
