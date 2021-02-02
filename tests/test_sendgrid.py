import unittest
import sys, os
import json
sys.path.append(os.getcwd())
os.environ['APP_RUN_ENV'] = 'development'

from services import sendgrid_service

class SendgridTests(unittest.TestCase):
    _ = None
    def test_get_new_contact_list_mutation_0(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_new_contact_list_mutation_1(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_new_contact_list_mutation_2(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_new_contact_list_mutation_3(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_new_contact_list_mutation_4(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_new_contact_list_mutation_5(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_new_contact_list_mutation_6(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_new_contact_list_mutation_7(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_new_contact_list_mutation_8(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_new_contact_list_mutation_9(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_new_contact_list_mutation_10(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_new_contact_list_mutation_11(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_new_contact_list_mutation_12(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_new_contact_list_mutation_13(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_new_contact_list_mutation_14(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_new_contact_list_mutation_15(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_new_contact_list_mutation_16(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_new_contact_list_mutation_17(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

    def test_get_new_contact_list_mutation_18(self):
        task = sendgrid_service.get_new_contact_list.s(
            
        ).apply()
        self.assertEqual(task.result, null)

if __name__ == '__main__':
    unittest.main(verbosity=2)
