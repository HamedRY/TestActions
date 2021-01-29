import unittest
import sys, os
import json
sys.path.append(os.getcwd())
os.environ['APP_RUN_ENV'] = 'development'

from services import gold_service

class GoldTests(unittest.TestCase):
    # def __init__(self):
    #     pass
    def test_get_top_ids_mutation_0(self):
        task = gold_service.get_top_ids.s(
            
        ).apply()
        self.assertEqual(task.result, {"content": []})

    def test_get_top_ids_mutation_0(self):
        task = gold_service.get_top_ids.s(
            
        ).apply()
        self.assertEqual(task.result, {"content": []})

    def test_get_gold_ids_mutation_0(self):
        task = gold_service.get_gold_ids.s(
            None, None, 'desc'
        ).apply()
        self.assertEqual(task.result, {"content": []})

if __name__ == '__main__':
    unittest.main(verbosity=2)
