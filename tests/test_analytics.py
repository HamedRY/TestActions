import unittest
import sys, os
import json
sys.path.append(os.getcwd())
os.environ['APP_RUN_ENV'] = 'development'

from services import analytics_service

class AnalyticsTests(unittest.TestCase):
    _ = None
    def test_daily_new_deposit_aggregator_mutation_0(self):
        task = analytics_service.daily_new_deposit_aggregator.s(
            
        ).apply()
        self.assertEqual(task.result, None)

if __name__ == '__main__':
    unittest.main(verbosity=2)