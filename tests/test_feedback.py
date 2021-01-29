import unittest
import sys, os
import json
sys.path.append(os.getcwd())
os.environ['APP_RUN_ENV'] = 'development'

from services import feedback_service

class FeedbackTests(unittest.TestCase):
    # def __init__(self):
    #     pass
    def test_get_feedbacks_mutation_0(self):
        task = feedback_service.get_feedbacks.s(
            20, 0, 'version', 'desc', None
        ).apply()
        self.assertEqual(task.result, {"total_elements": 2, "total_pages": 1, "current_page": 0, "content": [{"_id": "6013f192d7b676a9305f4585", "message": "Material floor him fish deal born. Thus station realize cultural hospital fish.\nName away unit financial design. Statement among face including federal part.", "score": 4, "type": "relaxing", "user_id": "6013f191d7b676a9305f3e0c", "version": "1.3.1"}, {"_id": "6013f192d7b676a9305f4584", "message": "With knowledge including the. Society message effort goal. Hundred last tax want.", "score": 4, "type": "super", "user_id": "6013f191d7b676a9305f3e0b", "version": "1.2.0"}]})

    def test_get_feedbacks_mutation_0(self):
        task = feedback_service.get_feedbacks.s(
            20, 0, 'version', 'desc', None
        ).apply()
        self.assertEqual(task.result, {"total_elements": 2, "total_pages": 1, "current_page": 0, "content": [{"_id": "6013f192d7b676a9305f4585", "message": "Material floor him fish deal born. Thus station realize cultural hospital fish.\nName away unit financial design. Statement among face including federal part.", "score": 4, "type": "relaxing", "user_id": "6013f191d7b676a9305f3e0c", "version": "1.3.1"}, {"_id": "6013f192d7b676a9305f4584", "message": "With knowledge including the. Society message effort goal. Hundred last tax want.", "score": 4, "type": "super", "user_id": "6013f191d7b676a9305f3e0b", "version": "1.2.0"}]})

if __name__ == '__main__':
    unittest.main(verbosity=2)
