import unittest
import sys, os
import json
sys.path.append(os.getcwd())
os.environ['APP_RUN_ENV'] = 'development'

from services import feedback_service

class FeedbackTests(unittest.TestCase):
    _ = None
    def test_get_feedbacks_mutation_0(self):
        task = feedback_service.get_feedbacks.s(
            20, 0, 'version', 'desc', None
        ).apply()
        self.assertEqual(task.result, {"total_elements": 2, "total_pages": 1, "current_page": 0, "content": [{"_id": "6017edf8eec798982293b191", "message": "If near have good.\nDecade yard three ball.\nStage listen budget vote city analysis go. Participant yeah those beat well network read.", "score": 8, "type": "loveit", "user_id": "6017edf6eec798982293a944", "version": "1.6.0"}, {"_id": "6017edf8eec798982293b192", "message": "Site company them agent offer. Student during land pressure about question.\nForget oil more.", "score": 5, "type": "loveit", "user_id": "6017edf6eec798982293a952", "version": "1.1.1"}]})

if __name__ == '__main__':
    unittest.main(verbosity=2)
