import unittest
import sys, os
import json
sys.path.append(os.getcwd())
os.environ['APP_RUN_ENV'] = 'development'

from services import match_service

class MatchTests(unittest.TestCase):
    _ = None
if __name__ == '__main__':
    unittest.main(verbosity=2)
