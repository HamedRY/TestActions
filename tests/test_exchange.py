import unittest
import sys, os
import json
sys.path.append(os.getcwd())
os.environ['APP_RUN_ENV'] = 'development'

from services import exchange_service

class ExchangeTests(unittest.TestCase):
    def __init__(self):
        pass
if __name__ == '__main__':
    unittest.main(verbosity=2)
