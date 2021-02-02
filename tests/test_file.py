import unittest
import sys, os
import json
sys.path.append(os.getcwd())
os.environ['APP_RUN_ENV'] = 'development'

from services import file_service

class FileTests(unittest.TestCase):
    _ = None
if __name__ == '__main__':
    unittest.main(verbosity=2)
