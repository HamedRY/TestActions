from pymongo import MongoClient
from bson.objectid import ObjectId
import unittest
import json

_mongo_client = MongoClient('mongodb://localhost:27017', connect=False)
_skynet_db = _mongo_client['Skynet']
_revenyou_db = _mongo_client['revenyou']

def get_some_stuff():
    return list(_revenyou_db.sign_up.find({}).limit(1))


class CustomerTests(unittest.TestCase):
    def test_TASK_NAME_mutation_TEST_NUMBER(self):
        result = get_some_stuff()
        self.assertEqual(result, [{
            "_id" : ObjectId("5e8e17f4e19ede2b232af5f4"),
            "first_name" : "example",
            "last_name" : "account",
            "gender" : "female",
            "dob" : "2006-12-27",
            "birth_place" : "NLD",
            "nationality" : "nld",
            "country" : "nld",
            "selfie_id" : "",
            "swiftdil_customer_id" : "4969f667-3f59-4b73-8db9-68e26647c01e",
            "swiftdil_screening_id" : "da470a1a-fb9d-4ee3-8a46-a3811fb50ab2",
            "swiftdil_document_id" : "da470a1a-fb9d-4ee3-8a46-a3811fb50ab2",
            "swiftdil_verification_id" : "da470a1a-fb9d-4ee3-8a46-a3811fb50ab2",
            "swiftdil_identity_id" : "da470a1a-fb9d-4ee3-8a46-a3811fb50ab2",
            "user_id" : "5f14a4d245900f6e34b7c084",
            "carrier_id" : "5f14a3b045900f6e34b7c06d",
            "youhex_name" : "example-name-IAlakgiscIEAnYA",
            "youhex_production_id" : "5e06842a9bcf3804022ae644",
            "youhex_paper_trading_id" : "5e06842ae8f42a08fb52afa8",
            "status" : "approved",
            "manual_check" : "approved",
            "date_added" : 1577485354,
            "date_modified" : 1577485354,
            "account_number" : "NL37BUNQ2206801778"
        }])

if __name__ == '__main__':
    unittest.main(verbosity=2)