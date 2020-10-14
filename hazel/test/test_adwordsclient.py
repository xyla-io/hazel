import unittest
from ..hazel import AdWordsClient

class Test_AdWordsClient(unittest.TestCase):
    def test_existence(self):
        """
        Test that the AdWordsClient class exists
        """
        self.assertIsNotNone(AdWordsClient)