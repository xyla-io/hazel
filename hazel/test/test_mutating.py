import unittest
import code
import pytest
import pandas as pd

from ..api import GoogleAdWordsAPI, GoogleAdsAPI
from ..mutating import GoogleAdsCampaignPauseMutator, GoogleAdsCampaignBudgetMutator

@pytest.fixture
def api():
  api = GoogleAdsAPI(
    developer_token='DEVELOPER_TOKEN',
    client_id='CLIENT_ID',
    client_secret='CLIENT_SECRET',
    refresh_token='REFRESH_TOKEN',
    login_customer_id='LOGIN_CUSTOMER_ID',
    customer_id='CUSTOMER_ID'
  )
  yield api

def test_pause_campaign(api):
  mutator = GoogleAdsCampaignPauseMutator(api=api, campaign_id='CAMPAIGNID')
  response = mutator.mutate()
  assert response
  import pdb; pdb.set_trace()

def test_mutate_campaign_budget(api):
  mutator = GoogleAdsCampaignBudgetMutator(
    api=api,
    campaign_id='CAMPAIGNID',
    budget_micros=5000000
  )
  response = mutator.mutate()
  assert response
  import pdb; pdb.set_trace()
