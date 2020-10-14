import unittest
import code
import pytest
import pandas as pd

from ..api import GoogleAdWordsAPI, GoogleAdsAPI

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

def test_get_customers(api):
  customers = api.get_customers()
  assert type(customers) is list

def test_campaign_targeting_info(api):
  customer_ids = api.get_customers()
  target_info_dicts = []
  for customer_id in customer_ids:
    is_manager = api.customer_is_manager(customer_id=customer_id)
    if is_manager is None or is_manager:
      continue
    customer_info = api.get_campaign_target_info(customer_id=customer_id)
    if not customer_info:
      continue
    target_info_dicts.append(customer_info)

  print(target_info_dicts)
  assert target_info_dicts
  import pdb; pdb.set_trace()

def test_get_campaigns(api):
  customer_ids = api.get_customers()
  campaigns = api.get_campaigns(customer_id=customer_ids[0])
  assert campaigns[0]['campaign_id']
  assert campaigns[0]['campaign_name']

def test_get_ad_groups(api):
  customer_ids = api.get_customers()
  campaigns = api.get_campaigns(customer_id=customer_ids[0])
  ad_groups = api.get_ad_groups(
    customer_id=customer_ids[0],
    campaign_id=campaigns[1]['campaign_id']
  )
  assert ad_groups[0]['ad_group_id']
  assert ad_groups[0]['ad_group_name']

def test_customer_metadata(api):
  customers = api.get_customers()
  metadata = api.get_customer_metadata(customer_id=customers[0])
  assert 'customer_manager' in metadata
  assert 'customer_descriptive_name' in metadata

def test_pause_campaign(api):
  response = api.pause_campaign(campaign_id='CAMPAIGN_ID')
  assert response
  import pdb; pdb.set_trace()
