import unittest
import os
import code
import pytest
import pandas as pd

from ..api import GoogleAdWordsAPI, GoogleAdsAPI
from ..reporting import GoogleAdWordsReporter, GoogleAdsReporter
from datetime import datetime, timedelta
from typing import Callable, Optional

@pytest.fixture
def reporter():
  api = GoogleAdWordsAPI(
    developer_token='DEVELOPER_TOKEN',
    client_id='CLIENT_ID',
    client_secret='CLIENT_SECRET',
    refresh_token='REFRESH_TOKEN',
  )
  api.customer_id = 1000000
  reporter = GoogleAdWordsReporter(api=api)
  yield reporter

@pytest.fixture
def ads_reporter():
  api = GoogleAdsAPI(
    developer_token='DEVELOPER_TOKEN',
    client_id='CLIENT_ID',
    client_secret='CLIENT_SECRET',
    refresh_token='REFRESH_TOKEN',
    login_customer_id='LOGIN_CUSTOMER_ID',
    customer_id='CUSTOMER_ID'
  )
  ads_reporter = GoogleAdsReporter(api=api)
  yield ads_reporter

def consolidate_reports(ads_reporter: GoogleAdsReporter, report_getter: Callable[[datetime, datetime, Optional[str]], pd.DataFrame], start_date: Optional[datetime]=None, end_date: Optional[datetime]=None):
  end = end_date if end_date else datetime.utcnow().date()
  start = start_date if start_date else end - timedelta(days=0)
  customer_ids = ads_reporter.api.get_customers()
  df = pd.DataFrame()
  for customer_id in customer_ids:
    is_manager = ads_reporter.api.customer_is_manager(customer_id=customer_id)
    if is_manager is None or is_manager:
      continue
    customer_df = report_getter(start_date=start, end_date=end, customer_id=customer_id)
    if customer_df.empty:
      continue
    df = df.append(customer_df)
  return df

def test_google_ads_ad_reporting(ads_reporter):
  df = consolidate_reports(
    ads_reporter=ads_reporter,
    report_getter=ads_reporter.get_ad_report
  )
  df.to_csv(os.path.join('output', 'test', 'get_ad_report.csv'))
  # import pdb; pdb.set_trace()
  print('\n', df)
  assert df is not None
  assert not df.empty

def test_google_ads_asset_reporting(ads_reporter):
  def run_report(start_date: datetime, end_date: datetime, *args, **kwargs):
    return ads_reporter.get_asset_report(*args, **kwargs)

  df = consolidate_reports(
    ads_reporter=ads_reporter,
    report_getter=run_report
  )
  df.to_csv(os.path.join('output', 'test', 'get_asset_report.csv'))
  # import pdb; pdb.set_trace()
  print('\n', df)
  assert df is not None
  assert not df.empty

def test_google_ads_ad_asset_reporting(ads_reporter):
  df = consolidate_reports(
    ads_reporter=ads_reporter,
    report_getter=ads_reporter.get_ad_asset_report
  )
  df.to_csv(os.path.join('output', 'test', 'get_ad_asset_report.csv'))
  # import pdb; pdb.set_trace()
  print('\n', df)
  assert df is not None
  assert not df.empty

def test_google_ads_ad_conversion_action_reporting(ads_reporter):
  df = consolidate_reports(
    ads_reporter=ads_reporter,
    report_getter=ads_reporter.get_ad_conversion_action_report
  )
  df.to_csv(os.path.join('output', 'test', 'get_ad_conversion_action_report.csv'))
  # import pdb; pdb.set_trace()
  print('\n', df)
  assert df is not None
  assert not df.empty

def test_google_ads_campaign_reporting(ads_reporter):
  end = datetime.utcnow().date()
  start = end - timedelta(days=6)
  customer_ids = ads_reporter.api.get_customers()
  df = pd.DataFrame()
  for customer_id in customer_ids:
    is_manager = ads_reporter.api.customer_is_manager(customer_id=customer_id)
    if is_manager is None or is_manager:
      continue
    customer_df = ads_reporter.get_campaign_performance_report(start_date=start, end_date=end, customer_id=customer_id)
    if customer_df.empty:
      continue
    df = df.append(customer_df)

  df['cost'] = df['metrics#cost_micros'].apply(lambda c: None if pd.isna(c) else c / 1000000)
  df.sort_values(by=['campaign#id', 'segments#date'])
  df.to_csv(os.path.join('output', 'test', 'get_campaign_performance_report.csv'))
  print('\n', df)
  assert df is not None
  assert not df.empty

def test_google_ads_ad_group_reporting(ads_reporter):
  end = datetime.utcnow().date()
  start = end - timedelta(days=6)
  customer_ids = ads_reporter.api.get_customers()
  df = pd.DataFrame()
  for customer_id in customer_ids:
    is_manager = ads_reporter.api.customer_is_manager(customer_id=customer_id)
    if is_manager is None or is_manager:
      continue
    customer_df = ads_reporter.get_ad_group_report(start_date=start, end_date=end, customer_id=customer_id)
    if customer_df.empty:
      continue
    df = df.append(customer_df)

  df['cost'] = df['metrics#cost_micros'].apply(lambda c: None if pd.isna(c) else c / 1000000)
  df.sort_values(by=['campaign#id', 'segments#date'])
  # df.to_csv(os.path.join('output', 'test', 'get_campaign_performance_report.csv'))
  import pdb; pdb.set_trace()
  print('\n', df)
  assert df is not None
  assert not df.empty

def test_google_ads_campaign_conversion_action_reporting(ads_reporter):
  end = datetime.utcnow().date()
  start = end - timedelta(days=6)
  customer_ids = ads_reporter.api.get_customers()
  df = pd.DataFrame()
  for customer_id in customer_ids:
    is_manager = ads_reporter.api.customer_is_manager(customer_id=customer_id)
    if is_manager is None or is_manager:
      continue
    customer_df = ads_reporter.get_campaign_conversion_action_report(start_date=start, end_date=end, customer_id=customer_id)
    if customer_df.empty:
      continue
    df = df.append(customer_df)

  df.sort_values(by=['campaign#id', 'segments#date'])
  df.to_csv(os.path.join('output', 'test', 'get_campaign_conversion_actions_report.csv'))
  # import pdb; pdb.set_trace()
  print('\n', df)
  assert df is not None
  assert not df.empty
  assert not df[df['segments#conversion_action'] == df['conversion_action#resource_name']].empty

def test_web_campaign_reporting(ads_reporter):
  start = datetime(2019, 6, 11)
  end = datetime(2019, 6, 17)
  customer_ids = ads_reporter.api.get_customers()
  df = pd.DataFrame()
  for customer_id in customer_ids:
    is_manager = ads_reporter.api.customer_is_manager(customer_id=customer_id)
    if is_manager is None or is_manager:
      continue
    customer_df = ads_reporter.get_web_keyword_report(start_date=start, end_date=end, customer_id=customer_id)
    if customer_df.empty:
      continue
    df = df.append(customer_df)

  print(df)
  assert df is not None
  assert not df.empty

def test_creative_conversion_reporting(reporter):
  start = datetime(2018, 1, 1)
  end = datetime(2018, 12, 18)
  df = reporter.get_creative_conversion_report(
    start_date=start, 
    end_date=end,
    columns=[
      'CampaignId',
      'CampaignName',
      'AdGroupId',
      'AdGroupName',
      'CreativeId',
      'Impressions',
      'CreativeConversions',
      'Date',
    ]
  )

  print(df)
  assert df is not None
  assert not df.empty

def test_ad_performance_reporting(reporter):
  start = datetime(2018, 1, 1)
  end = datetime(2018, 12, 18)
  df = reporter.get_ad_performance_report(
    start_date=start, 
    end_date=end,
    columns=[
      'UniversalAppAdDescriptions',
      'UniversalAppAdImages',
    ]
  )

  print(df)
  assert df is not None
  assert not df.empty