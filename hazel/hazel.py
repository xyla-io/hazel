from googleads import adwords, oauth2
import multiprocessing
import pandas as pd
import pdb
from typing import Dict

PAGE_SIZE = 100

class AdWordsClientOptions:
  developer_token: str
  client_id: str
  client_secret: str
  refresh_token: str

  def __init__(self, options: Dict[str, str]):
    self.developer_token = options['developer_token']
    self.client_id = options['client_id']
    self.client_secret = options['client_secret']
    self.refresh_token = options['refresh_token']


class AdWordsClient(object):
  options: AdWordsClientOptions

  def __init__(self, options: AdWordsClientOptions):
    self.options = options
    refresh_token_client = oauth2.GoogleRefreshTokenClient(client_id=options.client_id, client_secret=options.client_secret, refresh_token=options.refresh_token)
    self.client = adwords.AdWordsClient(developer_token=options.developer_token, refresh_token=options.refresh_token, oauth2_client=refresh_token_client)
  
  @property
  def client_customer_id(self):
    return self.client.client_customer_id

  @client_customer_id.setter
  def client_customer_id(self, id):
    self.client.SetClientCustomerId(id)

  def getCampaigns(self):
    # Initialize appropriate service.
    campaign_service = self.client.GetService('CampaignService', version='v201809')

    # Construct selector and get all campaigns.
    offset = 0
    selector = {
      'fields': ['Id', 'Name', 'Status', 'TargetCpa'],
      'paging': {
        'startIndex': str(offset),
        'numberResults': str(PAGE_SIZE)
      }
    }

    more_pages = True
    campaigns = []
    while more_pages:
      page = campaign_service.get(selector)

      # Display results.
      if 'entries' in page:
        for campaign in page['entries']:
          campaigns.append(campaign)

      offset += PAGE_SIZE
      selector['paging']['startIndex'] = str(offset)
      more_pages = offset < int(page['totalNumEntries'])

    return campaigns

  def getReportFields(self, report_type):
    # Initialize appropriate service.
    report_definition_service = self.client.GetService(
        'ReportDefinitionService', version='v201809')

    # Get report fields.
    fields = report_definition_service.getReportFields(report_type)

    # Display results.
    print('Report type "%s" contains the following fields:' % report_type)
    for field in fields:
      print(' - %s (%s)' % (field['fieldName'], field['fieldType']))
      if 'enumValues' in field:
        print('  := [%s]' % ', '.join(field['enumValues']))

  def getCustomerIDs(self):
    """Retrieves all CustomerIds in the account hierarchy.

    Note that your configuration file must specify a client_customer_id belonging
    to an AdWords manager account.

    Raises:
      Exception: if no CustomerIds could be found.

    Returns:
      A Queue instance containing all CustomerIds in the account hierarchy.
    """
    # For this example, we will use ManagedCustomerService to get all IDs in
    # hierarchy that do not belong to MCC accounts.
    managed_customer_service = self.client.GetService('ManagedCustomerService',
                                                  version='v201809')

    offset = 0

    # Get the account hierarchy for this account.
    selector = {
      'fields': ['CustomerId'],
      'predicates': [{
        'field': 'CanManageClients',
        'operator': 'EQUALS',
        'values': [False],
      }],
      'paging': {
        'startIndex': str(offset),
        'numberResults': str(PAGE_SIZE),
      }
    }

    # Using Queue to balance load between processes.
    queue = multiprocessing.Queue()
    more_pages = True

    while more_pages:
      page = managed_customer_service.get(selector)

      if page and 'entries' in page and page['entries']:
        for entry in page['entries']:
          queue.put(entry['customerId'])
      else:
        raise Exception('Can\'t retrieve any customer ID.')
      offset += PAGE_SIZE
      selector['paging']['startIndex'] = str(offset)
      more_pages = offset < int(page['totalNumEntries'])

    return queue

  def getCriteriaReport(self, start_date, end_date, columns):
    # Initialize appropriate service.
    report_downloader = self.client.GetReportDownloader(version='v201809')

    # Create report query.
    report_query = (adwords.ReportQueryBuilder()
                    .Select(*columns)
                    .From('CRITERIA_PERFORMANCE_REPORT')
                    .Where('Status').In('ENABLED', 'PAUSED')
                    .During(start_date=start_date, end_date=end_date)
                    .Build())

    stream_data = report_downloader.DownloadReportAsStreamWithAwql(report_query,
                                                                    'CSV',
                                                                    skip_report_header=True,
                                                                    skip_report_summary=True)

    df = pd.read_csv(stream_data)

    return df

  def getCampaignReport(self, start_date, end_date, columns):
    # Initialize appropriate service.
    report_downloader = self.client.GetReportDownloader(version='v201809')

    # Create report query.
    report_query = (adwords.ReportQueryBuilder()
                    .Select(*columns)
                    .From('CAMPAIGN_PERFORMANCE_REPORT')
                    .During(start_date=start_date, end_date=end_date)
                    .Build())

    stream_data = report_downloader.DownloadReportAsStreamWithAwql(report_query,
                                                                    'CSV',
                                                                    skip_report_header=True,
                                                                    skip_report_summary=True)

    df = pd.read_csv(stream_data)

    return df

  def getReport(self, report_type, start_date, end_date):
    if report_type == 'CRITERIA_PERFORMANCE_REPORT': return self.getCriteriaReport(start_date, end_date)
    elif report_type == 'CAMPAIGN_PERFORMANCE_REPORT': return self.getCampaignReport(start_date, end_date)
    else: raise ValueError('unsupported report type', report_type)
