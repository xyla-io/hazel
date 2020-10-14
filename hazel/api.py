import os
import sys
import json
import google
import pandas as pd

from .base import handle_ga_permission_error
from .query import GoogleAdsQuery
from googleads import adwords, oauth2
from typing import Dict, List, Set, Optional, Callable
from string import Formatter
from google.ads.google_ads.client import GoogleAdsClient
from google.ads.google_ads.v3.services.enums import DeviceEnum
from google.api_core import protobuf_helpers
from moda import log

GoogleAdsValueExtractorType = Callable[[any, List[str], any, any, any, str, any, bool, bool, bool], Optional[Dict[str, any]]]

class GoogleAdsAPI:
  client: GoogleAdsClient
  customer_id: Optional[str]
  api_version: str
  _page_size = 1000

  def __init__(self, developer_token: str, client_id: str, client_secret: str, refresh_token: str, login_customer_id: Optional[str]=None, customer_id: Optional[str]=None, api_version: str='v3'):
    login_config = f'login_customer_id: {login_customer_id}' if login_customer_id is not None else ''
    config = f'''
developer_token: {developer_token}
client_id: {client_id}
client_secret: {client_secret}
refresh_token: {refresh_token}
{login_config}
    '''
    self.client = GoogleAdsClient.load_from_string(yaml_str=config)
    self.customer_id = customer_id
    self.api_version = api_version

  @handle_ga_permission_error()
  def customer_is_manager(self, customer_id: str=None) -> Optional[bool]:
    ga_service = self.client.get_service('GoogleAdsService', version=self.api_version)
    query = GoogleAdsQuery(
      query=('SELECT customer.manager '
             'FROM customer '
             'WHERE customer.id = {customer_id}'),
      parameters={'customer_id': customer_id}
    )
    response = ga_service.search(customer_id, query.query_text, page_size=self._page_size)
    rows = list(response)
    return bool(rows[0].customer.manager.value) if rows else None

  def get_customer_hierarchy(self, exclude_customers: List[str]=[]) -> Dict[str, any]:
    def _filter_hierarchy(hierarchy: Dict[str, any], exclude_customers: List[str]):
      return {
        i: _filter_hierarchy(h, exclude_customers) 
        for i,h in hierarchy.items()
        if i not in exclude_customers
      } if exclude_customers and hierarchy else hierarchy

    hierarchy = {
      self.customer_id: self._get_customer_hierarchy(
        customer_id=self.customer_id,
        ignore_customers=[]
      )
    }
    return _filter_hierarchy(hierarchy, exclude_customers)

  @handle_ga_permission_error()
  def _get_customer_hierarchy(self, customer_id: str, ignore_customers: List[str]) -> Dict[str, any]:
    ga_service = self.client.get_service('GoogleAdsService', version=self.api_version)
    query = ('SELECT customer_client_link.client_customer, customer_client_link.status\n'
             'FROM customer_client_link')
    response = ga_service.search(customer_id, query, page_size=self._page_size)
    rows = list(response)
    client_customer_ids = [row.customer_client_link.client_customer.value.split('/')[1] for row in rows]
    client_customer_ids = sorted(filter(lambda i: i not in ignore_customers, client_customer_ids))
    hierarchy = {
      i: self._get_customer_hierarchy(
        customer_id=i,
        ignore_customers=ignore_customers
      )
      for i in client_customer_ids
    }
    hierarchy = {
      i: h for i,h in hierarchy.items() 
      if i not in ignore_customers
    }
    ignore_customers.extend(client_customer_ids)
    return hierarchy

  def get_customer_ids(self, exclude_customers: List[str]=[]) -> List[str]:
    def _get_ids(hierarchy: Dict[str, any]) -> List[str]:
      if not hierarchy:
        return []
      return list(hierarchy.keys()) + [
        id for value in hierarchy.values() 
        for id in _get_ids(value)
      ]

    if exclude_customers:
      hierarchy = self.get_customer_hierarchy(exclude_customers=exclude_customers)
      return sorted(_get_ids(hierarchy))
    else:
      return self.get_customers()

  def get_customers(self) -> List[str]:
    ga_service = self.client.get_service('GoogleAdsService', version=self.api_version)
    query = '''
SELECT
	 customer_client.level,
	 customer_client.hidden,
	 customer.id,
	 customer_client.client_customer
FROM
	customer_client
'''
    response = ga_service.search(self.customer_id, query, page_size=self._page_size)
    rows = list(response)
    return sorted([r.customer_client.client_customer.value.split('/')[1] for r in rows])

  @handle_ga_permission_error(default_value=pd.DataFrame())
  def get_campaign_target_info(self, customer_id: str=None) -> Dict[str, any]:
    if customer_id is None:
      customer_id = self.customer_id
    ga_service = self.client.get_service('GoogleAdsService', version=self.api_version)

    query = GoogleAdsQuery(
      query='''
SELECT
  campaign_criterion.device.type,
  campaign_criterion.campaign,
  campaign_criterion.location.geo_target_constant,
  campaign_criterion.negative,
  campaign.id
FROM campaign_criterion
WHERE
  campaign_criterion.type IN (LOCATION, DEVICE)
      '''
    )

    response = ga_service.search(customer_id, query.query_text, page_size=self._page_size)
    df = self.response_to_data_frame(response=response, delimiter='_')

    geo_targets = None
    if 'campaign_criterion_location_geo_target_constant' in df:
      geo_targets = list(filter(lambda v: not pd.isna(v), df['campaign_criterion_location_geo_target_constant'].unique()))
    if geo_targets:
      geo_targets_query = GoogleAdsQuery(
        query='''
SELECT
  geo_target_constant.resource_name,
  geo_target_constant.country_code
FROM
  geo_target_constant
WHERE
  geo_target_constant.resource_name IN {geo_targets}
        ''',
        parameters={'geo_targets': geo_targets}
      )
      geo_targets_response = ga_service.search(customer_id, geo_targets_query.query_text, page_size=self._page_size)
      geo_targets_df = self.response_to_data_frame(response=geo_targets_response, exclude_keys=[], delimiter='_')
      df = df.merge(geo_targets_df, left_on='campaign_criterion_location_geo_target_constant', right_on='geo_target_constant_resource_name', how='left')

    self.substitute_enum_name(df=df, column_name='campaign_criterion_device_type', enum=DeviceEnum.Device)
    df['plus_or_minus'] = df.campaign_criterion_negative.apply(lambda x: '-' if x else '+') if 'campaign_criterion_negative' in df else '+'
    mapping = {
      '{campaign_id}.country_code.{geo_target_constant_country_code}.{plus_or_minus}': '{plus_or_minus}',
      '{campaign_id}.country_codes.{plus_or_minus}': '{geo_target_constant_country_code}',
      '{campaign_id}.device.{campaign_criterion_device_type}.{plus_or_minus}': '{plus_or_minus}',
      '{campaign_id}.devices.{plus_or_minus}': '{campaign_criterion_device_type}',
    }
    return self._data_frame_to_dict(data_frame=df, column_path_map=mapping)

  def _data_frame_to_dict(self, data_frame: pd.DataFrame, column_path_map: Dict[str, str]) -> Dict[str, any]:
    return_dictionary = {}
    for row in data_frame.to_dict(orient='records'):
      for path, target in column_path_map.items():
        column_names = {
          c
          for s in [path, target] 
          for _, c, _, _ in Formatter().parse(s) if c
        }
        if {c for c in column_names if c not in row or pd.isna(row[c])}:
          continue

        substituted_path = path.format(**row)
        target_dictionary = return_dictionary
        path_components = substituted_path.split('.')
        for component in path_components[:-1]:
          if component not in target_dictionary:
            target_dictionary[component] = {}
          target_dictionary = target_dictionary[component]
        
        last_path_component = path_components[-1]
        if last_path_component not in target_dictionary:
          target_dictionary[last_path_component] = []
        target_dictionary[last_path_component] = list(sorted(set(target_dictionary[last_path_component] + [target.format(**row)])))
    return return_dictionary

  def _fields_to_dict(self, field_listable: any, substitute_enum_names: bool, context: Optional[Dict[str, any]]=None, value_extractor: Optional[GoogleAdsValueExtractorType]=None, _root_field_listable: Optional[any]=None, _path: Optional[List[str]]=None) -> Dict[str, any]:
    context = log_context(context=context)
    if _root_field_listable is None:
      _root_field_listable = field_listable
    if _path is None:
      _path = []

    d = {}
    for f in field_listable.ListFields():
      metadata = f[0]
      key = metadata.name
      value = f[1]
      recurse = metadata.type == metadata.TYPE_MESSAGE
      multiple_values = metadata.label == metadata.LABEL_REPEATED

      if value_extractor is not None:
        extracted = value_extractor(
          root_field_listable=_root_field_listable,
          path=_path,
          field_listable=field_listable,
          field=f,
          metadata=metadata,
          key=key,
          value=value,
          recurse=recurse,
          multiple_values=multiple_values,
          substitute_enum_names=substitute_enum_names
        )
        if extracted is not None:
          d.update(extracted)
          continue
      if recurse and multiple_values:
        d[key] = [
          self._fields_to_dict(
            field_listable=v,
            substitute_enum_names=substitute_enum_names,
            context=context,
            value_extractor=value_extractor,
            _root_field_listable=_root_field_listable,
            _path=_path + [key]
          )
          for v in value
        ]
      elif recurse:
        d[key] = self._fields_to_dict(
          field_listable=value,
          substitute_enum_names=substitute_enum_names,
          context=context,
          value_extractor=value_extractor,
          _root_field_listable=_root_field_listable,
          _path=_path + [key]
        )
      elif multiple_values:
        d[key] = list(value)
      else:
        d[key] = value

      if substitute_enum_names and metadata.enum_type:
        d[key] = list(map(lambda v: metadata.enum_type.values_by_number[v].name, d[key])) if multiple_values else metadata.enum_type.values_by_number[d[key]].name if d[key] is not None else None
    return d

  def _flatten_fields_dict(self, fields_dictionary: Dict[str, any], exclude_keys: List[str]=[], exclude_prefixes=[], prefixes: List[str]=[], delimiter: str='#', max_depth: Optional[int]=None, json_encode_repeated: bool=True, flatten_single_keys: Optional[Set[str]]={''}, path_overrides: Dict[str, Dict[str, any]]={}, context: Optional[Dict[str, any]]=None) -> Dict[str, any]:
    context = log_context(context=context)
    flattened_dict = {}
    key_components = None

    def flatten_dict(d: Dict[str, any], parameters: Dict[str, any], flatten_keys: Optional[Set[str]]) -> any:
      dictionary = self._flatten_fields_dict(
        fields_dictionary=d,
        context=context,
        **parameters
      )
      if flatten_keys is not None:
        if not dictionary:
          return None
        elif flatten_keys and len(dictionary) == 1 and list(dictionary.keys())[0] in flatten_keys:
          return list(dictionary.values())[0]
      return dictionary

    def flatten_list(l: List[any], parameters: Dict[str, any], flatten_keys: Optional[Set[str]]) -> List[any]:
      return [
        flatten_dict(i, parameters, flatten_keys) if isinstance(i, dict) 
        else flatten_list(i, parameters, flatten_keys) if isinstance(i, list) 
        else i
        for i in l
      ]

    parameters = {
      'prefixes': prefixes,
      'exclude_keys': exclude_keys,
      'exclude_prefixes': exclude_prefixes,
      'delimiter': delimiter,
      'max_depth': max_depth - 1 if max_depth else None,
      'json_encode_repeated': json_encode_repeated if max_depth is None or max_depth > 0 else False,
      'flatten_single_keys': flatten_single_keys,
      'path_overrides': path_overrides,
    }
    for k, v in fields_dictionary.items():
      overrides = path_overrides[k] if k in path_overrides else {}
      key_parameters = {
        **parameters,
        **overrides,
      }
      if k in key_parameters['exclude_keys']:
        continue
      key_components = key_parameters['prefixes'] + [k] if k not in key_parameters['exclude_prefixes'] else key_parameters['prefixes']
      key = key_parameters['delimiter'].join(key_components)
      key_max_depth = key_parameters['max_depth']
      key_overrides = key_parameters['path_overrides'][k] if k in key_parameters['path_overrides'] else {}
      key_json_encode_repeated = key_parameters['json_encode_repeated']
      key_flatten_single_keys = key_parameters['flatten_single_keys']
      if isinstance(v, dict):
        d = flatten_dict(
          v, 
          parameters={
            **key_parameters,
            'prefixes': key_components if key_max_depth is None or key_max_depth > 0 else [],
            **key_overrides,
          },
          flatten_keys=key_flatten_single_keys
        )
        if not isinstance(d, dict):
          flattened_dict[key] = d
        elif key_max_depth is None or key_max_depth > 0:
          flattened_dict.update(d)
        else:
          flattened_dict[key] = json.dumps(d) if key_json_encode_repeated and key_max_depth == 0 else d
      elif isinstance(v, list):
        l = flatten_list(
          v,
          parameters={
            **key_parameters,
            'prefixes': [],
            'json_encode_repeated': False,
            **key_overrides,
          },
          flatten_keys=key_flatten_single_keys
        )
        flattened_dict[key] = json.dumps(l) if key_json_encode_repeated and key_max_depth is None or key_max_depth == 0 else l
      else:
        flattened_dict[key] = v

    return flattened_dict      

  def response_to_record(self, response: any, substitute_enum_names: bool=False, value_extractor: Optional[GoogleAdsValueExtractorType]=None) -> Dict[str, any]:
    record = self._fields_to_dict(
      field_listable=response,
      substitute_enum_names=substitute_enum_names,
      value_extractor=value_extractor
    )
    return record
    
  def response_to_data_frame(self, response: any, exclude_keys: List[str]=['resource_name'], exclude_prefixes: List[str]=['value'], delimiter: str='#', substitute_enum_names: bool=False, json_encode_repeated: bool=False, flatten_single_keys: Optional[Set[str]]={''}, max_depth: Optional[int]=None, path_overrides: Dict[str, Dict[str, any]]={}, value_extractor: Optional[GoogleAdsValueExtractorType]=None) -> pd.DataFrame:
    fields_context = {'counter': 0}
    log.log('Parsing Google Ads response...')
    flatten_context = {
      'message': 'Flattening Google Ads response objects {counter}...',
      'interval': 100000,
    }
    flattened_records = []
    for row in response:
      dictionary = self._fields_to_dict(
        field_listable=row,
        substitute_enum_names=substitute_enum_names,
        context=fields_context,
        value_extractor=value_extractor
      )
      flattened_record = self._flatten_fields_dict(
        fields_dictionary=dictionary,
        exclude_keys=exclude_keys,
        exclude_prefixes=exclude_prefixes,
        delimiter=delimiter,
        max_depth=max_depth,
        json_encode_repeated=json_encode_repeated,
        flatten_single_keys=flatten_single_keys,
        path_overrides=path_overrides,
        context=flatten_context
      )
      flattened_records.append(flattened_record)

    log.log(f'Parsed {fields_context["counter"]} Google Ads response rows')
    df = pd.DataFrame(flattened_records)
    return df

  def substitute_enum_name(self, df: pd.DataFrame, column_name: str, enum: any):
    if column_name in df:
      df[column_name] = df[column_name].apply(lambda t: list(map(lambda v: enum(v).name, t)) if isinstance(t, list) else t if pd.isna(t) else enum(t).name)

  def substitute_enum_names(self, df: pd.DataFrame, column_to_enum_map: Dict[str, any]):
    for c, e in column_to_enum_map.items():
      self.substitute_enum_name(df=df, column_name=c, enum=e)

  #----------------------------------------------
  # Management
  #----------------------------------------------

  @handle_ga_permission_error()
  def get_customer_metadata(self, customer_id: str=None) -> Optional[bool]:
    ga_service = self.client.get_service('GoogleAdsService', version=self.api_version)
    query = GoogleAdsQuery(
      query=('SELECT customer.manager '
               ', customer.descriptive_name '
             'FROM customer '
             'WHERE customer.id = {customer_id}'),
      parameters={'customer_id': customer_id}
    )
    response = ga_service.search(customer_id, query.query_text, page_size=self._page_size)
    df = self.response_to_data_frame(response=response, delimiter='_')
    assert len(df) == 1
    return df.to_dict(orient='records')[0]

  @handle_ga_permission_error(default_value=pd.DataFrame())
  def get_campaigns(self, customer_id: str=None) -> List[Dict[str, any]]:
    if customer_id is None:
      customer_id = self.customer_id
    ga_service = self.client.get_service('GoogleAdsService', version=self.api_version)

    query = GoogleAdsQuery(
      query=(''
        'SELECT campaign.id'
          ', campaign.name '
        'FROM campaign'
      )
    )

    response = ga_service.search(customer_id, query.query_text, page_size=self._page_size)
    df = self.response_to_data_frame(response=response, delimiter='_')
    return df.to_dict(orient='records')

  @handle_ga_permission_error(default_value=pd.DataFrame())
  def get_ad_groups(self, customer_id: str=None, campaign_id: str=None) -> List[Dict[str, any]]:
    if customer_id is None:
      customer_id = self.customer_id
    ga_service = self.client.get_service('GoogleAdsService', version=self.api_version)

    query = GoogleAdsQuery(
      query=(''
        'SELECT ad_group.id'
          ', ad_group.name '
        'FROM ad_group '
        'WHERE campaign.id = {campaign_id}'
      ),
      parameters={
        'campaign_id': campaign_id
      }
    )

    response = ga_service.search(customer_id, query.query_text, page_size=self._page_size)
    df = self.response_to_data_frame(response=response, delimiter='_')
    return df.to_dict(orient='records')

class GoogleAdWordsAPI:
  client: adwords.AdWordsClient
  _page_size = 100

  def __init__(self, client_id: str, client_secret: str, refresh_token: str, developer_token: str):
    refresh_token_client = oauth2.GoogleRefreshTokenClient(
      client_id=client_id, 
      client_secret=client_secret, 
      refresh_token=refresh_token
    )
    self.client = adwords.AdWordsClient(
      developer_token=developer_token,
      refresh_token=refresh_token,
      oauth2_client=refresh_token_client
    )
    
  @property
  def client_customer_id(self):
    return self.client.client_customer_id

  @client_customer_id.setter
  def client_customer_id(self, id):
    self.client.SetClientCustomerId(id)

  def get_campaigns(self):
    # Initialize appropriate service.
    campaign_service = self.client.GetService(
      'CampaignService', 
      version='v201809'
    )

    # Construct selector and get all campaigns.
    offset = 0
    selector = {
      'fields': ['Id', 'Name', 'Status', 'TargetCpa', 'Settings'],
      'paging': {
        'startIndex': str(offset),
        'numberResults': str(self._page_size)
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

      offset += self._page_size
      selector['paging']['startIndex'] = str(offset)
      more_pages = offset < int(page['totalNumEntries'])

    return campaigns

  def print_report_fields(self, report_type: str):
    # Initialize appropriate service.
    report_definition_service = self.client.GetService(
      'ReportDefinitionService', 
      version='v201809'
    )

    # Get report fields.
    fields = report_definition_service.getReportFields(report_type)

    # Display results.
    log.log('Report type "%s" contains the following fields:' % report_type)
    for field in fields:
      log.log(' - %s (%s)' % (field['fieldName'], field['fieldType']))
      if 'enumValues' in field:
        log.log('  := [%s]' % ', '.join(field['enumValues']))

def log_context(context: Optional[Dict[str, any]]) -> Dict[str, any]:
  """Increments a counter for a recursive function context, and prints a log message at counter intervals"""
  if context is None:
    context = {}
  if 'counter' not in context:
    context['counter'] = 0
  context['counter'] += 1
  if 'message' in context and 'interval' in context and not context['counter'] % context['interval']:
    log.log(context['message'].format(**{k: v for k, v in context.items() if k != 'message'}))
    sys.stdout.flush()

  return context
