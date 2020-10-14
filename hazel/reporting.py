import multiprocessing
import pandas as pd
import google
import json

from .context import GoogleAdsEntity
from .api import GoogleAdsAPI, GoogleAdWordsAPI, GoogleAdsValueExtractorType
from .base import handle_ga_permission_error
from .query import GoogleAdsQuery
from typing import List, Dict, Set, Optional
from datetime import datetime
from googleads import adwords
from google.ads.google_ads.v3.services.enums import KeywordMatchTypeEnum, AdvertisingChannelTypeEnum, CriterionSystemServingStatusEnum, DeviceEnum, BiddingStrategyTypeEnum

class GoogleAdsReporter:
  api: GoogleAdsAPI
  verbose: bool

  def __init__(self, api: GoogleAdsAPI, verbose: bool=False):
    self.api = api
    self.verbose = verbose

  @handle_ga_permission_error()
  def get_query_data_frame(self, query: GoogleAdsQuery, customer_id: Optional[str]=None, exclude_keys: List[str]=['resource_name'], exclude_prefixes: List[str]=['value'], delimiter: str='#', substitute_enum_names: bool=True, json_encode_repeated: bool=True, flatten_single_keys: Optional[Set[str]]={''}, path_overrides: Dict[str, Dict[str, any]]={}, value_extractor: Optional[GoogleAdsValueExtractorType]=None) -> Optional[pd.DataFrame]:
    if customer_id is None:
      customer_id = self.api.customer_id

    ga_service = self.api.client.get_service('GoogleAdsService', version=self.api.api_version)

    response = ga_service.search(customer_id, query=query.query_text, page_size=self.api._page_size)
    df = self.api.response_to_data_frame(
      response=response,
      exclude_keys=exclude_keys,
      exclude_prefixes=exclude_prefixes,
      delimiter=delimiter,
      substitute_enum_names=substitute_enum_names,
      json_encode_repeated=json_encode_repeated,
      flatten_single_keys=flatten_single_keys,
      path_overrides=path_overrides,
      value_extractor=value_extractor
    )
    return df

  def get_ad_report(self, start_date: datetime, end_date: datetime, customer_id: Optional[str]=None, json_encode_repeated: bool=True) -> pd.DataFrame:
    query = GoogleAdsQuery(
      query=(''
        'SELECT customer.id'
          ', customer.descriptive_name'
          ', customer.currency_code'
          ', campaign.id'
          ', campaign.name'
          ', campaign.status'
          ', campaign.app_campaign_setting.app_id'
          ', campaign.app_campaign_setting.app_store'
          ', ad_group.id'
          ', ad_group.name'
          ', ad_group.status'
          ', ad_group.type'
          ', ad_group.base_ad_group'
          ', ad_group_ad.ad.id'
          ', ad_group_ad.ad.name'
          ', ad_group_ad.status'
          ', ad_group_ad.ad.type'
          ', ad_group_ad.ad.app_ad.html5_media_bundles'
          ', ad_group_ad.ad.app_ad.images'
          ', ad_group_ad.ad.app_ad.headlines'
          ', ad_group_ad.ad.app_ad.descriptions'
          ', ad_group_ad.ad.app_ad.mandatory_ad_text'
          ', ad_group_ad.ad.app_ad.youtube_videos'
          ', ad_group_ad.ad.app_engagement_ad.images'
          ', ad_group_ad.ad.app_engagement_ad.videos'
          ', ad_group_ad.ad.call_only_ad.description1'
          ', ad_group_ad.ad.call_only_ad.description2'
          ', ad_group_ad.ad.call_only_ad.headline1'
          ', ad_group_ad.ad.call_only_ad.headline2'
          ', ad_group_ad.ad.display_upload_ad.media_bundle'
          ', ad_group_ad.ad.display_url'
          ', ad_group_ad.ad.expanded_dynamic_search_ad.description'
          ', ad_group_ad.ad.expanded_dynamic_search_ad.description2'
          ', ad_group_ad.ad.expanded_text_ad.description'
          ', ad_group_ad.ad.expanded_text_ad.description2'
          ', ad_group_ad.ad.expanded_text_ad.headline_part1'
          ', ad_group_ad.ad.expanded_text_ad.headline_part2'
          ', ad_group_ad.ad.expanded_text_ad.headline_part3'
          ', ad_group_ad.ad.gmail_ad.marketing_image_description'
          ', ad_group_ad.ad.gmail_ad.marketing_image_display_call_to_action.text'
          ', ad_group_ad.ad.gmail_ad.marketing_image_headline'
          ', ad_group_ad.ad.gmail_ad.teaser.description'
          ', ad_group_ad.ad.gmail_ad.teaser.headline'
          ', ad_group_ad.ad.image_ad.image_url'
          ', ad_group_ad.ad.image_ad.name'
          ', ad_group_ad.ad.image_ad.preview_image_url'
          ', ad_group_ad.ad.legacy_responsive_display_ad.description'
          ', ad_group_ad.ad.legacy_responsive_display_ad.promo_text'
          ', ad_group_ad.ad.legacy_responsive_display_ad.short_headline'
          ', ad_group_ad.ad.responsive_display_ad.descriptions'
          ', ad_group_ad.ad.responsive_display_ad.headlines'
          ', ad_group_ad.ad.responsive_display_ad.logo_images'
          ', ad_group_ad.ad.responsive_display_ad.long_headline'
          ', ad_group_ad.ad.responsive_display_ad.marketing_images'
          ', ad_group_ad.ad.responsive_display_ad.promo_text'
          ', ad_group_ad.ad.responsive_display_ad.square_logo_images'
          ', ad_group_ad.ad.responsive_display_ad.square_marketing_images'
          ', ad_group_ad.ad.responsive_display_ad.youtube_videos'
          ', ad_group_ad.ad.responsive_search_ad.descriptions'
          ', ad_group_ad.ad.responsive_search_ad.headlines'
          ', ad_group_ad.ad.shopping_comparison_listing_ad.headline'
          ', ad_group_ad.ad.text_ad.description1'
          ', ad_group_ad.ad.text_ad.description2'
          ', ad_group_ad.ad.text_ad.headline'
          ', ad_group_ad.ad.video_ad.in_stream.action_headline'
          ', ad_group_ad.ad.video_ad.out_stream.description'
          ', ad_group_ad.ad.video_ad.out_stream.headline'
          ', metrics.cost_micros'
          ', metrics.impressions'
          ', metrics.clicks'
          ', metrics.conversions'
          ', metrics.conversions_value'
          ', segments.date '
        'FROM ad_group_ad '
        'WHERE segments.date >= {start_date} '
          'AND segments.date <= {end_date} '
      ),
      parameters={
        'start_date': start_date,
        'end_date': end_date,
      }
    )
    df = self.get_query_data_frame(
      query=query,
      customer_id=customer_id,
      json_encode_repeated=json_encode_repeated,
      path_overrides={
        'ad_group_ad': {
          'path_overrides': {
            'ad': {
              'path_overrides': {
                'app_ad': {
                  'exclude_prefixes': ['value', 'asset', 'text'],
                },
                'responsive_display_ad': {
                  'exclude_prefixes': ['value', 'text'],
                },
              }
            }
          }
        }
      }
    )
    return df if df is not None else pd.DataFrame()

  def get_asset_report(self, customer_id: Optional[str]=None, assets: Optional[List[str]]=None) -> pd.DataFrame:
    condition = f'WHERE asset.resource_name IN {assets}' if assets is not None else ''
    condition_parameters = {'assets': assets} if assets is not None else {}
    query = GoogleAdsQuery(
      query=(''
        'SELECT customer.id'
          ', customer.descriptive_name'
          ', customer.currency_code'
          ', asset.id'
          ', asset.image_asset.file_size'
          ', asset.image_asset.full_size.height_pixels'
          ', asset.image_asset.full_size.url'
          ', asset.image_asset.full_size.width_pixels'
          ', asset.image_asset.mime_type'
          ', asset.name'
          ', asset.resource_name'
          ', asset.text_asset.text'
          ', asset.type'
          ', asset.youtube_video_asset.youtube_video_id '
        'FROM asset '
        f'{condition}'
      ),
      parameters=condition_parameters
    )
    df = self.get_query_data_frame(
      query=query,
      customer_id=customer_id,
      path_overrides={
        'asset': {
          'exclude_keys': []
        }
      }
    )
    return df if df is not None else pd.DataFrame()

  def get_ad_asset_report(self, start_date: datetime, end_date: datetime, customer_id: Optional[str]=None) -> pd.DataFrame:
    query = GoogleAdsQuery(
      query=(''
        'SELECT customer.id'
          ', customer.descriptive_name'
          ', customer.currency_code'
          ', campaign.id'
          ', campaign.name'
          ', campaign.status'
          ', campaign.app_campaign_setting.app_id'
          ', campaign.app_campaign_setting.app_store'
          ', ad_group.id'
          ', ad_group.name'
          ', ad_group.status'
          ', ad_group.type'
          ', ad_group.base_ad_group'
          ', ad_group_ad.ad.id'
          ', ad_group_ad.ad.name'
          ', ad_group_ad.status'
          ', ad_group_ad.ad.type'
          ', asset.resource_name'
          ', asset.id'
          ', asset.name'
          ', asset.type'
          ', ad_group_ad_asset_view.resource_name'
          ', ad_group_ad_asset_view.field_type'
          ', ad_group_ad_asset_view.performance_label'
          ', ad_group_ad_asset_view.policy_summary'
          ', metrics.cost_micros'
          ', metrics.impressions'
          ', metrics.clicks'
          ', metrics.conversions'
          ', metrics.conversions_value'
          ', segments.date '
        'FROM ad_group_ad_asset_view '
        'WHERE segments.date >= {start_date} '
          'AND segments.date <= {end_date} '
      ),
      parameters={
        'start_date': start_date,
        'end_date': end_date,
      }
    )
    df = self.get_query_data_frame(
      query=query,
      customer_id=customer_id,
      path_overrides={
        'asset': {
          'exclude_keys': [],
        },
        'ad_group_ad_asset_view': {
          'exclude_keys': [],
          'path_overrides': {
            'policy_summary': {
              'max_depth': 0,
            }
          }
        },
      }
    )
    return df if df is not None else pd.DataFrame()

  def get_ad_conversion_action_report(self, start_date: datetime, end_date: datetime, customer_id: Optional[str]=None) -> pd.DataFrame:
    query = GoogleAdsQuery(
      query=(''
        'SELECT customer.id'
          ', customer.descriptive_name'
          ', customer.currency_code'
          ', campaign.id'
          ', campaign.name'
          ', campaign.status'
          ', campaign.app_campaign_setting.app_id'
          ', campaign.app_campaign_setting.app_store'
          ', ad_group.id'
          ', ad_group.name'
          ', ad_group.status'
          ', ad_group.type'
          ', ad_group.base_ad_group'
          ', ad_group_ad.ad.id'
          ', ad_group_ad.ad.name'
          ', ad_group_ad.status'
          ', ad_group_ad.ad.type'
          ', metrics.conversions'
          ', metrics.conversions_value'
          ', segments.date'
          ', segments.conversion_action'
          ', segments.conversion_action_category'
          ', segments.conversion_action_name '
        'FROM ad_group_ad '
        'WHERE segments.date >= {start_date} '
          'AND segments.date <= {end_date} '
      ),
      parameters={
        'start_date': start_date,
        'end_date': end_date,
      }
    )
    df = self.get_query_data_frame(query=query, customer_id=customer_id)
    if df is None or 'segments#conversion_action' not in df:
      return pd.DataFrame()

    conversion_action_query = GoogleAdsQuery(
      query=(''
        'SELECT conversion_action.id'
          ', conversion_action.resource_name'
          ', conversion_action.name'
          ', conversion_action.type'
          ', conversion_action.category'
          ', conversion_action.app_id'
          ', conversion_action.value_settings.default_value'
          ', conversion_action.value_settings.default_currency_code'
          ', metrics.conversion_last_conversion_date '
        'FROM conversion_action '
        'WHERE conversion_action.resource_name IN {conversion_actions}'
      ),
      parameters={
        'conversion_actions': sorted(filter(lambda v: not pd.isna(v), df['segments#conversion_action'].unique()))
      }
    )
    conversion_action_df = self.get_query_data_frame(
      query=conversion_action_query,
      customer_id=customer_id,
      exclude_keys=[]
    )
    if conversion_action_df is None or conversion_action_df.empty:
      return df

    # TODO: Figure out why the conversion_action_query does not retrieve most of the conversion action resources by which the ad query is segmented.
    merged_df = df.merge(
      left_on='segments#conversion_action', 
      right_on='conversion_action#resource_name', 
      right=conversion_action_df,
      how='left'
    )
    return merged_df

  @handle_ga_permission_error(default_value=pd.DataFrame())
  def get_campaign_performance_report(self, start_date: datetime, end_date: datetime, customer_id: str=None) -> pd.DataFrame:
    if customer_id is None:
      customer_id = self.api.customer_id
      
    ga_service = self.api.client.get_service('GoogleAdsService', version=self.api.api_version)

    query = GoogleAdsQuery(
      query=(''
        'SELECT campaign.id '
          ', campaign.network_settings.target_content_network '
          ', campaign.network_settings.target_partner_search_network '
          ', campaign.advertising_channel_type '
          ', campaign.advertising_channel_sub_type '
          ', campaign_budget.amount_micros '
          ', campaign_budget.total_amount_micros '
          ', campaign.app_campaign_setting.app_id '
          ', campaign.app_campaign_setting.app_store '
          ', campaign.app_campaign_setting.bidding_strategy_goal_type '
          ', bidding_strategy.name '
          ', campaign.bidding_strategy_type '
          ', campaign.name '
          ', campaign.status '
          ', campaign.serving_status '
          ', campaign.start_date '
          ', campaign.target_cpa.target_cpa_micros '
          ', customer.id '
          ', customer.descriptive_name '
          ', customer.currency_code '
          ', metrics.clicks '
          ', metrics.conversions '
          ', metrics.cost_micros '
          ', metrics.impressions '
          ', metrics.conversions_value '
          ', metrics.interactions '
          ', metrics.interaction_event_types '
          ', metrics.video_views '
          ', segments.date '
          ', segments.device '
        'FROM campaign '
        'WHERE segments.date >= {start_date} '
          'AND segments.date <= {end_date} '
      ),
      parameters={
        'start_date': start_date,
        'end_date': end_date,
      }
    )
    response = ga_service.search(customer_id, query=query.query_text, page_size=self.api._page_size)
    df = self.api.response_to_data_frame(
      response=response,
      substitute_enum_names=True,
      json_encode_repeated=True
    )

    return df

  @handle_ga_permission_error(default_value=pd.DataFrame())
  def get_ad_group_report(self, start_date: datetime, end_date: datetime, customer_id: str=None) -> pd.DataFrame:
    if customer_id is None:
      customer_id = self.api.customer_id
      
    ga_service = self.api.client.get_service('GoogleAdsService', version=self.api.api_version)

    query = GoogleAdsQuery(
      query=(''
        'SELECT ad_group.id '
          ', ad_group.name '
          ', ad_group.status '
          ', ad_group.type '
          ', ad_group.target_cpa_micros '
          ', ad_group.target_cpm_micros '
          ', ad_group.target_roas '
          ', campaign.id '
          ', campaign.network_settings.target_content_network '
          ', campaign.network_settings.target_partner_search_network '
          ', campaign.advertising_channel_type '
          ', campaign.advertising_channel_sub_type '
          ', campaign.app_campaign_setting.app_id '
          ', campaign.app_campaign_setting.app_store '
          ', campaign.app_campaign_setting.bidding_strategy_goal_type '
          ', campaign.bidding_strategy_type '
          ', campaign.name '
          ', campaign.status '
          ', campaign.serving_status '
          ', campaign.start_date '
          ', campaign.target_cpa.target_cpa_micros '
          ', customer.id '
          ', customer.descriptive_name '
          ', customer.currency_code '
          ', metrics.clicks '
          ', metrics.conversions '
          ', metrics.cost_micros '
          ', metrics.impressions '
          ', metrics.conversions_value '
          ', metrics.interactions '
          ', metrics.interaction_event_types '
          ', metrics.video_views '
          ', segments.date '
          ', segments.device '
        'FROM ad_group '
        'WHERE segments.date >= {start_date} '
          'AND segments.date <= {end_date} '
      ),
      parameters={
        'start_date': start_date,
        'end_date': end_date,
      }
    )
    response = ga_service.search(customer_id, query=query.query_text, page_size=self.api._page_size)
    df = self.api.response_to_data_frame(
      response=response,
      substitute_enum_names=True,
      json_encode_repeated=True
    )

    return df

  @handle_ga_permission_error(default_value=pd.DataFrame())
  def get_campaign_conversion_action_report(self, start_date: datetime, end_date: datetime, customer_id: str=None) -> pd.DataFrame:
    if customer_id is None:
      customer_id = self.api.customer_id
      
    ga_service = self.api.client.get_service('GoogleAdsService', version=self.api.api_version)

    query = GoogleAdsQuery(
      query=(''
        'SELECT campaign.id '
          ', campaign.name '
          ', campaign.status '
          ', metrics.conversions '
          ', metrics.conversions_value '
          ', segments.date '
          ', segments.device '
          ', segments.conversion_action '
          ', segments.conversion_action_name '
          ', segments.conversion_action_category '
          ', segments.click_type '
        'FROM campaign '
        'WHERE segments.date >= {start_date} '
          'AND segments.date <= {end_date} '
      ),
      parameters={
        'start_date': start_date,
        'end_date': end_date,
      }
    )
    response = ga_service.search(customer_id, query=query.query_text, page_size=self.api._page_size)
    df = self.api.response_to_data_frame(
      response=response, 
      substitute_enum_names=True,
      json_encode_repeated=True
    )
    if df.empty:
      return df

    conversion_action_query = GoogleAdsQuery(
      query=(''
        'SELECT conversion_action.id '
          ', conversion_action.resource_name '
          ', conversion_action.name '
          ', conversion_action.type '
          ', conversion_action.category '
          ', conversion_action.app_id '
          ', conversion_action.value_settings.default_value '
          ', conversion_action.value_settings.default_currency_code '
          ', metrics.conversion_last_conversion_date '
        'FROM conversion_action '
        'WHERE conversion_action.resource_name IN {conversion_actions} '
      ),
      parameters={
        'conversion_actions': list(df['segments#conversion_action'].unique())
      }
    )
    conversion_action_response = ga_service.search(customer_id, query=conversion_action_query.query_text, page_size=self.api._page_size)
    conversion_action_df = self.api.response_to_data_frame(response=conversion_action_response, exclude_keys=[], substitute_enum_names=True)
    if conversion_action_df.empty:
      return df

    # TODO: Figure out why the conversion_action_query does not retrieve the conversion action resources by which the campaign query is segmented.
    merged_df = df.merge(
      left_on='segments#conversion_action',
      right_on='conversion_action#resource_name',
      right=conversion_action_df,
      how='outer'
    )

    return merged_df

  def get_campaign_report(self):
    ga_service = self.api.client.get_service('GoogleAdsService', version=self.api.api_version)
    query = ('SELECT campaign.id, campaign.name FROM campaign '
             'ORDER BY campaign.id')
    results = ga_service.search(self.api.customer_id, query=query, page_size=self.api._page_size)

    try:
        for row in results:
            print('Campaign with ID %d and name "%s" was found.'
                  % (row.campaign.id.value, row.campaign.name.value))
    except google.ads.google_ads.errors.GoogleAdsException as ex:
        print('Request with ID "%s" failed with status "%s" and includes the '
              'following errors:' % (ex.request_id, ex.error.code().name))
        for error in ex.failure.errors:
            print('\tError with message "%s".' % error.message)
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print('\t\tOn field: %s' % field_path_element.field_name)

  @handle_ga_permission_error(default_value=pd.DataFrame())
  def get_web_keyword_report(self, start_date: datetime, end_date: datetime, customer_id: str=None):
    if customer_id is None:
      customer_id = self.api.customer_id
    ga_service = self.api.client.get_service('GoogleAdsService', version=self.api.api_version)

    query = GoogleAdsQuery(
      query=('SELECT campaign.id, campaign.name, campaign.advertising_channel_type, ad_group.id, ad_group.name, '
             'ad_group_criterion.criterion_id, '
             'ad_group_criterion.keyword.text, '
             'ad_group_criterion.keyword.match_type, '
             'ad_group_criterion.system_serving_status, '
             'customer.currency_code, '
             'customer.id, '
             'customer.descriptive_name, '
             'metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.average_cpc, '
             'segments.date, '
             'segments.device '
             'FROM keyword_view '
             'WHERE segments.date >= {start_date} '
             'AND segments.date <= {end_date} '
             'AND campaign.advertising_channel_type = \'SEARCH\' '
             'AND ad_group.status = \'ENABLED\' '
             'AND ad_group_criterion.status IN (\'ENABLED\', \'PAUSED\') '
             'ORDER BY segments.date, segments.device, campaign.id, ad_group.id'),
      parameters={
        'start_date': start_date,
        'end_date': end_date,
      }
    )

    response = ga_service.search(customer_id, query.query_text, page_size=self.api._page_size)
    df = self.api.response_to_data_frame(response=response)

    self.api.substitute_enum_name(df=df, column_name='ad_group_criterion#keyword#match_type', enum=KeywordMatchTypeEnum.KeywordMatchType)
    self.api.substitute_enum_name(df=df, column_name='campaign#advertising_channel_type', enum=AdvertisingChannelTypeEnum.AdvertisingChannelType)
    self.api.substitute_enum_name(df=df, column_name='ad_group_criterion#system_serving_status', enum=CriterionSystemServingStatusEnum.CriterionSystemServingStatus)
    self.api.substitute_enum_name(df=df, column_name='segments#device', enum=DeviceEnum.Device)
    return df
  
  @handle_ga_permission_error(default_value=pd.DataFrame())
  def get_performance_report(self, start_date: datetime, end_date: datetime, entity_granularity: str, time_granularity: str, entity_ids: Optional[List[str]]=None) -> pd.DataFrame:
    def select_columns(entity_granularity: str):
      if entity_granularity == 'ad':
        return (
          ', campaign.id'
          ', campaign.name'
          ', campaign.start_date'
          ', campaign.app_campaign_setting.app_id'
          ', campaign.app_campaign_setting.app_store'
          ', campaign.app_campaign_setting.bidding_strategy_goal_type'
          ', campaign.bidding_strategy_type'
          ', campaign.selective_optimization.conversion_actions'
          ', ad_group.id'
          ', ad_group.name'
          ', ad_group.status'
          ', ad_group.type'
          ', ad_group.target_cpa_micros'
          ', ad_group_ad.ad.id'
          ', ad_group_ad.ad.name'
          ', ad_group_ad.status'
        )
      elif entity_granularity == 'ad_group':
        return (
          ', campaign.id'
          ', campaign.name'
          ', campaign.start_date'
          ', campaign.app_campaign_setting.app_id'
          ', campaign.app_campaign_setting.app_store'
          ', campaign.app_campaign_setting.bidding_strategy_goal_type'
          ', campaign.selective_optimization.conversion_actions'
          ', campaign.bidding_strategy_type'
          ', ad_group.id'
          ', ad_group.name'
          ', ad_group.status'
          ', ad_group.type'
          ', ad_group.target_cpa_micros'
        )
      elif entity_granularity == 'campaign':
        return (
          ', campaign.id'
          ', campaign.name'
          ', campaign.start_date'
          ', campaign.app_campaign_setting.app_id'
          ', campaign.app_campaign_setting.app_store'
          ', campaign.app_campaign_setting.bidding_strategy_goal_type'
          ', campaign.bidding_strategy_type'
          ', campaign.selective_optimization.conversion_actions'
        )
    
    def select_table(entity_granularity: str):
      if entity_granularity == 'ad':
        return 'ad_group_ad'
      elif entity_granularity == 'ad_group':
        return 'ad_group'
      elif entity_granularity == 'campaign':
        return 'campaign'

    query = GoogleAdsQuery(
      query=(''
        'SELECT customer.id'
          ', customer.descriptive_name'
          ', customer.currency_code'
          ', customer.time_zone'
          f'{select_columns(entity_granularity)}'
          ', metrics.cost_micros'
          ', metrics.impressions'
          ', metrics.clicks'
          ', metrics.conversions'
          ', metrics.conversions_value'
          f"{(', metrics.all_conversions' if time_granularity != 'hourly' else '')}"
          ', segments.date '
          f"{(', segments.hour ' if time_granularity == 'hourly' else '')}"
        f'FROM {select_table(entity_granularity)} '
        'WHERE segments.date >= {start_date} '
          'AND segments.date <= {end_date} '
          f"{(f'AND {select_table(entity_granularity)}.id IN {{entity_ids}}' if entity_ids is not None else '')}"
      ),
      parameters={
        'start_date': start_date,
        'end_date': end_date,
        **({'entity_ids': entity_ids} if entity_ids is not None else {}),
      }
    )

    df = self.get_query_data_frame(
      query=query,
      customer_id=self.api.customer_id,
      value_extractor=lambda path, key, value, **kwargs: {key: 0} if ((path == ['segments'] and key == 'hour') or path == ['metrics']) and value.value == 0 else None
    )
    if df is None:
      return pd.DataFrame()
    return df

  def entity_table(self, entity_granularity: str):
    if entity_granularity == 'ad':
      return 'ad_group_ad'
    elif entity_granularity == 'ad_group':
      return 'ad_group'
    elif entity_granularity == 'campaign':
      return 'campaign'

  def add_selected_conversions(self, report: pd.DataFrame, start_date: datetime, end_date: datetime, entity_granularity: str, time_granularity: str, suffix: str='') -> pd.DataFrame:
    if report.empty:
      return report.copy()
    def finish(result_report: pd.DataFrame):
      for c in [
        'selected_conversion_actions',
        'selected_conversions',
        'total_conversions',
      ]:
        if c not in result_report.columns:
          result_report[c] = None
      result_report['selected_conversions'].fillna(0, inplace=True)
      result_report['total_conversions'].fillna(0, inplace=True)
      return result_report

    conversion_action_entity_ids = list(report[f'{self.entity_table(entity_granularity)}#id'].unique())
    if not conversion_action_entity_ids:
      return finish(report)

    selected_conversion_action_df = self.get_selected_conversions_report(
      start_date=start_date,
      end_date=end_date,
      entity_granularity=entity_granularity,
      time_granularity=time_granularity,
      entity_ids=conversion_action_entity_ids
    )
    if selected_conversion_action_df.empty:
      return finish(report)
    if 'customer#time_zone' in report.columns:
      selected_conversion_action_df.drop(columns='customer#time_zone', inplace=True)
    merged_df = report.merge(
      selected_conversion_action_df,
      how='left',
      on=[
        f'{self.entity_table(entity_granularity)}#id',
        'segments#date',
        *(['segments#hour'] if time_granularity == 'hourly' else []),
      ],
      suffixes=('', suffix)
    )
    return finish(merged_df)

  @handle_ga_permission_error(default_value=pd.DataFrame())
  def get_selected_conversions_report(self, start_date: datetime, end_date: datetime, entity_granularity: str, time_granularity: str, entity_ids: Optional[List[str]]=None) -> pd.DataFrame:
    conversion_action_query = GoogleAdsQuery(
      query=(''
        f'SELECT customer.time_zone'
          f', {self.entity_table(entity_granularity)}.id'
          ', campaign.selective_optimization.conversion_actions'
          ', metrics.conversions'
          ', segments.conversion_action'
          ', segments.conversion_action_name'
          ', segments.date'
          f"{(', segments.hour ' if time_granularity == 'hourly' else ' ')}"
        f'FROM {self.entity_table(entity_granularity)} '
        'WHERE segments.date >= {start_date} '
          'AND segments.date <= {end_date} '
          f"{(f'AND {self.entity_table(entity_granularity)}.id IN {{entity_ids}}' if entity_ids is not None else '')} "
          'AND metrics.conversions != 0'
      ),
      parameters={
        'start_date': start_date,
        'end_date': end_date,
        'entity_ids': entity_ids
      }
    )
    conversion_action_df = self.get_query_data_frame(
      query=conversion_action_query,
      customer_id=self.api.customer_id,
      json_encode_repeated=False,
      value_extractor=lambda path, key, value, **kwargs: {key: 0} if ((path == ['segments'] and key == 'hour') or path == ['metrics']) and value.value == 0 else None
    )

    if conversion_action_df is None:
      return pd.DataFrame()
    if conversion_action_df.empty:
      return conversion_action_df

    conversion_action_df['selected_conversions'] = 0
    conversion_action_df['total_conversions'] = conversion_action_df['metrics#conversions']
    conversion_action_df['selected_conversion_actions'] = None
    if 'campaign#selective_optimization#conversion_actions' in conversion_action_df.columns:
      conversion_action_df['selected_conversion_actions'] = conversion_action_df['campaign#selective_optimization#conversion_actions']
      location = conversion_action_df.loc[conversion_action_df['campaign#selective_optimization#conversion_actions'].notna()]
      conversion_action_df.loc[location.index, 'selected_conversions'] = location.apply(lambda r: r ['metrics#conversions'] if r['segments#conversion_action'] in r['campaign#selective_optimization#conversion_actions'] else 0, axis='columns')

    group_by = [
      'customer#time_zone',
      f'{self.entity_table(entity_granularity)}#id',
      'segments#date',
      *(['segments#hour'] if time_granularity == 'hourly' else []),
    ]
    columns = [
      *group_by,
      'selected_conversion_actions',
      'selected_conversions',
      'total_conversions',
    ]
    aggregated_df = conversion_action_df[columns].groupby(group_by).agg({
      'selected_conversion_actions': 'max',
      'selected_conversions': 'sum',
      'total_conversions': 'sum',
    }).reset_index()

    return aggregated_df

  @handle_ga_permission_error(default_value=pd.DataFrame())
  def get_change_report(self, entity_granularity: str, entity_ids: List[str], start_date: Optional[datetime]=None, end_date: Optional[datetime]=None) -> pd.DataFrame:
    def select_columns(entity_granularity: str):
      if entity_granularity == 'ad':
        return (
          ', campaign.id'
          ', campaign.name'
          ', ad_group.id'
          ', ad_group.name'
          ', ad_group_ad.ad.id'
          ', ad_group_ad.ad.name'
        )
      elif entity_granularity == 'ad_group':
        return (
          ', campaign.id'
          ', campaign.name'
          ', ad_group.id'
          ', ad_group.name'
          ', change_status.ad_group_criterion'
        )
      elif entity_granularity == 'campaign':
        return (
          ', campaign.id'
          ', campaign.name'
          ', change_status.campaign_criterion'
        )

    query = GoogleAdsQuery(
      query=(''
        'SELECT customer.id'
          ', customer.descriptive_name'
          ', customer.currency_code'
          ', customer.time_zone'
          f'{select_columns(entity_granularity)}'
          ', change_status.last_change_date_time'
          ', change_status.resource_status'
          ', change_status.resource_type '
        f'FROM change_status '
        f'WHERE {entity_granularity}.id IN {{entity_ids}} '
        f"{'AND change_status.last_change_date_time >= {start_date} ' if start_date is not None else ''}"
        f"{'AND change_status.last_change_date_time <= {end_date}' if end_date is not None else ''}"
      ),
      parameters={
        'start_date': GoogleAdsQuery.format_parameter(start_date, format_time=True).strip("'"),
        'end_date':  GoogleAdsQuery.format_parameter(end_date, format_time=True).strip("'"),
        'entity_ids': entity_ids,
      }
    )

    df = self.get_query_data_frame(
      query=query,
      customer_id=self.api.customer_id,
    )
    if df is None:
      return pd.DataFrame()

    merged_df = df
    for criterion_table in ['campaign_criterion', 'ad_group_criterion']:
      criterion_column = f'change_status#{criterion_table}'
      if criterion_column not in merged_df.columns:
        continue
      criterion_query = GoogleAdsQuery(
        query=(''
          f'SELECT {criterion_table}.resource_name'
            f', {criterion_table}.type'
            f', {criterion_table}.status '
          f'FROM {criterion_table} '
          f'WHERE {criterion_table}.resource_name IN {{criteria}}'
        ),
        parameters={
          'criteria': sorted(filter(lambda v: not pd.isna(v), df[criterion_column].unique()))
        }
      )
      criterion_df = self.get_query_data_frame(
        query=criterion_query,
        customer_id=self.api.customer_id,
        exclude_keys=[]
      )
      if criterion_df is None or criterion_df.empty:
        continue

      # TODO: Figure out why the conversion_action_query does not retrieve most of the conversion action resources by which the ad query is segmented.
      merged_df = merged_df.merge(
        left_on=criterion_column, 
        right_on=f'{criterion_table}#resource_name',
        right=criterion_df,
        how='left'
      )
    return merged_df
    
  @handle_ga_permission_error(default_value=pd.DataFrame())
  def get_safety_report(self, entity_granularity: str, entity_ids: List[str], start_date: Optional[datetime]=None, end_date: Optional[datetime]=None) -> pd.DataFrame:
    def select_columns(entity_granularity: str):
      if entity_granularity == 'ad':
        return (
          ', campaign.id'
          ', campaign.name'
          ', ad_group.id'
          ', ad_group.name'
          ', ad_group_ad.ad.id'
          ', ad_group_ad.ad.name'
        )
      elif entity_granularity == 'ad_group':
        return (
          ', campaign.id'
          ', campaign.name'
          ', ad_group.id'
          ', ad_group.name'
        )
      elif entity_granularity == 'campaign':
        return (
          ', campaign.id'
          ', campaign.name'
          # ', campaign.labels'
          ', campaign.start_date'
          ', campaign.status'
          ', campaign.bidding_strategy_type'
          ', campaign.target_cpa.target_cpa_micros'
          ', campaign_budget.type'
          ', campaign_budget.status'
          ', campaign_budget.amount_micros'
          ', campaign_budget.period'
          ', metrics.conversions'
        )
    
    def select_table(entity_granularity: str):
      if entity_granularity == 'ad':
        return 'ad_group_ad'
      elif entity_granularity == 'ad_group':
        return 'ad_group'
      elif entity_granularity == 'campaign':
        return 'campaign'

    query = GoogleAdsQuery(
      query=(''
        'SELECT customer.id'
          ', customer.descriptive_name'
          ', customer.currency_code'
          ', customer.time_zone'
          f'{select_columns(entity_granularity)} '
        f'FROM {select_table(entity_granularity)} '
        f'WHERE {entity_granularity}.id IN {{entity_ids}} '
        f"{'AND segments.date >= {start_date} ' if start_date is not None else ''}"
        f"{'AND segments.date <= {end_date}' if end_date is not None else ''}"
      ),
      parameters={
        'start_date': start_date,
        'end_date': end_date,
        **({'entity_ids': entity_ids} if entity_ids is not None else {}),
      }
    )

    df = self.get_query_data_frame(
      query=query,
      customer_id=self.api.customer_id,
      value_extractor=lambda path, key, value, **kwargs: {key: 0} if path == ['metrics'] and key == 'conversions' and value.value == 0 else None
    )
    return df if df is not None else pd.DataFrame()

  @handle_ga_permission_error(default_value=pd.DataFrame())
  def get_entity_report(self, granularity: str, ids: Optional[List[str]]=None, columns: Optional[List[str]]=None) -> pd.DataFrame:
    if columns is None:
      columns = []
    entity = GoogleAdsEntity(granularity)
    id_columns = [f'{e.value}.id' for e in (*reversed(entity.higher), entity)]
    all_columns = [
      *[
        c 
        for c in id_columns
        if c not in columns
      ],
      *columns,
    ]

    query = GoogleAdsQuery(
      query=(''
        f'SELECT {", ".join(all_columns)} '
        f'FROM {entity.value} '
        f"{f'WHERE {entity}.id IN {{ids}} ' if ids is not None else ''}"
      ),
      parameters={
        **({'ids': ids} if ids is not None else {}),
      }
    )

    df = self.get_query_data_frame(
      query=query,
      customer_id=self.api.customer_id,
    )
    df.rename(columns=lambda c: c.replace('#', '.'), inplace=True)
    return df if df is not None else pd.DataFrame()

class GoogleAdWordsReporter:
  api: GoogleAdWordsAPI
  verbose: bool

  def __init__(self, api: GoogleAdWordsAPI, verbose: bool=False):
    self.api = api
    self.verbose = verbose

  @property
  def report_downloader(self):
    return self.api.client.GetReportDownloader(version='v201809')

  def get_creative_conversion_report(self, start_date, end_date, columns) -> pd.DataFrame:
    report_query = (
      adwords.ReportQueryBuilder()
      .Select(*columns)
      .From('CREATIVE_CONVERSION_REPORT')
      .During(start_date=start_date, end_date=end_date)
      .Build()
    )

    stream_data = self.report_downloader.DownloadReportAsStreamWithAwql(
      report_query,
      'CSV',
      skip_report_header=True,
      skip_report_summary=True
    )

    df = pd.read_csv(stream_data)
    if self.verbose:
      print(df)

    return df

  def get_ad_performance_report(self, start_date, end_date, columns) -> pd.DataFrame:
    report_query = (
      adwords.ReportQueryBuilder()
      .Select(*columns)
      .From('AD_PERFORMANCE_REPORT')
      .During(start_date=start_date, end_date=end_date)
      .Build()
    )

    stream_data = self.report_downloader.DownloadReportAsStreamWithAwql(
      report_query,
      'CSV',
      skip_report_header=True,
      skip_report_summary=True
    )

    df = pd.read_csv(stream_data)
    if self.verbose:
      print(df)

    return df

  def get_criteria_report(self, start_date, end_date, columns) -> pd.DataFrame:
    report_query = (
      adwords.ReportQueryBuilder()
      .Select(*columns)
      .From('CRITERIA_PERFORMANCE_REPORT')
      .Where('Status').In('ENABLED', 'PAUSED')
      .During(start_date=start_date, end_date=end_date)
      .Build()
    )

    stream_data = self.report_downloader.DownloadReportAsStreamWithAwql(
      report_query,
      'CSV',
      skip_report_header=True,
      skip_report_summary=True
    )

    df = pd.read_csv(stream_data)
    if self.verbose:
      print(df)

    return df

  def get_campaign_report(self, start_date: datetime, end_date: datetime, columns: List[str]) -> pd.DataFrame:
    report_query = (
      adwords.ReportQueryBuilder()
      .Select(*columns)
      .From('CAMPAIGN_PERFORMANCE_REPORT')
      .During(start_date=start_date, end_date=end_date)
      .Build()
    )

    stream_data = self.report_downloader.DownloadReportAsStreamWithAwql(
      report_query,
      'CSV',
      skip_report_header=True,
      skip_report_summary=True
    )

    df = pd.read_csv(stream_data)
    if self.verbose:
      print(df)

    return df