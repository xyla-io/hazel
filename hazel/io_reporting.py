import pandas as pd

from typing import Dict, Optional
from io_channel import IOChannelSourceReporter, IOEntityGranularity, IOEntityAttribute, IOChannelGranularity, IOChannelProperty
from .api import GoogleAdsAPI
from .reporting import GoogleAdsReporter

class IOGoogleAdsReporter(IOChannelSourceReporter):
  @classmethod
  def _get_map_identifier(cls) -> str:
    return f'hazel/{cls.__name__}'

  def io_entity_granularity_to_api(self, granularity: IOEntityGranularity) -> Optional[str]:
    if granularity is IOEntityGranularity.account:
      return 'customer'
    elif granularity is IOEntityGranularity.adgroup:
      return 'ad_group'
    elif granularity is IOEntityGranularity.ad:
      return 'ad_group_ad'
    else:
      return super().io_entity_granularity_to_api(granularity)

  def io_entity_attribute_to_api(self, attribute: IOEntityAttribute, granularity: IOEntityGranularity=None) -> Optional[str]:
    if granularity:
      if granularity is IOEntityGranularity.account:
        if attribute is IOEntityAttribute.name:
          return f'{self.io_entity_granularity_to_api(granularity)}.descriptive_name'
      return f'{self.io_entity_granularity_to_api(granularity)}.{self.io_entity_attribute_to_api(attribute)}'
    return super().io_entity_attribute_to_api(
      attribute=attribute,
      granularity=granularity
    )

  def api_column_to_io(self, api_report: pd.DataFrame, api_column: str, granularity: IOChannelGranularity, property: IOChannelProperty) -> Optional[any]:
    if api_column not in api_report:
      return None
    if property is IOEntityAttribute.id:
      return api_report[api_column].apply(lambda i: str(int(i)) if pd.notna(i) else None)
    return super().api_column_to_io(
      api_report=api_report,
      api_column=api_column,
      granularity=granularity,
      property=property
    )

  def fetch_entity_report(self, granularity: IOEntityGranularity, reporter: GoogleAdsReporter):
    api_entity_granularity = self.io_entity_granularity_to_api(granularity)
    api_entity_columns = self.filtered_api_entity_attributes(granularity)
    report = reporter.get_entity_report(
      granularity=api_entity_granularity,
      columns=api_entity_columns
    )
    return report

  def run(self, credentials: Dict[str, any]) -> Dict[str, any]:
    api = GoogleAdsAPI(**credentials)
    customer_ids = api.get_customer_ids()
    reporter = GoogleAdsReporter(api=api)

    report = pd.DataFrame()
    for customer_id in customer_ids:
      api.customer_id = customer_id
      for granularity in reversed(sorted(self.filtered_io_entity_granularities)):
        # TODO: support metrics and time granularity as well
        api_report = self.fetch_entity_report(
          granularity=granularity,
          reporter=reporter
        )
        io_report = self.api_report_to_io(
          api_report=api_report,
          granularities=[granularity]
        )
        self.fill_api_ancestor_identifiers_in_io(
          api_report=api_report,
          io_report=io_report,
          granularities=[granularity]
        )
        report = report.append(io_report)

    report = self.finalized_io_report(report)
    return report