import google
import uuid

from .api import GoogleAdsAPI
from .base import print_ga_error
from google.api_core import protobuf_helpers
from typing import Optional

class GoogleAdsMutator:
  api: GoogleAdsAPI

  def __init__(self, api: GoogleAdsAPI):
    self.api = api

  def mutate(self) -> any:
    raise NotImplementedError()

  def prepare_mutation(self, mutation: any):
    raise NotImplementedError()

class GoogleAdsCampaignMutator(GoogleAdsMutator):
  campaign_id: str

  def __init__(self, api: GoogleAdsAPI, campaign_id: str):
    self.campaign_id = campaign_id
    super().__init__(api=api)

  @print_ga_error()
  def mutate(self) -> any:
    service = self.api.client.get_service('CampaignService', version=self.api.api_version)
    operation = self.api.client.get_type('CampaignOperation', version=self.api.api_version)

    campaign = operation.update
    campaign.resource_name = service.campaign_path(self.api.customer_id, self.campaign_id)
    self.prepare_mutation(mutation=campaign)

    field_mask = protobuf_helpers.field_mask(None, campaign)
    operation.update_mask.CopyFrom(field_mask)

    response = service.mutate_campaigns(self.api.customer_id, [operation])
    return response

class GoogleAdsCampaignPauseMutator(GoogleAdsCampaignMutator):
  def prepare_mutation(self, mutation: any):
    mutation.status = self.api.client.get_type('CampaignStatusEnum', version=self.api.api_version).PAUSED

class GoogleAdsCampaignTargetCPAMutator(GoogleAdsCampaignMutator):
  target_cpa_micros: int

  def __init__(self, api: GoogleAdsAPI, campaign_id: str, target_cpa_micros: int):
    self.target_cpa_micros = target_cpa_micros
    super().__init__(api=api, campaign_id=campaign_id)

  def prepare_mutation(self, mutation: any):
    mutation.target_cpa.target_cpa_micros.value = self.target_cpa_micros

class GoogleAdsCampaignBudgetMutator(GoogleAdsCampaignMutator):
  budget_micros: float
  budget_name: Optional[str]

  def __init__(self, api: GoogleAdsAPI, campaign_id: str, budget_micros: float, budget_name: Optional[str]=None):
    self.budget_micros = budget_micros
    self.budget_name = budget_name
    super().__init__(api=api, campaign_id=campaign_id)

  def prepare_mutation(self, mutation: any):
    campaign_budget_operation = self.api.client.get_type(
      'CampaignBudgetOperation',
      version=self.api.api_version
    )
    campaign_budget = campaign_budget_operation.create
    # Notes on the name:
    # ------------------
    # The name of the campaign budget.
    # When creating a campaign budget through CampaignBudgetService, every explicitly shared campaign budget must have a non-null, non-empty name. Campaign budgets that are not explicitly shared derive their name from the attached campaign's name.
    # The length of this string must be between 1 and 255, inclusive, in UTF-8 bytes, (trimmed).
    if self.budget_name is not None:
      campaign_budget.name.value = self.budget_name

    # BudgetDeliveryMethodEnum notes:
    # -------------------------------
    # * STANDARD: The budget server will throttle serving evenly across the entire time period.
    # * ACCELERATED: The budget server will not throttle serving, and ads will serve as fast as possible.
    # source: https://developers.google.com/google-ads/api/reference/rpc/google.ads.googleads.v3.enums#google.ads.googleads.v3.enums.BudgetDeliveryMethodEnum.BudgetDeliveryMethod
    campaign_budget.delivery_method = self.api.client.get_type('BudgetDeliveryMethodEnum').STANDARD

    # Notes on explicity_shared:
    # --------------------------
    # Specifies whether the budget is explicitly shared. Defaults to true if unspecified in a create operation.
    # If true, the budget was created with the purpose of sharing across one or more campaigns.
    # If false, the budget was created with the intention of only being used with a single campaign. The budget's name and status will stay in sync with the campaign's name and status. Attempting to share the budget with a second campaign will result in an error.
    # A non-shared budget can become an explicitly shared. The same operation must also assign the budget a name.
    # A shared campaign budget can never become non-shared.
    # source: https://developers.google.com/google-ads/api/reference/rpc/google.ads.googleads.v3.resources#google.ads.googleads.v3.resources.CampaignBudget
    campaign_budget.explicitly_shared.value = False

    # Notes an amount assignment:
    # ------------------------
    # * total_amount_micros: The lifetime amount of the budget, in the local currency for the account. Amount is specified in micros, where one million is equivalent to one currency unit.
    # * amount_micros: The amount of the budget, in the local currency for the account. Amount is specified in micros, where one million is equivalent to one currency unit. Monthly spend is capped at 30.4 times this amount.
    # source: https://developers.google.com/google-ads/api/reference/rpc/google.ads.googleads.v3.resources#google.ads.googleads.v3.resources.CampaignBudget
    campaign_budget.amount_micros.value = self.budget_micros

    service = self.api.client.get_service('CampaignBudgetService', version=self.api.api_version)
    campaign_budget_response = (
      service.mutate_campaign_budgets(self.api.customer_id, [campaign_budget_operation])
    )
    budget_resource_name = campaign_budget_response.results[0].resource_name
    mutation.campaign_budget.value = (budget_resource_name)