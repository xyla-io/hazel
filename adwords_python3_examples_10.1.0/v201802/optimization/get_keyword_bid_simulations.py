#!/usr/bin/env python
#
# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This example gets a bid landscape for an ad group and a criterion.

To get ad groups, run get_ad_groups.py. To get criteria, run get_keywords.py.

The LoadFromStorage method is pulling credentials and properties from a
"googleads.yaml" file. By default, it looks for this file in your home
directory. For more information, see the "Caching authentication information"
section of our README.

"""

from googleads import adwords


AD_GROUP_ID = 'INSERT_AD_GROUP_ID_HERE'
CRITERION_ID = 'INSERT_CRITERION_ID_HERE'
PAGE_SIZE = 100


def main(client, ad_group_id, criterion_id):
  # Initialize appropriate service.
  data_service = client.GetService('DataService', version='v201802')

  # Construct bid landscape selector object and retrieve bid landscape.
  selector = {
      'fields': ['AdGroupId', 'CriterionId', 'StartDate', 'EndDate', 'Bid',
                 'BiddableConversions', 'BiddableConversionsValue',
                 'LocalClicks', 'LocalCost', 'LocalImpressions'],
      'predicates': [
          {
              'field': 'AdGroupId',
              'operator': 'EQUALS',
              'values': [ad_group_id]
          },
          {
              'field': 'CriterionId',
              'operator': 'EQUALS',
              'values': [criterion_id]
          }
      ],
      'paging': {
          'startIndex': 0,
          'numberResults': PAGE_SIZE
      }
  }

  # Set initial values.
  offset = 0
  more_pages = True

  while more_pages:
    num_landscape_points = 0
    page = data_service.getCriterionBidLandscape(selector)

    if page and 'entries' in page:
      entries = page['entries']
      print('Bid landscape(s) retrieved: %d.' % len(entries))

      for bid_landscape in entries:
        print(('Retrieved keyword bid landscape with ad group ID "%d", '
               'keyword ID "%d", start date "%s", end date "%s", '
               'with landscape points:' % (
                   bid_landscape['adGroupId'], bid_landscape['criterionId'],
                   bid_landscape['startDate'], bid_landscape['endDate'])))
        for bid_landscape_point in bid_landscape['landscapePoints']:
          num_landscape_points += 1
          print(('  bid: %s => clicks: %s, cost: %s, impressions: %s, '
                 'biddable conversions: %.2f, '
                 'biddable conversions value: %.2f'
                 % (bid_landscape_point['bid']['microAmount'],
                    bid_landscape_point['clicks'],
                    bid_landscape_point['cost']['microAmount'],
                    bid_landscape_point['impressions'],
                    bid_landscape_point['biddableConversions'],
                    bid_landscape_point['biddableConversionsValue'])))

    # Need to increment by the total # of landscape points within the page,
    # NOT the number of entries (bid landscapes) in the page.
    offset += num_landscape_points
    selector['paging']['startIndex'] = str(offset)
    more_pages = num_landscape_points >= PAGE_SIZE


if __name__ == '__main__':
  # Initialize client object.
  adwords_client = adwords.AdWordsClient.LoadFromStorage()

  main(adwords_client, AD_GROUP_ID, CRITERION_ID)
