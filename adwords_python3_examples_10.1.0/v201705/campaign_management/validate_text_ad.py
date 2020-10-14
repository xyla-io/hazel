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

"""This example shows how to use validateOnly SOAP header.

No objects will be created, but exceptions will still be returned.

The LoadFromStorage method is pulling credentials and properties from a
"googleads.yaml" file. By default, it looks for this file in your home
directory. For more information, see the "Caching authentication information"
section of our README.

"""

import suds

from googleads import adwords


AD_GROUP_ID = 'INSERT_AD_GROUP_ID_HERE'


def main(client, ad_group_id):
  # Initialize appropriate service with validate only flag enabled.
  client.validate_only = True
  ad_group_ad_service = client.GetService('AdGroupAdService', version='v201705')

  # Construct operations to add an expanded text ad.
  operations = [{
      'operator': 'ADD',
      'operand': {
          'xsi_type': 'AdGroupAd',
          'adGroupId': ad_group_id,
          'ad': {
              'xsi_type': 'ExpandedTextAd',
              'headlinePart1': 'Luxury Cruise to Mars',
              'headlinePart2': 'Visit the Red Planet in style.',
              'description': 'Low-gravity fun for everyone!',
              'finalUrls': ['http://www.example.com']
          }
      }
  }]
  ad_group_ad_service.mutate(operations)
  # No error means the request is valid.

  # Now let's check an invalid ad using a very long line to trigger an error.
  operations = [{
      'operator': 'ADD',
      'operand': {
          'xsi_type': 'AdGroupAd',
          'adGroupId': ad_group_id,
          'ad': {
              'xsi_type': 'ExpandedTextAd',
              'headlinePart1': 'Luxury Cruise to Mars',
              'headlinePart2': 'Visit the Red Planet in style.',
              'description': 'Low-gravity fun for all astronauts in orbit',
              'finalUrls': ['http://www.example.com'],
          }
      }
  }]
  try:
    ad_group_ad_service.mutate(operations)
  except suds.WebFault as e:
    print('Validation correctly failed with "%s".' % str(e))


if __name__ == '__main__':
  # Initialize client object.
  adwords_client = adwords.AdWordsClient.LoadFromStorage()

  main(adwords_client, AD_GROUP_ID)
