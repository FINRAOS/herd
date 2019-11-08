"""
  Copyright 2015 herd contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
"""
# Herd imports
import herdsdk
from herdsdk.rest import ApiException

# Local imports
from herdcl import logger

LOGGER = logger.get_logger(__name__)

# Configure HTTP basic authorization: basicAuthentication
configuration = herdsdk.Configuration()


def setup_config(creds):
    configuration.host = creds['url']
    configuration.username = creds['userName']
    configuration.password = creds['userPwd']


def get_build_info():
    # create an instance of the API class
    api_instance = herdsdk.ApplicationApi(herdsdk.ApiClient(configuration))

    try:
        # Gets the build information
        api_response = api_instance.application_get_build_info()
        return api_response
    except ApiException as e:
        print("Exception when calling ApplicationApi->application_get_build_info: %s\n" % e)


def test_method():
    # create an instance of the API class
    api_instance = herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi(herdsdk.ApiClient(configuration))
    namespace = 'eFOCUS'  # str | the namespace code
    business_object_definition_name = 'custody_flng_dtl_SEC'  # str | the business object definition name

    try:
        api_response = api_instance. \
            business_object_definition_subject_matter_expert_get_business_object_definition_subject_matter_experts_by_business_object_definition(
            namespace, business_object_definition_name)
        return api_response.business_object_definition_subject_matter_expert_keys
    except ApiException as e:
        print(
            "Exception when calling BusinessObjectDefinitionSubjectMatterExpertApi->business_object_definition_subject_matter_expert_get_business_object_definition_subject_matter_experts_by_business_object_definition: %s\n" % e)
