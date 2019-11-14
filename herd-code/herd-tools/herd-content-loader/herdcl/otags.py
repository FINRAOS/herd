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
# Standard library imports
import os, sys, configparser, json

# Herd imports
import herdsdk
from herdsdk.rest import ApiException

# Local imports
from herdcl import logger

LOGGER = logger.get_logger(__name__)


################################################################################
class Controller:
    """
     The controller class. Makes calls to herdsdk
    """
    def __init__(self):
        # TODO attach methods
        self.acts = {
            'tags': self.get_build_info,
            'objects': self.get_build_info,
            'columns': self.get_build_info,
            'samples': self.get_build_info,
            'bdef description': self.get_build_info,
        }
        self.actions = ['Tags', 'Objects', 'Columns', 'Samples', 'BDef Description']
        self.envs = ['DEV', 'DEV-INT', 'QA', 'QA-INT', 'QA-STRESS', 'CT', 'PROD', 'PROD-CT']

        # Configure HTTP basic authorization: basicAuthentication
        self.configuration = herdsdk.Configuration()

    ############################################################################
    def load_config(self):
        """
        Load configuration file

        :return: current working directory and configparser
        """
        LOGGER.debug(sys.argv)
        if os.name == 'nt':
            path = os.getcwd()
        elif '/' in sys.argv[0]:
            path = '/'.join(sys.argv[0].split('/')[:-1])
        else:
            path = os.getcwd()
        LOGGER.info('Current working directory: {}'.format(path))

        # Get config file
        config_file = path + "/loader.cfg"
        LOGGER.debug('Checking for loader config: {}'.format(config_file))
        if not os.path.exists(config_file):
            message = "No config file found"
            LOGGER.error(message)
            raise Exception(message)
        config = configparser.ConfigParser()
        config.read(config_file)

        return path, config

    ############################################################################
    def setup_config(self, creds):
        """
        Setup url and credentials

        """
        self.configuration.host = creds['url']
        self.configuration.username = creds['userName']
        self.configuration.password = creds['userPwd']

    ############################################################################
    def run_action(self, key):
        """
        Runs a particular method. Returns result

        :return: response from herdsdk call

        """
        return self.acts[key]()

    ############################################################################
    def get_build_info(self):
        """
        Gets the build information for the Data Management deployed code.

        :return: response from herdsdk call

        """
        # create an instance of the API class
        api_instance = herdsdk.ApplicationApi(herdsdk.ApiClient(self.configuration))

        try:
            # Gets the build information
            api_instance.application_get_build_info()
            return self.get_response(api_instance)
        except ApiException as e:
            LOGGER.error("Exception when calling ApplicationApi->application_get_build_info: %s\n" % e)

    ############################################################################
    def test_method(self):
        # create an instance of the API class
        api_instance = herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi(herdsdk.ApiClient(self.configuration))
        namespace = 'eFOCUS'  # str | the namespace code
        business_object_definition_name = 'custody_flng_dtl_SEC'  # str | the business object definition name

        try:
            api_response = api_instance. \
                business_object_definition_subject_matter_expert_get_business_object_definition_subject_matter_experts_by_business_object_definition(
                namespace, business_object_definition_name)
            return api_response.business_object_definition_subject_matter_expert_keys
        except ApiException as e:
            LOGGER.error(
                "Exception when calling BusinessObjectDefinitionSubjectMatterExpertApi->business_object_definition_subject_matter_expert_get_business_object_definition_subject_matter_experts_by_business_object_definition: %s\n" % e)

    ############################################################################
    def get_response(self, instance):
        response = {
            'status': instance.api_client.last_response.status,
            'data': json.loads(instance.api_client.last_response.data)
        }
        return response
