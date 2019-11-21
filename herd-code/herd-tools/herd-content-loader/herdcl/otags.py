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
import os, sys, configparser, base64, traceback

# Third party imports
import pandas as pd

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
    file_name = ''

    def __init__(self):
        # TODO attach methods
        self.action = None
        self.acts = {
            'tags': self.get_build_info,
            'objects': self.load_object,
            'columns': self.get_build_info,
            'samples': self.get_build_info,
            'export bdef': self.get_build_info,
        }
        self.actions = ['Objects', 'Columns', 'Samples', 'Tags', 'Export BDef']
        self.envs = ['DEV-INT', 'QA-INT', 'CT', 'PROD', 'PROD-CT']
        self.excel_file = ''

        self.path = ''
        self.config = None

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
            message = "Config file loader.cfg not found"
            LOGGER.error(message)
            raise FileNotFoundError(message)
        config = configparser.ConfigParser()
        config.read(config_file)

        self.path = path
        self.config = config

    ############################################################################
    def setup_run(self, config):
        """
        Setup run variables

        """
        if config['gui_enabled']:
            self.action = str.lower(config['action'])
            self.excel_file = config['excel_file']
            self.configuration.host = self.config.get('url', config['env'])
            self.configuration.username = config['userName']
            self.configuration.password = config['userPwd']
        else:
            self.action = str.lower(self.config.get('console', 'action'))
            if self.action == 'objects':
                self.excel_file = self.config.get('console', 'excelFile')
            env = self.config.get('console', 'env')
            self.configuration.host = self.config.get('url', env)
            self.configuration.username = self.config.get('credentials', 'userName')
            self.configuration.password = base64.b64decode(self.config.get('credentials', 'userPwd')).decode('utf-8')

    ############################################################################
    def get_action(self):
        """
        Gets a particular method

        :param: key: reference to a function
        :return: the function

        """
        method = self.acts[self.action]
        LOGGER.info("Running {}".format(method.__name__))
        return method

    ############################################################################
    def load_object(self):
        """
        One of the controller actions. Loads business object definitions

        :return:

        """
        excel = self.load_worksheet('Bus Obj Definition')

        for index, row in excel.iterrows():
            try:
                # Update business object definition descriptive information
                # self.update_bdef_descriptive_info(row)
                self.update_sme(row)
            except ApiException as e:
                LOGGER.error(e)
            except:
                LOGGER.error(traceback.format_exc())

        # json_data = json.dumps(team.__dict__, lambda o: o.__dict__, indent=4)

        # tag_keys = self.get_tag_types().tag_type_keys

        # Get list of all tag types
        tag_types = {}

        # # Get details of each tag type
        # for tag in tag_keys:
        #     code = tag.tag_type_code
        #     tag_types[code] = self.get_tag_type_code(code).display_name

        return tag_types

    ############################################################################
    def load_worksheet(self, sheet):
        """
        Loads Excel worksheet to Pandas DataFrame

        :param: sheet: Excel sheet name
        :return: Pandas DataFrame

        """
        return pd.read_excel(self.excel_file, sheet_name=sheet).fillna('')

    ############################################################################
    def update_bdef_descriptive_info(self, row):
        """
        Updates an existing business object definition description

        :param: row: A row inside the Pandas DataFrame

        """
        namespace, usage, file_type, bdef_name, logical_name, description = row[:6]
        LOGGER.info('Getting BDef')
        resp = self.get_business_object_definition(namespace, bdef_name)
        LOGGER.info('Success')
        LOGGER.info(resp)

        if (resp.description != description or
                    resp.display_name != logical_name or
                    resp.descriptive_business_object_format.business_object_format_usage != usage or
                    resp.descriptive_business_object_format.business_object_format_file_type != file_type):
            json = {
                "description": description,
                "displayName": logical_name,
                "formatUsage": usage,
                "fileType": file_type
            }
            LOGGER.info('Updating BDef Descriptive Info')
            resp = self.update_business_object_definition_descriptive_info(namespace=namespace,
                                                                           business_object_definition_name=bdef_name,
                                                                           update_request=json)
            LOGGER.info('Success')
            LOGGER.info(resp)

    ############################################################################
    def update_sme(self, row):
        """
        Updates existing business object definition subject matter experts for a specific business object definition

        :param: row: A row inside the Pandas DataFrame

        """
        namespace, _, _, bdef_name = row[:4]

        LOGGER.info('Getting SME')
        resp = self.get_subject_matter_experts(namespace, bdef_name)
        LOGGER.info('Success')
        LOGGER.info(resp)

        user = row['Bus Obj Def SME User ID']
        if user:
            user = set([u.strip(" ,\t") for u in user.strip().split(',')])

        current_smes = []
        remove_sme_list = []
        for entry in resp.business_object_definition_subject_matter_expert_keys:
            user_id = entry.user_id
            if '@' in user_id:
                user_id = user_id.split('@')[0]
            current_smes.append(user_id)
            if user and user_id in user:
                user.remove(user_id)
            else:
                remove_sme_list.append(user_id)

        LOGGER.info('Current expert list: {}'.format(', '.join(current_smes)))

        for sme in remove_sme_list:
            user_id = '{}{}{}rp.{}.{}sd.{}'.format(sme, chr(64), 'co', 'root', 'na', 'com')
            LOGGER.info('Deleting SMEs')
            self.delete_subject_matter_expert(namespace, bdef_name, user_id)
            LOGGER.info('SME {} deleted'.format(sme))

        if user:
            for user_id in user:
                if not '@' in user_id:
                    user_id = '{}{}{}rp.{}.{}sd.{}'.format(user_id, chr(64), 'co', 'root', 'na', 'com')
                LOGGER.info('Adding SMEs')
                self.create_subject_matter_expert(namespace, bdef_name, user_id)
                LOGGER.info('SME {} Added'.format(user_id))


    ############################################################################
    def get_build_info(self):
        """
        Gets the build information for the Data Management deployed code.

        :return: response from herdsdk call

        """
        # create an instance of the API class
        api_instance = herdsdk.ApplicationApi(herdsdk.ApiClient(self.configuration))

        # Gets the build information
        api_response = api_instance.application_get_build_info()
        return api_response

    ############################################################################
    def get_tag_types(self):
        api_instance = herdsdk.TagTypeApi(herdsdk.ApiClient(self.configuration))

        LOGGER.info('GET /tagTypes')
        api_response = api_instance.tag_type_get_tag_types()
        return api_response

    ############################################################################
    def get_tag_type_code(self, tag):
        api_instance = herdsdk.TagTypeApi(herdsdk.ApiClient(self.configuration))

        LOGGER.info('GET /tagTypes/{}'.format(tag))
        api_response = api_instance.tag_type_get_tag_type(tag)
        return api_response

    ############################################################################
    def get_business_object_definition(self, namespace, business_object_definition_name):
        api_instance = herdsdk.BusinessObjectDefinitionApi(herdsdk.ApiClient(self.configuration))

        LOGGER.info(
            'GET /businessObjectDefinitions/namespaces/{}/businessObjectDefinitionNames/{}'.format(
                namespace,
                business_object_definition_name))
        api_response = api_instance.business_object_definition_get_business_object_definition(namespace,
                                                                                              business_object_definition_name)
        return api_response

    ############################################################################
    def update_business_object_definition_descriptive_info(self, namespace, business_object_definition_name,
                                                           update_request):
        api_instance = herdsdk.BusinessObjectDefinitionApi(herdsdk.ApiClient(self.configuration))

        descriptive_business_object_format = herdsdk.DescriptiveBusinessObjectFormatUpdateRequest(
            business_object_format_usage=update_request['formatUsage'],
            business_object_format_file_type=update_request['fileType'])

        business_object_definition_descriptive_information_update_request = herdsdk.BusinessObjectDefinitionDescriptiveInformationUpdateRequest(
            description=update_request['description'],
            display_name=update_request['displayName'],
            descriptive_business_object_format=descriptive_business_object_format)

        LOGGER.info(
            'PUT /businessObjectDefinitionDescriptiveInformation/namespaces/{}/businessObjectDefinitionNames/{}'.format(
                namespace,
                business_object_definition_name))
        api_response = api_instance.business_object_definition_update_business_object_definition_descriptive_information(
            namespace, business_object_definition_name,
            business_object_definition_descriptive_information_update_request)
        return api_response

    ############################################################################
    def get_subject_matter_experts(self, namespace, business_object_definition_name):
        api_instance = herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi(herdsdk.ApiClient(self.configuration))

        LOGGER.info(
            'GET /businessObjectDefinitionSubjectMatterExperts/namespaces/{}/businessObjectDefinitionNames/{}'.format(
                namespace,
                business_object_definition_name))
        api_response = api_instance. \
            business_object_definition_subject_matter_expert_get_business_object_definition_subject_matter_experts_by_business_object_definition(
            namespace, business_object_definition_name)
        return api_response

    ############################################################################
    def delete_subject_matter_expert(self, namespace, business_object_definition_name, user_id):
        api_instance = herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi(herdsdk.ApiClient(self.configuration))

        LOGGER.info(
            'DELETE /businessObjectDefinitionSubjectMatterExperts/namespaces/{}/businessObjectDefinitionNames/{}/userIds/{}'.format(
                namespace,
                business_object_definition_name,
                user_id))
        api_response = api_instance.business_object_definition_subject_matter_expert_delete_business_object_definition_subject_matter_expert(
            namespace, business_object_definition_name, user_id)
        return api_response

    ############################################################################
    def create_subject_matter_expert(self, namespace, business_object_definition_name, user_id):
        api_instance = herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi(herdsdk.ApiClient(self.configuration))

        business_object_definition_subject_matter_expert_key = herdsdk.BusinessObjectDefinitionSubjectMatterExpertKey(
            namespace=namespace,
            business_object_definition_name=business_object_definition_name,
            user_id=user_id)
        business_object_definition_subject_matter_expert_create_request = herdsdk.BusinessObjectDefinitionSubjectMatterExpertCreateRequest(
            business_object_definition_subject_matter_expert_key=business_object_definition_subject_matter_expert_key
        )

        LOGGER.info(
            'POST /businessObjectDefinitionSubjectMatterExperts'.format(
                namespace,
                business_object_definition_name,
                user_id))
        api_response = api_instance.business_object_definition_subject_matter_expert_create_business_object_definition_subject_matter_expert(
            business_object_definition_subject_matter_expert_create_request)
        return api_response
