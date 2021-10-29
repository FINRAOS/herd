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
import os, sys, configparser, base64, traceback, pprint, filecmp, json
from datetime import datetime

# Third party imports
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

# Herd imports
import herdsdk
from herdsdk.rest import ApiException

# Local imports
try:
    import logger
    from constants import *
    from aws import AwsClient
except ImportError:
    from herdcl import logger
    from herdcl.constants import *
    from herdcl.aws import AwsClient

LOGGER = logger.get_logger(__name__)
ERROR_CODE = -99


################################################################################
class Controller:
    """
     The controller class. Makes calls to herdsdk
    """
    # Class variables
    start_time = None
    action = None
    excel_file = ''
    sample_dir = ''
    data_frame = ''
    path = ''
    config = None
    domain = ''
    export_namespace = ''
    export_dir = ''
    export_path = ''
    debug_mode = False
    fip_endpoint = ''

    # Configure HTTP basic authorization: basicAuthentication
    configuration = herdsdk.Configuration()

    actions = [Menu.OBJECTS.value, Menu.SME.value, Menu.OBJECT_TAG.value, Menu.COLUMNS.value, Menu.LINEAGE.value,
               Menu.SAMPLES.value, Menu.TAGS.value, Menu.RELATIONAL.value]
    # actions = ['test_api']
    export_actions = [Menu.LATEST_DESC.value, Menu.LATEST_COLUMNS.value, Menu.LATEST_BDEF_TAGS.value]
    envs = Menu.ENVS.value

    def __init__(self):
        # Instance variables
        self.run_steps = []
        self.tag_types = {
            'columns': []
        }
        self.format_columns = {}
        self.run_summary = {
            Summary.TOTAL.value: 0,
            Summary.SUCCESS.value: 0,
            Summary.FAIL.value: 0,
            Summary.FAIL_INDEX.value: [],
            Summary.COMMENTS.value: '',
            Summary.CHANGES.value: [],
            Summary.WARNINGS.value: [],
            Summary.ERRORS.value: []
        }
        self.sample_files = {}
        self.delete_tag_children = {}
        self.tag_list = {}

        self.acts = {
            str.lower(Menu.OBJECTS.value): self.load_object,
            str.lower(Menu.SME.value): self.load_sme,
            str.lower(Menu.OBJECT_TAG.value): self.load_object_tags,
            str.lower(Menu.COLUMNS.value): self.load_columns,
            str.lower(Menu.LINEAGE.value): self.load_lineage,
            str.lower(Menu.SAMPLES.value): self.load_samples,
            str.lower(Menu.TAGS.value): self.load_tags,
            str.lower(Menu.RELATIONAL.value): self.load_relational,
            str.lower(Menu.LATEST_DESC.value): self.export_descriptive,
            str.lower(Menu.LATEST_COLUMNS.value): self.export_columns,
            str.lower(Menu.LATEST_BDEF_TAGS.value): self.export_bdef_tags,
            'test_api': self.test_api
        }

    ############################################################################
    def load_config(self):
        """
        Load configuration file

        """
        LOGGER.debug(sys.argv)
        self.get_current_directory()

        # Get config file
        config_file = self.path + "/loader.cfg"
        LOGGER.debug('Checking for loader config: {}'.format(config_file))
        if not os.path.exists(config_file):
            message = "Config file loader.cfg not found"
            LOGGER.error(message)
            raise FileNotFoundError(message)
        config = configparser.ConfigParser()
        config.read(config_file)

        self.config = config

    ############################################################################
    def get_current_directory(self):
        """
        Get current working directory

        """
        if os.name == 'nt':
            path = os.getcwd()
        elif '/' in sys.argv[0]:
            path = '/'.join(sys.argv[0].split('/')[:-1])
        else:
            path = os.getcwd()

        self.path = path
        LOGGER.info('Current working directory: {}'.format(self.path))

    ############################################################################
    def get_save_file_path(self, filename):
        """
        Get file path for Excel worksheet

        :param filename: Excel file name
        :type filename: str

        """
        now = datetime.now()

        if self.export_dir:
            self.export_path = self.export_dir + os.sep + 'export_' + now.strftime('%m%d%Y_%H%M%S_') + filename
        else:
            self.export_path = self.path + os.sep + 'export_' + now.strftime('%m%d%Y_%H%M%S_') + filename

    ############################################################################
    def setup_run(self, config):
        """
        Setup run variables

        :param config: Configuration file parser
        :type config: ConfigParser

        """
        self.domain = self.config.get('url', 'domain')
        if config['gui_enabled']:
            self.debug_mode = config['debug_mode']
            self.action = str.lower(config['action'])
            self.excel_file = config['excelFile']
            self.sample_dir = config['sampleDir']
            self.export_namespace = config['namespace']
            self.export_dir = config['exportDir']
            self.configuration.host = self.config.get('url', config['env'])
            self.configuration.username = config['userName']
            self.configuration.password = config['userPwd']
            self.fip_endpoint = self.config.get('fip', config['env'])
        else:
            self.debug_mode = self.config.getboolean('console', 'debug')
            self.action = str.lower(self.config.get('console', 'action'))
            self.excel_file = self.config.get('console', 'excelFile')
            self.sample_dir = self.config.get('console', 'sampleDir')
            self.export_namespace = self.config.get('console', 'namespace')
            self.export_dir = self.config.get('console', 'exportDir')
            env = self.config.get('console', 'env')
            self.configuration.host = self.config.get('url', env)
            self.configuration.username = self.config.get('credentials', 'userName')
            self.configuration.password = base64.b64decode(self.config.get('credentials', 'userPwd')).decode('utf-8')
            self.fip_endpoint = self.config.get('fip', env)

        LOGGER.setLevel(logger.get_level(self.debug_mode))

    ############################################################################
    def setup_access_token(self):
        """
        Setup oauth2 access token

        """

        response = requests.post(self.fip_endpoint, params={'grant_type': 'client_credentials'},
                                 auth=HTTPBasicAuth(self.configuration.username, self.configuration.password))
        response.raise_for_status()
        self.configuration.access_token = response.json()['access_token']

    ############################################################################
    def get_action(self):
        """
        Gets a particular method

        :return: reference to function

        """
        self.reset_run()
        method = self.acts[self.action]
        LOGGER.info('Running {}'.format(method.__name__))
        return method

    ############################################################################
    def load_worksheet(self, sheet):
        """
        Loads Excel worksheet to Pandas DataFrame

        :param sheet: Excel sheet name
        :type sheet: str
        :return: Pandas DataFrame

        """
        LOGGER.info('Loading worksheet name: {}'.format(sheet))
        return pd.read_excel(self.excel_file, sheet_name=sheet).fillna('')

    ############################################################################
    def save_worksheet(self, sheet, filename):
        """
        Saves Pandas DataFrame to Excel worksheet

        :param sheet: Excel sheet name
        :type sheet: str
        :param filename: Excel file name
        :type filename: str

        """
        self.get_save_file_path(filename)
        LOGGER.info('Saving worksheet name: {}'.format(sheet))
        LOGGER.info('Path: {}'.format(self.export_path))
        writer = pd.ExcelWriter(path=self.export_path, engine='xlsxwriter')
        self.data_frame.to_excel(excel_writer=writer, sheet_name=sheet, index=False)

        # Formatting
        # Minus 1 because of zero indexing
        worksheet = writer.sheets[sheet]
        worksheet.set_column(first_col=0, last_col=len(self.data_frame.columns) - 1, width=25)

        workbook = writer.book
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'border': 1
        })
        # Overwrite first row (header) with new format
        for col_num, value in enumerate(self.data_frame.columns.values):
            worksheet.write(0, col_num, value, header_format)

        writer.save()

    ############################################################################
    def load_object(self):
        """
        One of the controller actions. Loads business object definition descriptive information

        :return: Run Summary dict

        """
        self.data_frame = self.load_worksheet(Objects.WORKSHEET.value)
        self.run_summary['total_rows'] = len(self.data_frame.index)

        for index, row in self.data_frame.iterrows():
            row_pass = True

            try:
                self.update_bdef_descriptive_info(index, row)
            except ApiException as e:
                LOGGER.error(e)
                self.update_run_summary_batch([index], e, Summary.ERRORS.value)
                row_pass = False
            except Exception:
                LOGGER.error(traceback.format_exc())
                self.update_run_summary_batch([index], traceback.format_exc(), Summary.ERRORS.value)
                row_pass = False

            if row_pass:
                self.run_summary['success_rows'] += 1

        return self.run_summary

    ############################################################################
    def load_sme(self):
        """
        One of the controller actions. Loads business object definition subject matter experts

        :return: Run Summary dict

        """
        self.data_frame = self.load_worksheet(SubjectMatterExpert.WORKSHEET.value)
        self.run_summary['total_rows'] = len(self.data_frame.index)

        for index, row in self.data_frame.iterrows():
            row_pass = True

            try:
                self.update_sme(index, row)
            except ApiException as e:
                LOGGER.error(e)
                self.update_run_summary_batch([index], e, Summary.ERRORS.value)
                row_pass = False
            except Exception:
                LOGGER.error(traceback.format_exc())
                self.update_run_summary_batch([index], traceback.format_exc(), Summary.ERRORS.value)
                row_pass = False

            if row_pass:
                self.run_summary['success_rows'] += 1

        return self.run_summary

    ############################################################################
    def load_object_tags(self):
        """
        One of the controller actions. Loads business object definition tags

        Steps involved:
        1) Get all tag types and compare with worksheet
        2) Update business object definition tags

        :return: Run Summary dict

        """
        self.data_frame = self.load_worksheet(ObjectTags.WORKSHEET.value)
        self.run_summary['total_rows'] = len(self.data_frame.index)
        self.load_worksheet_tag_types()

        for index, row in self.data_frame.iterrows():
            row_pass = True

            try:
                self.update_bdef_tags(index, row)
            except ApiException as e:
                LOGGER.error(e)
                self.update_run_summary_batch([index], e, Summary.ERRORS.value)
                row_pass = False
            except Exception:
                LOGGER.error(traceback.format_exc())
                self.update_run_summary_batch([index], traceback.format_exc(), Summary.ERRORS.value)
                row_pass = False

            if row_pass:
                self.run_summary['success_rows'] += 1

        return self.run_summary

    ############################################################################
    def load_columns(self):
        """
        One of the controller actions. Loads business object columns

        Steps involved:
        1) Check worksheet schema columns
        2) Get business object definition columns and compare with business object format schema columns
        3) Update business object definition columns

        :return: Run Summary dict

        """
        self.data_frame = self.load_worksheet(Columns.WORKSHEET.value)
        self.run_summary['total_rows'] = len(self.data_frame.index)

        self.check_format_schema_columns()

        self.run_steps = [
            self.get_bdef_columns,
            self.update_bdef_columns
        ]

        group_df = self.data_frame.groupby([Columns.NAMESPACE.value, Columns.DEFINITION_NAME.value])
        for key, index_array in group_df.groups.items():
            for step in self.run_steps:
                step(key, list(index_array.values))

        return self.run_summary

    ############################################################################
    def load_lineage(self):
        """
        One of the controller actions. Loads business object format lineage

        Steps involved:
        1) Check worksheet lineage columns
        2) Compare worksheet with existing business object format parents

        :return: Run Summary dict

        """
        self.data_frame = self.load_worksheet(Lineage.WORKSHEET.value)
        self.run_summary['total_rows'] = len(self.data_frame.index)

        self.check_lineage()

        group_df = self.data_frame.groupby(
            [Lineage.NAMESPACE.value, Lineage.DEFINITION_NAME.value, Lineage.USAGE.value, Lineage.FILE_TYPE.value])
        for key, index_array in group_df.groups.items():
            try:
                self.update_lineage(key, list(index_array.values))
            except ApiException as e:
                LOGGER.error(e)
                self.update_run_summary_batch(index_array, e, Summary.ERRORS.value)
            except Exception:
                LOGGER.error(traceback.format_exc())
                self.update_run_summary_batch(index_array, traceback.format_exc(), Summary.ERRORS.value)

        return self.run_summary

    ############################################################################
    def load_samples(self):
        """
        One of the controller actions. Loads business object sample files

        Steps involved:
        1) Check sample files listed in worksheet
        2) Get existing business object definition sample data files
        3) Download existing and compare. Upload new files

        :return: Run Summary dict

        """
        self.data_frame = self.load_worksheet(Samples.WORKSHEET.value)
        self.run_summary['total_rows'] = len(self.data_frame.index)

        self.check_sample_files()

        self.run_steps = [
            self.get_bdef_sample_files,
            self.upload_download_sample_files
        ]

        group_df = self.data_frame.groupby([Objects.NAMESPACE.value, Objects.DEFINITION_NAME.value])
        for key, index_array in group_df.groups.items():
            for step in self.run_steps:
                step(key, list(index_array.values))

        return self.run_summary

    ############################################################################
    def load_tags(self):
        """
        One of the controller actions. Loads tag types and tag entities

        Steps involved:
        1) Get Tag Type Codes
        2) Update any changes to Tag Type Codes
        3) Get Tag Entities
        4) Update any changes to Tag Entities

        :return: Run Summary dict

        """
        # Tag Types
        self.data_frame = self.load_worksheet(TagTypes.WORKSHEET.value)
        self.run_summary['total_rows'] = len(self.data_frame.index)

        self.check_tag_types()

        self.run_steps = [
            self.get_tag_type_code_list,
            self.update_tag_type_code_list,
            self.delete_tag_type_code_list
        ]

        run_fail = False
        for step in self.run_steps:
            if not run_fail:
                run_fail = step()

        # Tags
        self.data_frame = self.load_worksheet(Tags.WORKSHEET.value)
        self.run_summary['total_rows'] += len(self.data_frame.index)

        self.check_tags()

        self.run_steps = [
            self.update_tag_list,
            self.delete_tag_list
        ]
        for step in self.run_steps:
            if not run_fail:
                run_fail = step()

        return self.run_summary

    ############################################################################
    def load_relational(self):
        """
        One of the controller actions. Registers a relational table

        :return: Run Summary dict

        """
        self.data_frame = self.load_worksheet(Relational.WORKSHEET.value)
        self.run_summary['total_rows'] = len(self.data_frame.index)

        for index, row in self.data_frame.iterrows():
            row_pass = True

            try:
                self.create_relational_table(index, row)
            except ApiException as e:
                LOGGER.error(e)
                self.update_run_summary_batch([index], e, Summary.ERRORS.value)
                row_pass = False
            except Exception:
                LOGGER.error(traceback.format_exc())
                self.update_run_summary_batch([index], traceback.format_exc(), Summary.ERRORS.value)
                row_pass = False

            if row_pass:
                self.run_summary['success_rows'] += 1

        return self.run_summary

    ############################################################################
    def export_descriptive(self):
        """
        One of the controller actions. Gets latest business object definition descriptive information
        Exports data into excel file

        :return: Run Summary dict

        """

        # Get the list of business object definitions
        bdef_keys = self.get_all_bdefs()

        if not bdef_keys:
            return self.run_summary

        # For each bdef, get descriptive info
        data = []
        columns = [
            Objects.NAMESPACE.value, Objects.DEFINITION_NAME.value, Objects.FORMAT_USAGE.value, Objects.FILE_TYPE.value,
            Objects.DISPLAY_NAME.value, Objects.DESCRIPTION.value
        ]
        for index, key in enumerate(bdef_keys):
            try:
                LOGGER.info(
                    'Getting Descriptive Info for {}'.format((key.namespace, key.business_object_definition_name)))
                resp = self.get_business_object_definition(key.namespace, key.business_object_definition_name)
                LOGGER.debug(resp)

                if resp.descriptive_business_object_format:
                    row = [
                        key.namespace,
                        key.business_object_definition_name,
                        resp.descriptive_business_object_format.business_object_format_usage,
                        resp.descriptive_business_object_format.business_object_format_file_type,
                        resp.display_name,
                        resp.description
                    ]
                else:
                    row = [
                        key.namespace,
                        key.business_object_definition_name,
                        '',
                        '',
                        resp.display_name,
                        resp.description
                    ]
                data.append(row)
                self.run_summary[Summary.SUCCESS.value] += 1
            except ApiException as e:
                LOGGER.error(e)
                self.update_run_summary_batch([index], e, Summary.ERRORS.value)
            except Exception:
                LOGGER.error(traceback.format_exc())
                self.update_run_summary_batch([index], traceback.format_exc(), Summary.ERRORS.value)

        message = ('Total rows is total number of bdefs in namespace\n' +
                   'Success rows is total number of rows in excel worksheet')
        self.run_summary[Summary.COMMENTS.value] = message
        self.run_summary[Summary.TOTAL.value] = len(bdef_keys)
        self.data_frame = pd.DataFrame(data, columns=columns)
        self.save_worksheet(Objects.WORKSHEET.value, Objects.EXCEL_NAME.value)
        return self.run_summary

    ############################################################################
    def export_columns(self):
        """
        One of the controller actions. Gets latest business object definition columns
        Exports data into excel file

        :return: Run Summary dict

        """
        bdef_keys = self.get_all_bdefs()

        if not bdef_keys:
            return self.run_summary

        # For each bdef, get columns
        data = []
        columns = [
            Columns.NAMESPACE.value, Columns.DEFINITION_NAME.value, Columns.SCHEMA_NAME.value,
            Columns.COLUMN_NAME.value, Columns.DESCRIPTION.value
        ]
        for index, key in enumerate(bdef_keys):
            try:
                LOGGER.info(
                    'Getting BDef Columns for {}'.format((key.namespace, key.business_object_definition_name)))
                resp = self.post_bdef_column_search(key.namespace, key.business_object_definition_name)
                LOGGER.debug(resp)

                if len(resp.business_object_definition_columns) > 0:
                    LOGGER.info('Found {} columns'.format(len(resp.business_object_definition_columns)))
                    for column in resp.business_object_definition_columns:
                        row = [
                            key.namespace,
                            key.business_object_definition_name,
                            column.schema_column_name,
                            column.business_object_definition_column_key.business_object_definition_column_name,
                            column.description
                        ]
                        data.append(row)
                        self.run_summary[Summary.SUCCESS.value] += 1

            except ApiException as e:
                LOGGER.error(e)
                self.update_run_summary_batch([ERROR_CODE], e, Summary.ERRORS.value)
            except Exception:
                LOGGER.error(traceback.format_exc())
                self.update_run_summary_batch([ERROR_CODE], traceback.format_exc(), Summary.ERRORS.value)

        message = ('Total rows is total number of bdefs in namespace\n' +
                   'Success rows is total number of rows in excel worksheet')
        self.run_summary[Summary.COMMENTS.value] = message
        self.run_summary[Summary.TOTAL.value] = len(bdef_keys)
        self.data_frame = pd.DataFrame(data, columns=columns)
        self.save_worksheet(Columns.WORKSHEET.value, Columns.EXCEL_NAME.value)
        return self.run_summary

    ############################################################################
    def export_bdef_tags(self):
        """
        One of the controller actions. Gets latest business object definition tags
        Exports data into excel file

        :return: Run Summary dict

        """
        # Get the list of business object definitions
        bdef_keys = self.get_all_bdefs()

        if not bdef_keys:
            return self.run_summary

        # Get the list of all tag types for column headers
        if not self.get_all_tags():
            return self.run_summary

        # For each bdef, get tags
        data = []
        columns = [
            ObjectTags.NAMESPACE.value, ObjectTags.DEFINITION_NAME.value
        ]
        columns.extend(self.tag_types['columns'])
        for index, key in enumerate(bdef_keys):
            try:
                for k in self.tag_types['row_entry'].keys():
                    self.tag_types['row_entry'][k].clear()

                LOGGER.info(
                    'Getting BDef Tags for {}'.format((key.namespace, key.business_object_definition_name)))
                resp = self.get_bdef_tags(key.namespace, key.business_object_definition_name)
                LOGGER.debug(resp)
                if len(resp.business_object_definition_tag_keys) > 0:
                    for bdef_tag in resp.business_object_definition_tag_keys:
                        tag_key = bdef_tag.tag_key
                        LOGGER.info('Found Tag Key: {}'.format(tag_key))
                        tag_type_code, tag_code = tag_key.tag_type_code, tag_key.tag_code
                        self.tag_types['row_entry'][tag_type_code].append(tag_code)

                    row = [key.namespace, key.business_object_definition_name]
                    for k in self.tag_types['row_entry'].keys():
                        row.append(','.join(self.tag_types['row_entry'][k]))
                    data.append(row)
                    self.run_summary[Summary.SUCCESS.value] += 1
            except ApiException as e:
                LOGGER.error(e)
                self.update_run_summary_batch([index], e, Summary.ERRORS.value)
            except Exception:
                LOGGER.error(traceback.format_exc())
                self.update_run_summary_batch([index], traceback.format_exc(), Summary.ERRORS.value)

        message = ('Total rows is total number of bdefs in namespace\n' +
                   'Success rows is total number of rows in excel worksheet')
        self.run_summary[Summary.COMMENTS.value] = message
        self.run_summary[Summary.TOTAL.value] = len(bdef_keys)
        self.data_frame = pd.DataFrame(data, columns=columns)
        self.save_worksheet(ObjectTags.WORKSHEET.value, ObjectTags.EXCEL_NAME.value)
        return self.run_summary

    ############################################################################
    def reset_run(self):
        """
        Reset controller variables

        """
        self.run_steps = []
        self.tag_types = {
            'columns': []
        }
        self.format_columns = {}
        self.run_summary = {
            Summary.TOTAL.value: 0,
            Summary.SUCCESS.value: 0,
            Summary.FAIL.value: 0,
            Summary.FAIL_INDEX.value: [],
            Summary.COMMENTS.value: '',
            Summary.CHANGES.value: [],
            Summary.WARNINGS.value: [],
            Summary.ERRORS.value: []
        }
        self.sample_files = {}
        self.delete_tag_children = {}
        self.tag_list = {}

    ############################################################################
    def update_run_summary_batch(self, index_array, message, category):
        """
        Updates run summary. Category can be changes, warnings, or errors

        :param index_array: List of int corresponding to row index in Excel worksheet
        :param message: Error message
        :param category: Type of message. Changes, warnings, or errors

        """
        for index in index_array:
            if index < 0:
                item = {
                    Summary.INDEX.value: index,
                    Summary.MESSAGE.value: message
                }
            else:
                item = {
                    Summary.INDEX.value: index + 2,
                    Summary.MESSAGE.value: message
                }
                if category == Summary.ERRORS.value:
                    self.run_summary[Summary.FAIL.value] += 1
                    self.run_summary[Summary.FAIL_INDEX.value].append(index + 2)
            self.run_summary[category].append(item)

    ############################################################################
    def get_all_bdefs(self):
        """
        Get all bdefs from user entered namespace

        :return: List of bdef keys

        """

        try:
            LOGGER.info('Getting list of Bdefs for {}'.format(self.export_namespace))
            resp = self.get_business_object_definitions(self.export_namespace)
            LOGGER.debug(resp)
            LOGGER.info('Success')
            bdef_keys = resp.business_object_definition_keys

            if len(bdef_keys) == 0:
                message = ('No data entities found for namespace {}'.format(self.export_namespace))
                self.run_summary[Summary.COMMENTS.value] = message
                return
        except ApiException as e:
            LOGGER.error(e)
            self.update_run_summary_batch([ERROR_CODE], e, Summary.ERRORS.value)
            return
        except Exception:
            LOGGER.error(traceback.format_exc())
            self.update_run_summary_batch([ERROR_CODE], traceback.format_exc(), Summary.ERRORS.value)
            return

        return bdef_keys

    ############################################################################
    def get_all_tags(self):
        """
        Get all tags and tag type codes

        :return: True if success, False if fail

        """

        # There can be multiple tags in each tag type code. Collect a list of tags for each code
        self.tag_types['row_entry'] = {}

        try:
            LOGGER.info('Getting list of all tag types')
            resp = self.get_tag_types().tag_type_keys
            LOGGER.debug(resp)
            LOGGER.info('Success')

            for tag in resp:
                code = tag.tag_type_code
                LOGGER.info('Getting display name of tag type code: {}'.format(code))
                display_name = self.get_tag_type_code(code).display_name.strip()
                self.tag_types[code] = display_name
                self.tag_types['columns'].append(display_name)
                self.tag_types['row_entry'][code] = []
        except ApiException as e:
            LOGGER.error(e)
            self.update_run_summary_batch([ERROR_CODE], e, Summary.ERRORS.value)
            return
        except Exception:
            LOGGER.error(traceback.format_exc())
            self.update_run_summary_batch([ERROR_CODE], traceback.format_exc(), Summary.ERRORS.value)
            return

        return True

    ############################################################################
    def update_bdef_descriptive_info(self, index, row):
        """
        Updates an existing business object definition descriptive information

        :param index: Row index in Excel worksheet
        :param row: A row inside the Pandas DataFrame

        """
        # Descriptive format information is inside the business object definition
        namespace, bdef_name, usage, file_type, logical_name, description = row[:6]
        description = description.replace('\n', '<br>')
        LOGGER.info('Getting BDef for {}'.format((namespace, bdef_name)))
        resp = self.get_business_object_definition(namespace, bdef_name)
        LOGGER.debug(resp)
        LOGGER.info('Success')

        # Check if descriptive format exists
        if not resp.descriptive_business_object_format:
            request_json = {
                'description': description,
                'displayName': logical_name,
                'formatUsage': usage,
                'fileType': file_type
            }
            LOGGER.info('Adding BDef Descriptive Info')
            resp = self.update_business_object_definition_descriptive_info(namespace=namespace,
                                                                           business_object_definition_name=bdef_name,
                                                                           update_request=request_json)
            LOGGER.debug(resp)
            LOGGER.info('Success')
            message = 'Change in row. Old Descriptive Info:\nNone'
            LOGGER.info(message)
            self.update_run_summary_batch([index], message, Summary.CHANGES.value)

        # See if description, display name, usage, or file type in excel differs from UDC
        elif (resp.description != description or
              resp.display_name != logical_name or
              resp.descriptive_business_object_format.business_object_format_usage != usage or
              resp.descriptive_business_object_format.business_object_format_file_type != file_type):
            old_data = {
                'description': resp.description,
                'displayName': resp.display_name,
                'formatUsage': resp.descriptive_business_object_format.business_object_format_usage,
                'fileType': resp.descriptive_business_object_format.business_object_format_file_type
            }
            request_json = {
                'description': description,
                'displayName': logical_name,
                'formatUsage': usage,
                'fileType': file_type
            }
            LOGGER.info('Updating BDef Descriptive Info')
            resp = self.update_business_object_definition_descriptive_info(namespace=namespace,
                                                                           business_object_definition_name=bdef_name,
                                                                           update_request=request_json)
            LOGGER.debug(resp)
            LOGGER.info('Success')
            message = 'Change in row. Old Descriptive Info:\n{}'.format(old_data)
            LOGGER.info(message)
            self.update_run_summary_batch([index], message, Summary.CHANGES.value)

    ############################################################################
    def update_sme(self, index, row):
        """
        Updates existing business object definition subject matter experts for a specific business object definition

        :param index: Row index in Excel worksheet
        :param row: A row inside the Pandas DataFrame

        """
        namespace, bdef_name = row[:2]

        # Business Object Definition SMEs Get
        LOGGER.info('Getting SME for {}'.format((namespace, bdef_name)))
        resp = self.get_subject_matter_experts(namespace, bdef_name)
        LOGGER.debug(resp)
        LOGGER.info('Success')

        user = row[SubjectMatterExpert.SME.value]
        if user:
            user = set([u.strip(" ,\t") for u in user.strip().split(',')])

        # Get list of SMEs to create and remove
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

        row_change = False

        # Remove SMEs
        for sme in remove_sme_list:
            user_id = sme + self.domain
            LOGGER.info('Deleting SME: {}'.format(sme))
            resp = self.delete_subject_matter_expert(namespace, bdef_name, user_id)
            LOGGER.debug(resp)
            LOGGER.info('SME deleted')
            row_change = True

        # Create SMEs
        if user:
            for user_id in user:
                if not '@' in user_id:
                    user_id = user_id + self.domain
                LOGGER.info('Adding SME: {}'.format(user_id))
                resp = self.create_subject_matter_expert(namespace, bdef_name, user_id)
                LOGGER.debug(resp)
                LOGGER.info('SME Added')
                row_change = True

        # Add any changes to run summary
        if row_change:
            message = 'Change in row. Old SME list:\n{}'.format(', '.join(current_smes))
            LOGGER.info(message)
            self.update_run_summary_batch([index], message, Summary.CHANGES.value)

    ############################################################################
    def load_worksheet_tag_types(self):
        """
        Gets list of all tag types and compares with Excel worksheet columns

        Each tag type key has a tag type code
        With the tag type code, you can look up tag type display names
        These match with columns in the worksheet

        """
        LOGGER.info('Getting list of all tag types')
        resp = self.get_tag_types().tag_type_keys
        LOGGER.debug(resp)
        LOGGER.info('Success')

        for tag in resp:
            code = tag.tag_type_code
            LOGGER.info('Getting display name of tag type code: {}'.format(code))
            display_name = self.get_tag_type_code(code).display_name.strip()
            LOGGER.info('Display name found: {}'.format(display_name))
            self.tag_types[code] = display_name
            LOGGER.info('Checking if \'{}\' is a column in worksheet'.format(display_name))
            if display_name in list(self.data_frame):
                self.tag_types['columns'].append(code)
                LOGGER.info('Column \'{}\' added'.format(display_name))

    ############################################################################
    def update_bdef_tags(self, index, row):
        """
        Updates business object definition tags

        :param index: Row index in Excel worksheet
        :param row: A row inside the Pandas DataFrame

        """
        namespace, bdef_name = row[:2]

        # Check worksheet for tags in each tag type code
        LOGGER.info('Checking worksheet for BDef tags to add')
        tags_to_add = {}
        for code in self.tag_types['columns']:
            row_entry = []
            display_name = self.tag_types[code]
            if row[display_name]:
                LOGGER.info('Tag data found in column \'{}\''.format(display_name))
                row_entry = [x.strip() for x in row[display_name].split(',')]
            tags_to_add[code] = row_entry

        # Compare existing tags with tags in worksheet
        LOGGER.info('Tags in worksheet: {}'.format(tags_to_add))
        LOGGER.info('Getting Current Bdef Tags')
        row_change = False
        old_tags = {}
        resp = self.get_bdef_tags(namespace, bdef_name)
        LOGGER.debug(resp)
        for bdef_tag in resp.business_object_definition_tag_keys:
            tag_key = bdef_tag.tag_key
            LOGGER.info('Found Tag Key: {}'.format(tag_key))
            tag_type_code, tag_code = tag_key.tag_type_code, tag_key.tag_code
            old_tags[tag_type_code] = tag_code
            if tag_type_code in tags_to_add and tag_code in tags_to_add[tag_type_code]:
                tags_to_add[tag_type_code].remove(tag_code)
            else:
                LOGGER.info('Deleting Tag Key: {}'.format(tag_key))
                resp = self.delete_bdef_tags(namespace, bdef_name, tag_type_code, tag_code)
                LOGGER.debug(resp)
                LOGGER.info('Deleted')
                row_change = True

        for tag_type_code, row_entry in tags_to_add.items():
            for tag_code in row_entry:
                LOGGER.info('Adding {}'.format(tag_code))
                resp = self.create_bdef_tags(namespace, bdef_name, tag_type_code, tag_code)
                LOGGER.debug(resp)
                LOGGER.info('Added')
                row_change = True

        # Add any changes to run summary
        if row_change:
            message = 'Change in row. Old tags:\n{}'.format(json.dumps(old_tags, indent=1))
            LOGGER.info(message)
            self.update_run_summary_batch([index], message, Summary.CHANGES.value)

    ############################################################################
    def check_format_schema_columns(self):
        """
        Checks Excel worksheet for rows with missing schema or column names

        """
        LOGGER.info('Checking schema worksheet for empty values')
        empty_column_name_filter = self.data_frame[Columns.COLUMN_NAME.value] == ''
        empty_schema_name_filter = self.data_frame[Columns.SCHEMA_NAME.value] == ''

        empty_df = self.data_frame[empty_column_name_filter | empty_schema_name_filter]
        good_df = self.data_frame[~empty_column_name_filter & ~empty_schema_name_filter]

        if len(empty_df.index.values) > 0:
            message = 'Columns \'{}\' and \'{}\' cannot have blank values'.format(Columns.COLUMN_NAME.value,
                                                                                  Columns.SCHEMA_NAME.value)
            self.update_run_summary_batch(empty_df.index.values, message, Summary.ERRORS.value)

        self.data_frame = good_df

    ############################################################################
    def get_bdef_columns(self, key, index_array):
        """
        Gets business object definition columns

        :param key: Tuple of namespace and bdef name
        :param index_array: List of int corresponding to row index in Excel worksheet

        """
        LOGGER.info('Getting business definition columns for {}'.format(key))
        (namespace, bdef_name) = key
        try:
            # Descriptive format information is inside the business object definition
            LOGGER.info('Getting BDef')
            resp = self.get_business_object_definition(namespace, bdef_name)
            LOGGER.debug(resp)
            if not resp.descriptive_business_object_format:
                message = 'No Descriptive Format defined for {}'.format(key)
                LOGGER.error(message)
                self.update_run_summary_batch(index_array, message, Summary.ERRORS.value)
                return

            # Once you have descriptive format you can look up schema columns
            LOGGER.info('Success')
            LOGGER.info(resp.descriptive_business_object_format)
            LOGGER.info('Getting Format')
            format_resp = self.get_format(namespace, bdef_name,
                                          resp.descriptive_business_object_format.business_object_format_usage,
                                          resp.descriptive_business_object_format.business_object_format_file_type,
                                          resp.descriptive_business_object_format.business_object_format_version)
            LOGGER.debug(format_resp)
            if not (format_resp.schema and format_resp.schema.columns):
                message = 'No Schema Columns found for {}'.format(key)
                LOGGER.error(message)
                self.update_run_summary_batch(index_array, message, Summary.ERRORS.value)
                return

            # Get schema columns and bdef columns as dataframes. Outer merge the two to check if both contain schema name
            LOGGER.info('Success')
            schema_df = pd.DataFrame(
                [{Columns.SCHEMA_NAME.value: str.upper(x.name).strip()} for x in format_resp.schema.columns])
            LOGGER.info('Getting BDef Columns')
            col_resp = self.post_bdef_column_search(namespace, bdef_name)
            LOGGER.debug(col_resp)
            if len(col_resp.business_object_definition_columns) > 0:
                col_df = pd.DataFrame([{
                    Columns.SCHEMA_NAME.value: str.upper(x.schema_column_name).strip(),
                    Columns.COLUMN_NAME.value: x.business_object_definition_column_key.business_object_definition_column_name,
                    Columns.DESCRIPTION.value: x.description
                } for x in col_resp.business_object_definition_columns])
                LOGGER.info('Comparing Schema Columns and BDef Columns')
                df = pd.merge(schema_df, col_df, on=[Columns.SCHEMA_NAME.value], how='outer', indicator='Found')
                df['Found'] = df['Found'].apply(lambda x: x == 'both')
                self.format_columns[key] = df.fillna('')
            # Found column used to track schema columns that have no corresponding bdef column name
            else:
                schema_df[Columns.COLUMN_NAME.value] = ''
                schema_df[Columns.DESCRIPTION.value] = ''
                schema_df['Found'] = False
                self.format_columns[key] = schema_df
        except ApiException as e:
            LOGGER.error(e)
            self.update_run_summary_batch(index_array, e, Summary.ERRORS.value)
        except Exception:
            LOGGER.error(traceback.format_exc())
            self.update_run_summary_batch(index_array, traceback.format_exc(), Summary.ERRORS.value)

    ############################################################################
    def update_bdef_columns(self, key, index_array):
        """
        Updates business object definition columns

        :param key: Tuple of namespace and bdef name
        :param index_array: List of int corresponding to row index in Excel worksheet

        """
        if key in self.format_columns:
            LOGGER.info('Updating business definition columns for {}'.format(key))
            (namespace, bdef_name) = key

            # Delete bdef columns with no schema name
            LOGGER.info('Checking for bdef column names with no schema name')
            empty_schema_filter = self.format_columns[key][Columns.SCHEMA_NAME.value] == ''
            empty_schema_df = self.format_columns[key][empty_schema_filter]
            self.format_columns[key] = self.format_columns[key][~empty_schema_filter]
            for index, row in empty_schema_df.iterrows():
                try:
                    LOGGER.warning(
                        'Schema Name not found. Deleting BDef Column Name: {}'.format(row[Columns.COLUMN_NAME.value]))
                    self.delete_bdef_column(namespace, bdef_name, row[Columns.COLUMN_NAME.value])
                    LOGGER.warning('Success')
                except ApiException as e:
                    LOGGER.error(e)
                    self.update_run_summary_batch([ERROR_CODE], e, Summary.WARNINGS.value)
                except Exception:
                    LOGGER.error(traceback.format_exc())
                    self.update_run_summary_batch([ERROR_CODE], traceback.format_exc(), Summary.WARNINGS.value)
            empty_schema_list = empty_schema_df[Columns.COLUMN_NAME.value].tolist()
            if len(empty_schema_list) > 0:
                message = 'Could not find a schema name for the following bdef columns:\n{}'.format(
                    pprint.pformat(empty_schema_list, width=60, compact=True))
                self.update_run_summary_batch([ERROR_CODE], message, Summary.WARNINGS.value)

            # Compare excel data to UDC data
            LOGGER.info('Comparing Excel worksheet with UDC data')
            for index in index_array:
                row_change = False
                try:
                    xls_schema_name = str.upper(self.data_frame.at[index, Columns.SCHEMA_NAME.value]).strip()
                    xls_column_name = self.data_frame.at[index, Columns.COLUMN_NAME.value]
                    xls_description = self.data_frame.at[index, Columns.DESCRIPTION.value]
                    xls_description = xls_description.replace('\n', '<br>')

                    # Check if schema name in worksheet matches existing schema name
                    schema_match_filter = self.format_columns[key][Columns.SCHEMA_NAME.value] == xls_schema_name
                    schema_match_df = self.format_columns[key][schema_match_filter]
                    old_column = {}
                    if len(schema_match_df.index) > 0:
                        row = schema_match_df.iloc[0]
                        i = schema_match_df.index.tolist()[0]
                        column_name = row[Columns.COLUMN_NAME.value]
                        description = row[Columns.DESCRIPTION.value]

                        LOGGER.info('Current Column Name: {}\nCurrent Description: {}'.format(column_name, description))
                        old_column = {
                            'Column': column_name,
                            'Description': description
                        }

                        # Found is False means schema had no existing bdef column name
                        if not row['Found']:
                            LOGGER.info('Adding bdef column name: {}'.format(xls_column_name))
                            resp = self.create_bdef_column(namespace, bdef_name, xls_column_name, xls_schema_name,
                                                           xls_description)
                            LOGGER.debug(resp)
                            LOGGER.info('Success')
                            self.format_columns[key].at[i, 'Found'] = True
                            row_change = True
                        # Update existing bdef column name
                        elif column_name != xls_column_name or description != xls_description:
                            LOGGER.info('Changing bdef column name: {}'.format(xls_column_name))
                            self.delete_bdef_column(namespace, bdef_name, row[Columns.COLUMN_NAME.value])
                            resp = self.create_bdef_column(namespace, bdef_name, xls_column_name, xls_schema_name,
                                                           xls_description)
                            LOGGER.debug(resp)
                            LOGGER.info('Success')
                            row_change = True
                        else:
                            LOGGER.info('No changes made')
                    else:
                        message = 'Could not find schema column for bdef column name: {}'.format(xls_column_name)
                        LOGGER.warning(message)
                        self.update_run_summary_batch([index], message, Summary.WARNINGS.value)

                    self.run_summary['success_rows'] += 1
                    if row_change:
                        message = 'Change in row. Old column:\n{}'.format(old_column)
                        LOGGER.info(message)
                        self.update_run_summary_batch([index], message, Summary.CHANGES.value)

                except ApiException as e:
                    LOGGER.error(e)
                    self.update_run_summary_batch([index], e, Summary.ERRORS.value)
                except Exception:
                    LOGGER.error(traceback.format_exc())
                    self.update_run_summary_batch([index], traceback.format_exc(), Summary.ERRORS.value)

            # Found is False means there are still schema names with no bdef column name
            not_found_filter = self.format_columns[key]['Found'] == False
            not_found_df = self.format_columns[key][not_found_filter]
            not_found_list = not_found_df[Columns.SCHEMA_NAME.value].tolist()
            if len(not_found_list) > 0:
                message = 'Could not find bdef column info for the following schema columns:\n{}'.format(
                    pprint.pformat(not_found_list, width=60, compact=True))
                self.update_run_summary_batch([ERROR_CODE], message, Summary.WARNINGS.value)

    ############################################################################
    def check_lineage(self):
        """
        Checks Excel worksheet for rows with missing values

        """
        LOGGER.info('Checking lineage worksheet for empty values')
        # Find rows with any empty values in format columns
        columns = [Lineage.NAMESPACE.value, Lineage.DEFINITION_NAME.value, Lineage.USAGE.value, Lineage.FILE_TYPE.value]
        err_filter = (self.data_frame[columns] == '').any(axis='columns')
        err_df = self.data_frame[err_filter]
        other_df = self.data_frame[~err_filter]

        if len(err_df.index.values) > 0:
            message = 'Row missing values. Double check columns: {}'.format(columns)
            self.update_run_summary_batch(err_df.index.values, message, Summary.ERRORS.value)

        # Find rows with all empty values in parent columns
        parent_columns = ['Parent ' + x for x in columns]
        all_empty_filter = (other_df[parent_columns] == '').all(axis='columns')
        empty_df = other_df[all_empty_filter]
        check_df = other_df[~all_empty_filter]

        # Find rows with missing values in parent columns
        err_filter = (check_df[parent_columns] == '').any(axis='columns')
        err_df = check_df[err_filter]
        passed_df = check_df[~err_filter]

        if len(err_df.index.values) > 0:
            message = 'Row missing values. Double check columns: {}'.format(parent_columns)
            self.update_run_summary_batch(err_df.index.values, message, Summary.ERRORS.value)

        # Combine good dataframes
        good_df = pd.concat([empty_df, passed_df])
        sorted_index = sorted(good_df.index)
        good_df = good_df.reindex(sorted_index)

        self.data_frame = good_df

    ############################################################################
    def update_lineage(self, key, index_array):
        """
        Updates business object format lineage

        :param key: Tuple of namespace and bdef name
        :param index_array: List of int corresponding to row index in Excel worksheet

        """
        (namespace, bdef_name, usage, file_type) = key

        # Get existing format parents
        LOGGER.info('Getting business object format for {}'.format(key))
        resp = self.get_format(namespace, bdef_name, usage, file_type)
        LOGGER.debug(resp)
        LOGGER.info('Success')

        # Create list of format parents from response
        format_parents = []
        for parent in resp.business_object_format_parents:
            format_parents.append({
                Lineage.NAMESPACE.value: str.upper(parent.namespace).strip(),
                Lineage.DEFINITION_NAME.value: str.upper(parent.business_object_definition_name).strip(),
                Lineage.USAGE.value: str.upper(parent.business_object_format_usage).strip(),
                Lineage.FILE_TYPE.value: str.upper(parent.business_object_format_file_type).strip()
            })

        columns = [Lineage.NAMESPACE.value, Lineage.DEFINITION_NAME.value, Lineage.USAGE.value, Lineage.FILE_TYPE.value]
        parent_columns = ['Parent ' + x for x in columns]

        # Find empty rows based on parent columns
        df = self.data_frame.loc[index_array]
        empty_filter = (df[parent_columns] == '').all(axis='columns')
        empty_df = df[empty_filter]
        filled_df = df[~empty_filter]

        # Check if the data is empty, mixed, or completely full
        skip_found = False
        if len(list(empty_df.index.values)) > 0:
            # Data is completely empty. This will remove any existing parents
            if list(empty_df.index.values) == list(df.index.values):
                if format_parents:
                    LOGGER.info('Removing all parents')
                    resp = self.update_format_parents(namespace, bdef_name, usage, file_type, [])
                    LOGGER.debug(resp)
                    LOGGER.info('Success')

                    message = 'Change in rows: {}\nAll parents removed. Old Parents:\n{}'.format(
                        empty_df.index.tolist(), format_parents)
                    LOGGER.info(message)
                    self.update_run_summary_batch([empty_df.index[0]], message, Summary.CHANGES.value)
                else:
                    LOGGER.info('No parent changes made')
                self.run_summary['success_rows'] += len(df.index)
                return
            else:
                skip_found = True

        # There are rows with no parents and rows with parents
        if skip_found:
            message = 'Mix of empty and nonempty rows found. Skipping empty rows: {}'.format(empty_df.index.tolist())
            self.update_run_summary_batch([empty_df.index[0]], message, Summary.WARNINGS.value)

        # Create list of format parents in worksheet
        xls_parent_list = []
        filled_df.apply(lambda row: xls_parent_list.append({
            Lineage.NAMESPACE.value: str.upper(row[parent_columns[0]]).strip(),
            Lineage.DEFINITION_NAME.value: str.upper(row[parent_columns[1]]).strip(),
            Lineage.USAGE.value: str.upper(row[parent_columns[2]]).strip(),
            Lineage.FILE_TYPE.value: str.upper(row[parent_columns[3]]).strip()
        }), 1)

        # Check if existing format parents differ from worksheet
        if format_parents:
            format_parents, xls_parent_list = [
                sorted(l, key=lambda x: (x[columns[0]], x[columns[1]], x[columns[2]], x[columns[3]]))
                for l in (format_parents, xls_parent_list)
            ]
            if format_parents == xls_parent_list:
                LOGGER.info('No parent changes made')
                self.run_summary['success_rows'] += len(df.index)
                return

        LOGGER.info('Updating parents')
        resp = self.update_format_parents(namespace, bdef_name, usage, file_type, xls_parent_list)
        LOGGER.debug(resp)
        LOGGER.info('Success')
        message = 'Change in rows: {}\nUpdated parents. Old Parents:\n{}'.format(filled_df.index.tolist(),
                                                                                 format_parents)
        LOGGER.info(message)
        self.update_run_summary_batch([filled_df.index[0]], message, Summary.CHANGES.value)
        self.run_summary['success_rows'] += len(df.index)

    ############################################################################
    def check_sample_files(self):
        """
        Checks Excel worksheet for rows with no sample file

        """
        LOGGER.info('Checking samples worksheet for empty values')
        empty_sample_filter = self.data_frame[Samples.SAMPLE.value] == ''
        empty_df = self.data_frame[empty_sample_filter]
        good_df = self.data_frame[~empty_sample_filter]

        self.run_summary['success_rows'] += len(empty_df.index.values)
        self.data_frame = good_df

    ############################################################################
    def get_bdef_sample_files(self, key, index_array):
        """
        Gets list of sample files and uploads new sample files associated with a business object definition

        :param key: Tuple of namespace and bdef name
        :param index_array: List of int corresponding to row index in Excel worksheet

        """
        (namespace, bdef_name) = key
        try:
            # List of sample files is inside the business object definition
            LOGGER.info('Getting BDef for {}'.format(key))
            resp = self.get_business_object_definition(namespace, bdef_name)
            LOGGER.debug(resp)
            LOGGER.info('Success')

            if key not in self.sample_files:
                self.sample_files[key] = {}

            if resp.sample_data_files:
                LOGGER.info('Found existing sample files')
                for sample in resp.sample_data_files:
                    self.sample_files[key][sample.file_name] = sample.directory_path
            else:
                LOGGER.info('No files found for bdef: {}'.format(key))

        except ApiException as e:
            LOGGER.error(e)
            self.update_run_summary_batch(index_array, e, Summary.ERRORS.value)
            return
        except Exception:
            LOGGER.error(traceback.format_exc())
            self.update_run_summary_batch(index_array, traceback.format_exc(), Summary.ERRORS.value)
            return

    ############################################################################
    def upload_download_sample_files(self, key, index_array):
        """
        Gets list of sample files and uploads new sample files associated with a business object definition

        :param key: Tuple of namespace and bdef name
        :param index_array: List of int corresponding to row index in Excel worksheet

        """
        (namespace, bdef_name) = key

        if key in self.sample_files:
            LOGGER.info('Updating Sample Files for {}'.format(key))
            sample_files = self.sample_files[key]
            uploaded_files = []

            for index in index_array:
                try:
                    file = self.data_frame.at[index, Samples.SAMPLE.value]
                    path = self.sample_dir + os.sep + file

                    if not os.path.exists(path):
                        message = 'File not found. Please double check path: {}'.format(path)
                        LOGGER.error(message)
                        self.update_run_summary_batch([index], message, Summary.ERRORS.value)
                        continue

                    # No need to reupload the same file
                    if file in uploaded_files:
                        LOGGER.info('File already uploaded. Skipping: {}'.format(file))
                        self.run_summary['success_rows'] += 1
                        continue

                    if file in sample_files:
                        LOGGER.info('Matched File: {}\nChecking if contents changed'.format(file))

                        LOGGER.info('Getting download request')
                        download_resp = self.download_sample_file(namespace, bdef_name, sample_files[file], file)
                        LOGGER.debug(download_resp)
                        LOGGER.info('Success')

                        temp_path = self.sample_dir + os.sep + 'temp'
                        setattr(download_resp, 's3_key_prefix',
                                download_resp.business_object_definition_sample_data_file_key.directory_path)
                        LOGGER.info('Downloading File: {}'.format(file))
                        aws_err = self.run_aws_command('s3_download', download_resp, temp_path, file)
                        if aws_err:
                            LOGGER.error(aws_err)
                            self.update_run_summary_batch([index], aws_err, Summary.ERRORS.value)
                            continue
                        LOGGER.info('Success')

                        # No need to upload a file if the contents haven't changed
                        LOGGER.info('Comparing contents')
                        content_same = filecmp.cmp(temp_path, path, shallow=False)
                        os.remove(temp_path)
                        if content_same:
                            LOGGER.info('Files are identical. Skipping: {}'.format(file))
                            uploaded_files.append(file)
                            self.run_summary['success_rows'] += 1
                            continue
                        else:
                            LOGGER.info('Contents have changed. Updating')

                    LOGGER.info('Getting upload request')
                    upload_resp = self.upload_sample_file(namespace, bdef_name)
                    LOGGER.debug(upload_resp)
                    LOGGER.info('Success')
                    LOGGER.info('Uploading File: {}'.format(file))
                    aws_err = self.run_aws_command('s3_upload', upload_resp, path, file)
                    if aws_err:
                        LOGGER.error(aws_err)
                        self.update_run_summary_batch([index], aws_err, Summary.ERRORS.value)
                        continue
                    LOGGER.info('Success')

                    uploaded_files.append(file)
                    self.run_summary['success_rows'] += 1
                    message = 'Change in row. Old files: {}'.format(sample_files)
                    LOGGER.info(message)
                    self.update_run_summary_batch([index], message, Summary.CHANGES.value)

                except ApiException as e:
                    LOGGER.error(e)
                    self.update_run_summary_batch([index], e, Summary.ERRORS.value)
                except Exception:
                    LOGGER.error(traceback.format_exc())
                    self.update_run_summary_batch([index], traceback.format_exc(), Summary.ERRORS.value)

    ############################################################################
    def create_relational_table(self, index, row):
        """
        Registers a relational table. Relational table is mapped to Herd data model

        :param index: Row index in Excel worksheet
        :param row: A row inside the Pandas DataFrame

        """
        namespace, bdef_name, usage, data_provider, schema, table_name, storage, append = row[:8]

        # Relational Table Post
        LOGGER.info('Creating Relational Table')
        request_json = {
            'namespace': namespace,
            'business_object_definition_name': bdef_name,
            'business_object_format_usage': usage,
            'data_provider_name': data_provider,
            'relational_schema_name': schema,
            'relational_table_name': table_name,
            'storage_name': storage,
            'append': str(append).lower()
        }
        resp = self.post_relational_table(request_json)
        LOGGER.debug(resp)
        LOGGER.info('Success')
        message = 'Change in row. Created Relational Table:\n{}'.format(json.dumps(request_json, indent=1))
        LOGGER.info(message)
        self.update_run_summary_batch([index], message, Summary.CHANGES.value)

    ############################################################################
    def run_aws_command(self, command, resp, path, file):
        """
        Runs aws command

        :return: None if command succeeds, error message if fails

        """
        aws = AwsClient(resp, file)

        try:
            method = aws.get_method(command)
        except AttributeError:
            return 'Command {} not found'.format(command)

        return method(path)

    ############################################################################
    def check_tag_types(self):
        """
        Checks Excel worksheet for rows with no tag types

        """
        LOGGER.info('Checking schema worksheet for empty values')
        empty_name_filter = self.data_frame[TagTypes.NAME.value] == ''
        empty_code_filter = self.data_frame[TagTypes.CODE.value] == ''

        empty_df = self.data_frame[empty_name_filter | empty_code_filter]
        good_df = self.data_frame[~empty_name_filter & ~empty_code_filter]

        if len(empty_df.index.values) > 0:
            message = 'Columns \'{}\' and \'{}\' cannot have blank values'.format(TagTypes.NAME.value,
                                                                                  TagTypes.CODE.value)
            self.update_run_summary_batch(empty_df.index.values, message, Summary.ERRORS.value)

        self.data_frame = good_df

    ############################################################################
    def get_tag_type_code_list(self):
        """
        Get entire tag type code list

        :return: None if success, True if fail

        """
        try:
            LOGGER.info('Getting list of all tag types')
            resp = self.get_tag_types().tag_type_keys
            LOGGER.debug(resp)
            LOGGER.info('Success')

            for tag in resp:
                code = str.upper(tag.tag_type_code).strip()
                LOGGER.info('Getting info of tag type code: {}'.format(code))
                code_resp = self.get_tag_type_code(code)
                LOGGER.debug(code_resp)
                if code not in self.tag_types:
                    self.tag_types[code] = {
                        'name': code_resp.display_name,
                        'description': code_resp.description,
                        'order': code_resp.tag_type_order
                    }
        except ApiException as e:
            LOGGER.error(e)
            self.update_run_summary_batch([ERROR_CODE], e, Summary.ERRORS.value)
            return True
        except Exception:
            LOGGER.error(traceback.format_exc())
            self.update_run_summary_batch([ERROR_CODE], traceback.format_exc(), Summary.ERRORS.value)
            return True

    ############################################################################
    def update_tag_type_code_list(self):
        """
        Compare tag types with excel worksheet and update

        :return: None if success, True if fail

        """
        run_fail = False

        # Create list of tag types and tags to remove
        remove_tags = []
        remove_tag_types = list(self.tag_types.keys())
        remove_tag_types.remove('columns')
        LOGGER.info('Comparing Excel worksheet with UDC data')
        for index, row in self.data_frame.iterrows():
            try:
                xls_order = index + 1
                xls_code = str.upper(row[TagTypes.CODE.value]).strip()
                xls_name = row[TagTypes.NAME.value]
                xls_description = row[TagTypes.DESCRIPTION.value]

                # Found excel tag type
                if xls_code in self.tag_types:
                    # Get list of all tags
                    if xls_code not in self.tag_list:
                        self.tag_list[xls_code] = []
                    self.get_tag_type_children(tag_type_code=xls_code, tag_code=None)

                    for tag in self.tag_list[xls_code]:
                        item = (xls_code, tag)
                        remove_tags.append(item)

                    # Take out excel tag types found in Herd
                    remove_tag_types.remove(xls_code)

                    # Make updates if there are differences
                    if (xls_name != self.tag_types[xls_code]['name'] or
                            xls_description != self.tag_types[xls_code]['description']):
                        # TODO Check order
                        xls_order = self.tag_types[xls_code]['order']
                        LOGGER.info('Updating {}'.format(xls_code))
                        resp = self.update_tag_type(xls_code, xls_name, xls_order, xls_description)
                        LOGGER.debug(resp)
                        LOGGER.info('Success')

                        message = 'Change in row. Old Tag Type Code:\n{}'.format(self.tag_types[xls_code])
                        LOGGER.info(message)
                        self.update_run_summary_batch([index], message, Summary.CHANGES.value)
                    else:
                        LOGGER.info('No change made to {}'.format(xls_code))
                # Add new tag type
                else:
                    LOGGER.info('Tag Type Code not found. Adding {}'.format(xls_code))
                    resp = self.create_tag_type(xls_code, xls_name, xls_order, xls_description)
                    LOGGER.debug(resp)
                    LOGGER.info('Success')

                    message = 'Change in row. Old Tag Type Code:\nNone'
                    LOGGER.info(message)
                    self.update_run_summary_batch([index], message, Summary.CHANGES.value)

                    self.tag_list[xls_code] = []

                self.run_summary['success_rows'] += 1
            except ApiException as e:
                LOGGER.error(e)
                self.update_run_summary_batch([index], e, Summary.ERRORS.value)
                run_fail = True
            except Exception:
                LOGGER.error(traceback.format_exc())
                self.update_run_summary_batch([index], traceback.format_exc(), Summary.ERRORS.value)
                run_fail = True

        self.tag_types['remove'] = remove_tag_types
        self.tag_list['remove'] = remove_tags
        return run_fail

    ############################################################################
    def get_tag_type_children(self, tag_type_code, tag_code):
        """
        Recursive function. Finds all children of parent tag type code

        :param tag_type_code: Tag type reference code
        :param tag_code: Tag reference code

        """
        resp = self.get_associated_tags(tag_type_code, tag_code)
        LOGGER.debug(resp)
        for child in resp.tag_children:
            self.tag_list[tag_type_code].append(child.tag_key.tag_code)
            if child.has_children:
                self.get_tag_type_children(child.tag_key.tag_type_code, child.tag_key.tag_code)

    ############################################################################
    def delete_tag_type_code_list(self):
        """
        Delete tag types not found in excel worksheet

        :return: None if success, True if fail

        """
        run_fail = False

        if len(self.tag_types['remove']) > 0:
            LOGGER.info('Deleting tag types not found in Excel')
            for code in self.tag_types['remove']:
                try:
                    if code not in self.delete_tag_children:
                        self.delete_tag_children[code] = []
                    LOGGER.info('Deleting children of Tag Type Code: {}'.format(code))
                    self.delete_tag_type_children(tag_type_code=code, tag_code=None, level=0)
                    LOGGER.info('Deleting Tag Type Code: {}'.format(code))
                    resp = self.delete_tag_type(code)
                    LOGGER.debug(resp)
                    LOGGER.info('Success')
                except ApiException as e:
                    LOGGER.error(e)
                    self.update_run_summary_batch([ERROR_CODE], e, Summary.ERRORS.value)
                    self.delete_tag_children[code].append('Error while deleting')
                    run_fail = True
                except Exception:
                    LOGGER.error(traceback.format_exc())
                    self.update_run_summary_batch([ERROR_CODE], traceback.format_exc(), Summary.ERRORS.value)
                    self.delete_tag_children[code].append('Error while deleting')
                    run_fail = True

            tag_children = self.delete_tag_children
            message = 'Tag Type Codes not found in Excel and Children deleted:\n{}'.format(
                json.dumps(tag_children, indent=1))
            LOGGER.info(message)
            self.update_run_summary_batch([ERROR_CODE], message, Summary.CHANGES.value)

        return run_fail

    ############################################################################
    def delete_tag_type_children(self, tag_type_code, tag_code, level):
        """
        Recursive function. Finds and deletes children of parent tag type code

        :param tag_type_code: Tag type reference code
        :param tag_code: Tag reference code
        :param level: Child level

        """
        resp = self.get_associated_tags(tag_type_code, tag_code)
        LOGGER.debug(resp)
        for child in resp.tag_children:
            if level == 0:
                self.delete_tag_children[tag_type_code].append(child.tag_key.tag_code)
            else:
                if tag_code not in self.delete_tag_children:
                    self.delete_tag_children[tag_code] = []
                self.delete_tag_children[tag_code].append(child.tag_key.tag_code)

            LOGGER.info('Checking for children in {}'.format(child.tag_key.tag_code))
            if child.has_children:
                LOGGER.info('Child found in {}'.format(child.tag_key.tag_code))
                new_level = level + 1
                self.delete_tag_type_children(child.tag_key.tag_type_code, child.tag_key.tag_code, new_level)
                LOGGER.info('Finished deleting all children found in {}'.format(child.tag_key.tag_code))

            LOGGER.info('Deleting {}'.format(child.tag_key.tag_code))
            resp = self.delete_tags(child.tag_key.tag_type_code, child.tag_key.tag_code)
            LOGGER.debug(resp)
            LOGGER.info('Success')

    ############################################################################
    def check_tags(self):
        """
        Checks Excel worksheet for rows with no tags

        """
        LOGGER.info('Checking schema worksheet for empty values')
        empty_name_filter = self.data_frame[Tags.NAME.value] == ''
        empty_type_filter = self.data_frame[Tags.TAGTYPE.value] == ''
        empty_code_filter = self.data_frame[Tags.TAG.value] == ''

        empty_df = self.data_frame[empty_name_filter | empty_type_filter | empty_code_filter]
        good_df = self.data_frame[~empty_name_filter & ~empty_type_filter & ~empty_code_filter]

        if len(empty_df.index.values) > 0:
            message = 'Columns \'{}\' and \'{}\' and \'{}\' cannot have blank values'.format(Tags.NAME.value,
                                                                                             Tags.TAGTYPE.value,
                                                                                             Tags.TAG.value)
            self.update_run_summary_batch(empty_df.index.values, message, Summary.ERRORS.value)

        self.data_frame = good_df

    ############################################################################
    def update_tag_list(self):
        """
        Compare tags with excel worksheet and update

        :return: None if success, True if fail

        """
        run_fail = False

        LOGGER.info('Comparing Excel worksheet with UDC data')
        for index, row in self.data_frame.iterrows():
            try:
                xls_name = row[Tags.NAME.value]
                xls_tag = str.upper(row[Tags.TAG.value]).strip()
                xls_tag_type = str.upper(row[Tags.TAGTYPE.value]).strip()
                xls_description = row[Tags.DESCRIPTION.value]
                xls_description = xls_description.replace('\n', '<br>')
                xls_parent = row[Tags.PARENT.value]

                xls_description, xls_parent = self.get_tag_optional_fields(xls_description,
                                                                           xls_parent,
                                                                           index)
                # Found excel tag
                if xls_tag in self.tag_list[xls_tag_type]:
                    item = (xls_tag_type, xls_tag)
                    if item in self.tag_list['remove']:
                        self.tag_list['remove'].remove(item)

                    LOGGER.info('Getting tag info for: {}'.format(xls_tag))
                    resp = self.get_tags(xls_tag_type, xls_tag)
                    LOGGER.debug(resp)
                    LOGGER.info('Success')

                    parent_key = resp.parent_tag_key
                    if parent_key:
                        parent = {
                            'tag_type_code': parent_key.tag_type_code,
                            'tag_code': parent_key.tag_code
                        }
                    else:
                        parent = None

                    # Update if differences
                    if (resp.display_name != xls_name or
                            resp.tag_key.tag_type_code != xls_tag_type or
                            resp.tag_key.tag_code != xls_tag or
                            resp.description != xls_description or
                            parent != xls_parent):
                        # TODO Multiplier
                        xls_multiplier = resp.search_score_multiplier
                        LOGGER.info('Updating Tag: {}'.format(xls_tag))
                        update_resp = self.update_tags(
                            tag_type_code=xls_tag_type,
                            tag_code=xls_tag,
                            display_name=xls_name,
                            multiplier=xls_multiplier,
                            description=xls_description,
                            parent_tag=xls_parent
                        )
                        LOGGER.debug(update_resp)
                        LOGGER.info('Success')

                        tag = {
                            'name': resp.display_name,
                            'tag_type_code': resp.tag_key.tag_type_code,
                            'tag_code': resp.tag_key.tag_code,
                            'description': resp.description,
                            'parent': parent,
                            'multiplier': resp.search_score_multiplier
                        }
                        message = 'Change in row. Old Tag:\n{}'.format(tag)
                        LOGGER.info(message)
                        self.update_run_summary_batch([index], message, Summary.CHANGES.value)

                    else:
                        LOGGER.info('No change made to {}'.format(xls_tag))

                # Did not find existing tag, adding new tag
                else:
                    LOGGER.info('Creating new tag: {}'.format(xls_tag))
                    create_resp = self.create_tags(
                        tag_type_code=xls_tag_type,
                        tag_code=xls_tag,
                        display_name=xls_name,
                        description=xls_description,
                        parent_tag=xls_parent
                    )
                    LOGGER.debug(create_resp)
                    LOGGER.info('Success')
                    message = 'Change in row. Old Tag:\nNone'
                    LOGGER.info(message)
                    self.update_run_summary_batch([index], message, Summary.CHANGES.value)

                self.run_summary['success_rows'] += 1
            except ApiException as e:
                LOGGER.error(e)
                self.update_run_summary_batch([index], e, Summary.ERRORS.value)
                run_fail = True
            except Exception:
                LOGGER.error(traceback.format_exc())
                self.update_run_summary_batch([index], traceback.format_exc(), Summary.ERRORS.value)
                run_fail = True

        return run_fail

    ############################################################################
    def delete_tag_list(self):
        """
        Delete tags not found in excel worksheet

        :return: None if success, True if fail

        """
        self.delete_tag_children = {}
        if len(self.tag_list['remove']) > 0:
            LOGGER.info('Deleting tags not found in Excel')
            for item in self.tag_list['remove']:
                try:
                    tag_type, tag = item
                    LOGGER.info('Deleting tag: {}'.format(tag))
                    resp = self.delete_tags(tag_type, tag)
                    LOGGER.debug(resp)
                    LOGGER.info('Success')

                    if tag_type not in self.delete_tag_children:
                        self.delete_tag_children[tag_type] = []

                    self.delete_tag_children[tag_type].append(tag)

                except ApiException as e:
                    LOGGER.error(e)
                    self.update_run_summary_batch([ERROR_CODE], e, Summary.ERRORS.value)
                except Exception:
                    LOGGER.error(traceback.format_exc())
                    self.update_run_summary_batch([ERROR_CODE], traceback.format_exc(), Summary.ERRORS.value)

            message = 'Tags not found in Excel and deleted:\n{}'.format(
                json.dumps(self.delete_tag_children, indent=1))
            LOGGER.info(message)
            self.update_run_summary_batch([ERROR_CODE], message, Summary.CHANGES.value)

    ############################################################################
    def get_tag_optional_fields(self, description, parent, index):
        """
        Checks the optional tag fields for NULL or blank values

        :return: description, parent (as dict if exists)

        """
        if description == '':
            description = None

        if parent == 'NULL' or parent == '':
            parent = None
        else:
            match_index = self.data_frame.index[self.data_frame[Tags.NAME.value] == parent].tolist()

            if len(match_index) > 0:
                row = self.data_frame.loc[match_index[0]]
                parent = {
                    'tag_type_code': row[Tags.TAGTYPE.value],
                    'tag_code': row[Tags.TAG.value]
                }
            else:
                parent = None
                message = 'Parent {} not found in Column {}. Please double check spelling'.format(parent,
                                                                                                  Tags.NAME.value)
                LOGGER.warning(message)
                self.update_run_summary_batch([index], message, Summary.WARNINGS.value)

        return description, parent

    ############################################################################
    def test_api(self):
        """
        One of the controller actions. Calls Get Build Info. Quick way to check api

        :return: Run Summary dict

        """
        self.run_summary['total_rows'] = 1
        resp = self.get_current_user()
        LOGGER.debug(resp)
        return self.run_summary

    '''
    ############################################################################
    --- HERDSDK CALLS ---
    ############################################################################
    '''

    def get_current_user(self):
        """
        Gets the current user permissions for DM

        :return: response from herdsdk call

        """
        # create an instance of the API class
        api_instance = herdsdk.CurrentUserApi(ApiClientOverwrite(self.configuration))

        LOGGER.info('GET /currentUser')
        api_response = api_instance.current_user_get_current_user()
        return api_response

    ############################################################################
    def get_business_object_definitions(self, namespace):
        api_client = ApiClientOverwrite(self.configuration)
        api_instance = herdsdk.BusinessObjectDefinitionApi(api_client)

        LOGGER.info('GET /businessObjectDefinitions/namespaces/{}'.format(namespace))
        api_response = api_instance.business_object_definition_get_business_object_definitions1(namespace)
        return api_response

    ############################################################################
    def get_business_object_definition(self, namespace, business_object_definition_name):
        api_client = ApiClientOverwrite(self.configuration)
        api_instance = herdsdk.BusinessObjectDefinitionApi(api_client)

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
        api_client = ApiClientOverwrite(self.configuration)
        api_instance = herdsdk.BusinessObjectDefinitionApi(api_client)

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
            namespace,
            business_object_definition_name)
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
            namespace,
            business_object_definition_name,
            user_id)
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

        LOGGER.info('POST /businessObjectDefinitionSubjectMatterExperts')
        api_response = api_instance.business_object_definition_subject_matter_expert_create_business_object_definition_subject_matter_expert(
            business_object_definition_subject_matter_expert_create_request)
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
    def get_bdef_tags(self, namespace, business_object_definition_name):
        api_instance = herdsdk.BusinessObjectDefinitionTagApi(herdsdk.ApiClient(self.configuration))

        LOGGER.info(
            'GET /businessObjectDefinitionTags/namespaces/{}/businessObjectDefinitionNames/{}'.format(
                namespace,
                business_object_definition_name))
        api_response = api_instance.business_object_definition_tag_get_business_object_definition_tags_by_business_object_definition(
            namespace,
            business_object_definition_name)
        return api_response

    ############################################################################
    def delete_bdef_tags(self, namespace, business_object_definition_name, tag_type_code, tag_code):
        api_instance = herdsdk.BusinessObjectDefinitionTagApi(herdsdk.ApiClient(self.configuration))

        LOGGER.info(
            'DELETE /businessObjectDefinitionTags/namespaces/{}/businessObjectDefinitionNames/{}/tagTypes/{}/tagCodes/{}'.format(
                namespace,
                business_object_definition_name,
                tag_type_code,
                tag_code))
        api_response = api_instance.business_object_definition_tag_delete_business_object_definition_tag(
            namespace,
            business_object_definition_name,
            tag_type_code,
            tag_code)
        return api_response

    ############################################################################
    def create_bdef_tags(self, namespace, business_object_definition_name, tag_type_code, tag_code):
        api_instance = herdsdk.BusinessObjectDefinitionTagApi(herdsdk.ApiClient(self.configuration))

        business_object_definition_key = herdsdk.BusinessObjectDefinitionKey(
            namespace=namespace,
            business_object_definition_name=business_object_definition_name
        )
        tag_key = herdsdk.TagKey(
            tag_type_code=tag_type_code,
            tag_code=tag_code
        )
        business_object_definition_tag_key = herdsdk.BusinessObjectDefinitionTagKey(
            business_object_definition_key=business_object_definition_key,
            tag_key=tag_key
        )
        business_object_definition_tag_create_request = herdsdk.BusinessObjectDefinitionTagCreateRequest(
            business_object_definition_tag_key=business_object_definition_tag_key
        )

        LOGGER.info('POST /businessObjectDefinitionTags')
        api_response = api_instance.business_object_definition_tag_create_business_object_definition_tag(
            business_object_definition_tag_create_request)
        return api_response

    ############################################################################
    def get_format(self, namespace, business_object_definition_name, business_object_format_usage,
                   business_object_format_file_type, business_object_format_version=None):
        api_instance = herdsdk.BusinessObjectFormatApi(herdsdk.ApiClient(self.configuration))

        LOGGER.info(
            'GET /businessObjectFormats/namespaces/{}/businessObjectDefinitionNames/{}/businessObjectFormatUsages/{}/businessObjectFormatFileTypes/{}'
            '?businessObjectFormatVersion={}'.format(
                namespace,
                business_object_definition_name,
                business_object_format_usage,
                business_object_format_file_type,
                business_object_format_version))

        if business_object_format_version is None:
            api_response = api_instance.business_object_format_get_business_object_format(
                namespace,
                business_object_definition_name,
                business_object_format_usage,
                business_object_format_file_type)
        else:
            api_response = api_instance.business_object_format_get_business_object_format(
                namespace,
                business_object_definition_name,
                business_object_format_usage,
                business_object_format_file_type,
                business_object_format_version=business_object_format_version)
        return api_response

    ############################################################################
    def post_bdef_column_search(self, namespace, business_object_definition_name):
        api_instance = herdsdk.BusinessObjectDefinitionColumnApi(herdsdk.ApiClient(self.configuration))
        business_object_definition_column_search_keys = [herdsdk.BusinessObjectDefinitionColumnSearchKey(
            namespace=namespace,
            business_object_definition_name=business_object_definition_name
        )]
        business_object_definition_column_search_filters = [herdsdk.BusinessObjectDefinitionColumnSearchFilter(
            business_object_definition_column_search_keys=business_object_definition_column_search_keys
        )]
        business_object_definition_column_search_request = herdsdk.BusinessObjectDefinitionColumnSearchRequest(
            business_object_definition_column_search_filters=business_object_definition_column_search_filters
        )
        fields = 'schemaColumnName, description'

        LOGGER.info('POST /businessObjectDefinitionColumns/search?fields={}'.format(fields))
        api_response = api_instance.business_object_definition_column_search_business_object_definition_columns(
            business_object_definition_column_search_request, fields=fields)
        return api_response

    ############################################################################
    def delete_bdef_column(self, namespace, business_object_definition_name, business_object_definition_column_name):
        api_instance = herdsdk.BusinessObjectDefinitionColumnApi(herdsdk.ApiClient(self.configuration))

        LOGGER.info(
            'DELETE /businessObjectDefinitionColumns/namespaces/{}/businessObjectDefinitionNames/{}/businessObjectDefinitionColumnNames/{}'.format(
                namespace,
                business_object_definition_name,
                business_object_definition_column_name))
        api_response = api_instance.business_object_definition_column_delete_business_object_definition_column(
            namespace, business_object_definition_name, business_object_definition_column_name)
        return api_response

    ############################################################################
    def create_bdef_column(self, namespace, business_object_definition_name, business_object_definition_column_name,
                           schema_column_name, description):
        api_instance = herdsdk.BusinessObjectDefinitionColumnApi(herdsdk.ApiClient(self.configuration))

        business_object_definition_column_key = herdsdk.BusinessObjectDefinitionColumnKey(
            namespace=namespace,
            business_object_definition_name=business_object_definition_name,
            business_object_definition_column_name=business_object_definition_column_name
        )
        business_object_definition_column_create_request = herdsdk.BusinessObjectDefinitionColumnCreateRequest(
            business_object_definition_column_key=business_object_definition_column_key,
            schema_column_name=schema_column_name,
            description=description
        )

        LOGGER.info('POST /businessObjectDefinitionColumns')
        api_response = api_instance.business_object_definition_column_create_business_object_definition_column(
            business_object_definition_column_create_request)
        return api_response

    ############################################################################
    def update_format_parents(self, namespace, business_object_definition_name, business_object_format_usage,
                              business_object_format_file_type,
                              parent_list):
        api_instance = herdsdk.BusinessObjectFormatApi(herdsdk.ApiClient(self.configuration))

        business_object_format_parents = []
        if parent_list:
            for parent in parent_list:
                format_key = herdsdk.BusinessObjectFormatKey(
                    namespace=parent[Lineage.NAMESPACE.value],
                    business_object_definition_name=parent[Lineage.DEFINITION_NAME.value],
                    business_object_format_usage=parent[Lineage.USAGE.value],
                    business_object_format_file_type=parent[Lineage.FILE_TYPE.value]
                )
                business_object_format_parents.append(format_key)
        business_object_format_parents_update_request = herdsdk.BusinessObjectFormatParentsUpdateRequest(
            business_object_format_parents=business_object_format_parents
        )

        LOGGER.info(
            'PUT /businessObjectFormatParents/namespaces/{}/businessObjectDefinitionNames/{}/businessObjectFormatUsages/{}/businessObjectFormatFileTypes/{}'.format(
                namespace,
                business_object_definition_name,
                business_object_format_usage,
                business_object_format_file_type
            ))
        api_response = api_instance.business_object_format_update_business_object_format_parents(
            namespace,
            business_object_definition_name,
            business_object_format_usage,
            business_object_format_file_type,
            business_object_format_parents_update_request)
        return api_response

    ############################################################################
    def upload_sample_file(self, namespace, business_object_definition_name):
        api_client = ApiClientOverwrite(self.configuration)
        api_instance = herdsdk.UploadAndDownloadApi(api_client)

        business_object_definition_key = herdsdk.BusinessObjectDefinitionKey(
            namespace=namespace,
            business_object_definition_name=business_object_definition_name
        )

        upload_request = herdsdk.UploadBusinessObjectDefinitionSampleDataFileInitiationRequest(
            business_object_definition_key=business_object_definition_key
        )

        LOGGER.info('POST /upload/businessObjectDefinitionSampleDataFile/initiation')
        api_response = api_instance.uploadand_download_initiate_upload_sample_file(upload_request)
        return api_response

    ############################################################################
    def download_sample_file(self, namespace, business_object_definition_name, directory_path, file_name):
        api_client = ApiClientOverwrite(self.configuration)
        api_instance = herdsdk.UploadAndDownloadApi(api_client)

        business_object_definition_sample_data_file_key = herdsdk.BusinessObjectDefinitionSampleDataFileKey(
            namespace=namespace,
            business_object_definition_name=business_object_definition_name,
            directory_path=directory_path,
            file_name=file_name
        )

        download_request = herdsdk.DownloadBusinessObjectDefinitionSampleDataFileSingleInitiationRequest(
            business_object_definition_sample_data_file_key=business_object_definition_sample_data_file_key
        )

        LOGGER.info('POST /download/businessObjectDefinitionSampleDataFile/initiation')
        api_response = api_instance.uploadand_download_initiate_download_single_sample_file(download_request)
        return api_response

    ############################################################################
    def create_tag_type(self, tag_type_code, display_name, tag_type_order, description):
        api_instance = herdsdk.TagTypeApi(herdsdk.ApiClient(self.configuration))

        tag_type_key = herdsdk.TagTypeKey(
            tag_type_code=tag_type_code
        )
        tag_type_create_request = herdsdk.TagTypeCreateRequest(
            tag_type_key=tag_type_key,
            display_name=display_name,
            tag_type_order=tag_type_order,
            description=description
        )

        LOGGER.info('POST /tagTypes')
        api_response = api_instance.tag_type_create_tag_type(tag_type_create_request)
        return api_response

    ############################################################################
    def update_tag_type(self, tag_type_code, display_name, tag_type_order, description):
        api_instance = herdsdk.TagTypeApi(herdsdk.ApiClient(self.configuration))

        tag_type_update_request = herdsdk.TagTypeUpdateRequest(
            display_name=display_name,
            tag_type_order=tag_type_order,
            description=description,
        )

        LOGGER.info('PUT /tagTypes/{}'.format(tag_type_code))
        api_response = api_instance.tag_type_update_tag_type(tag_type_code, tag_type_update_request)
        return api_response

    ############################################################################
    def delete_tag_type(self, tag_type_code):
        api_instance = herdsdk.TagTypeApi(herdsdk.ApiClient(self.configuration))

        LOGGER.info('DELETE /tagTypes/{}'.format(tag_type_code))
        api_response = api_instance.tag_type_delete_tag_type(tag_type_code)
        return api_response

    ############################################################################
    def get_associated_tags(self, tag_type_code, tag_code=None):
        api_instance = herdsdk.TagApi(herdsdk.ApiClient(self.configuration))

        if tag_code:
            LOGGER.info('GET /tags/tagTypes/{}?tagCode={}'.format(tag_type_code, tag_code))
            api_response = api_instance.tag_get_tags(tag_type_code, tag_code=tag_code)
        else:
            LOGGER.info('GET /tags/tagTypes/{}'.format(tag_type_code))
            api_response = api_instance.tag_get_tags(tag_type_code)

        return api_response

    ############################################################################
    def get_tags(self, tag_type_code, tag_code):
        api_client = ApiClientOverwrite(self.configuration)
        api_instance = herdsdk.TagApi(api_client)

        LOGGER.info('GET /tags/tagTypes/{}/tagCodes/{}'.format(tag_type_code, tag_code))
        api_response = api_instance.tag_get_tag(tag_type_code, tag_code)
        return api_response

    ############################################################################
    def delete_tags(self, tag_type_code, tag_code):
        api_client = ApiClientOverwrite(self.configuration)
        api_instance = herdsdk.TagApi(api_client)

        LOGGER.info('DELETE /tags/tagTypes/{}/tagCodes/{}'.format(tag_type_code, tag_code))
        api_response = api_instance.tag_delete_tag(tag_type_code, tag_code)
        return api_response

    ############################################################################
    def create_tags(self, tag_type_code, tag_code, display_name, multiplier=None, description=None, parent_tag=None):
        api_client = ApiClientOverwrite(self.configuration)
        api_instance = herdsdk.TagApi(api_client)

        tag_key = herdsdk.TagKey(
            tag_type_code=tag_type_code,
            tag_code=tag_code
        )

        if parent_tag:
            parent_tag_key = herdsdk.TagKey(
                tag_type_code=parent_tag['tag_type_code'],
                tag_code=parent_tag['tag_code']
            )
        else:
            parent_tag_key = None

        tag_create_request = herdsdk.TagCreateRequest(
            tag_key=tag_key,
            display_name=display_name,
            search_score_multiplier=multiplier,
            description=description,
            parent_tag_key=parent_tag_key
        )

        LOGGER.info('POST /tags')
        api_response = api_instance.tag_create_tag(tag_create_request)
        return api_response

    ############################################################################
    def update_tags(self, tag_type_code, tag_code, display_name, multiplier=None, description=None, parent_tag=None):
        api_client = ApiClientOverwrite(self.configuration)
        api_instance = herdsdk.TagApi(api_client)

        if parent_tag:
            parent_tag_key = herdsdk.TagKey(
                tag_type_code=parent_tag['tag_type_code'],
                tag_code=parent_tag['tag_code']
            )
        else:
            parent_tag_key = None

        tag_update_request = herdsdk.TagUpdateRequest(
            display_name=display_name,
            search_score_multiplier=multiplier,
            description=description,
            parent_tag_key=parent_tag_key
        )

        LOGGER.info('PUT /tags/tagTypes/{}/tagCodes/{}'.format(tag_type_code, tag_code))
        api_response = api_instance.tag_update_tag(tag_type_code, tag_code, tag_update_request)
        return api_response

    ############################################################################
    def get_bdef_tags_by_tag(self, tag_type_code, tag_code):
        api_instance = herdsdk.BusinessObjectDefinitionTagApi(herdsdk.ApiClient(self.configuration))

        LOGGER.info('GET /businessObjectDefinitionTags/tagTypes/{}/tagCodes/{}'.format(tag_type_code, tag_code))
        api_response = api_instance.business_object_definition_tag_get_business_object_definition_tags_by_tag(
            tag_type_code, tag_code)
        return api_response

    ############################################################################
    def post_relational_table(self, post_request):
        api_instance = herdsdk.RelationalTableRegistrationApi(herdsdk.ApiClient(self.configuration))

        LOGGER.info('POST /relationalTableRegistrations?appendToExistingBusinessObjectDefinition={}'.format(
            post_request['append']))
        table_create_request = herdsdk.RelationalTableRegistrationCreateRequest(
            namespace=post_request['namespace'],
            business_object_definition_name=post_request['business_object_definition_name'],
            business_object_format_usage=post_request['business_object_format_usage'],
            data_provider_name=post_request['data_provider_name'],
            relational_schema_name=post_request['relational_schema_name'],
            relational_table_name=post_request['relational_table_name'],
            storage_name=post_request['storage_name']
        )
        api_response = api_instance.relational_table_registration_create_relational_table_registration(
            relational_table_registration_create_request=table_create_request,
            append_to_existing_business_object_definition=post_request['append']
        )
        return api_response


################################################################################
class ApiClientOverwrite(herdsdk.ApiClient):
    def deserialize(self, response, response_type):  # pragma: no cover
        """Deserializes response into an object.

        :param response: RESTResponse object to be deserialized.
        :param response_type: class literal for
            deserialized object, or string of class name.

        :return: deserialized object.
        """
        # handle file downloading
        # save response body into a tmp file and return the instance
        if response_type == "file":
            return self._ApiClient__deserialize_file(response)

        # fetch data from response object
        try:
            import json
            data = json.loads(response.data)
        except ValueError:
            data = response.data

        '''
        NOTE: Due to datetime parser issue, converting data
        '''
        for key in ['lastUpdatedOn', 'awsSessionExpirationTime', 'updatedTime']:
            if key in data:
                import time
                data[key] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(data[key] / 1000))

        return self._ApiClient__deserialize(data, response_type)
