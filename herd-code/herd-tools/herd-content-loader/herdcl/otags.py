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
import os, sys, configparser, base64, traceback, pprint, filecmp

# Third party imports
import pandas as pd

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
    action = None
    excel_file = ''
    sample_dir = ''
    data_frame = ''
    path = ''
    config = None

    # Configure HTTP basic authorization: basicAuthentication
    configuration = herdsdk.Configuration()

    # actions = [Menu.OBJECTS.value, Menu.COLUMNS.value, Menu.SAMPLES.value, Menu.TAGS.value, Menu.EXPORT.value]
    actions = [Menu.OBJECTS.value, Menu.COLUMNS.value, Menu.LINEAGE.value, Menu.SAMPLES.value]
    envs = Menu.ENVS.value

    def __init__(self):
        # Instance variables
        self.run_steps = []
        self.tag_types = {
            'columns': []
        }
        self.format_columns = {}
        self.run_summary = {
            'total_rows': 0,
            'success_rows': 0,
            'fail_rows': 0,
            'fail_index': [],
            Summary.CHANGES.value: [],
            Summary.WARNINGS.value: [],
            Summary.ERRORS.value: []
        }
        self.sample_files = {}

        self.acts = {
            str.lower(Menu.OBJECTS.value): self.load_object,
            str.lower(Menu.COLUMNS.value): self.load_columns,
            str.lower(Menu.LINEAGE.value): self.load_lineage,
            str.lower(Menu.SAMPLES.value): self.load_samples,
            str.lower(Menu.TAGS.value): self.load_tags,
            # str.lower(Menu.EXPORT.value): self.get_build_info,
            'test_api': self.test_api
        }

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

        :param config: Configuration file parser
        :type config: ConfigParser

        """
        if config['gui_enabled']:
            self.action = str.lower(config['action'])
            self.excel_file = config['excel_file']
            self.sample_dir = config['sample_dir']
            self.configuration.host = self.config.get('url', config['env'])
            self.configuration.username = config['userName']
            self.configuration.password = config['userPwd']
        else:
            self.action = str.lower(self.config.get('console', 'action'))
            if self.action in ['objects', 'columns', 'lineage']:
                self.excel_file = self.config.get('console', 'excelFile')
            elif self.action == 'samples':
                self.excel_file = self.config.get('console', 'excelFile')
                self.sample_dir = self.config.get('console', 'sampleDir')
            env = self.config.get('console', 'env')
            self.configuration.host = self.config.get('url', env)
            self.configuration.username = self.config.get('credentials', 'userName')
            self.configuration.password = base64.b64decode(self.config.get('credentials', 'userPwd')).decode('utf-8')

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
    def load_worksheet_tag_types(self):
        """
        Gets list of all tag types and compares with Excel worksheet columns

        """
        LOGGER.info('Getting list of all tag types')
        resp = self.get_tag_types().tag_type_keys
        LOGGER.info('Success')
        LOGGER.info(resp)

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
    def load_object(self):
        """
        One of the controller actions. Loads business object definitions

        :return: Run Summary dict

        """
        self.data_frame = self.load_worksheet(Objects.WORKSHEET.value)
        self.run_summary['total_rows'] = len(self.data_frame.index)
        self.load_worksheet_tag_types()

        self.run_steps = [
            self.update_bdef_descriptive_info,
            self.update_sme,
            self.update_bdef_tags
        ]

        for index, row in self.data_frame.iterrows():
            row_pass = True
            for step in self.run_steps:
                if row_pass:
                    try:
                        step(index, row)
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

        :return: Run Summary dict

        """
        self.data_frame = self.load_worksheet(Objects.WORKSHEET.value)
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
        One of the controller actions. Loads business object sample files

        :return: Run Summary dict

        """
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
            'total_rows': 0,
            'success_rows': 0,
            'fail_rows': 0,
            'fail_index': [],
            Summary.CHANGES.value: [],
            Summary.WARNINGS.value: [],
            Summary.ERRORS.value: []
        }
        self.sample_files = {}

    ############################################################################
    def update_run_summary_batch(self, index_array, message, category):
        """
        Updates run summary. Category can be changes, warnings, or errors

        :param index_array: List of int corresponding to row index in Excel worksheet
        :param message: Error message
        :param category: Type of message. Changes, warnings, or errors

        """
        if category == Summary.ERRORS.value:
            self.run_summary['fail_rows'] += len(index_array)
            self.run_summary['fail_index'].extend([i + 2 for i in index_array])
        for index in index_array:
            if index < 0:
                item = {
                    'index': index,
                    'message': message
                }
            else:
                item = {
                    'index': index + 2,
                    'message': message
                }
            self.run_summary[category].append(item)

    ############################################################################
    def update_bdef_descriptive_info(self, index, row):
        """
        Updates an existing business object definition descriptive information

        :param index: Row index in Excel worksheet
        :param row: A row inside the Pandas DataFrame

        """
        namespace, usage, file_type, bdef_name, logical_name, description = row[:6]
        LOGGER.info('Getting BDef for {}'.format((namespace, bdef_name)))
        resp = self.get_business_object_definition(namespace, bdef_name)
        LOGGER.info('Success')
        LOGGER.info(resp)

        # See if description, display name, usage, or file type in excel differs from UDC
        if not resp.descriptive_business_object_format:
            json = {
                'description': description,
                'displayName': logical_name,
                'formatUsage': usage,
                'fileType': file_type
            }
            LOGGER.info('Adding BDef Descriptive Info')
            resp = self.update_business_object_definition_descriptive_info(namespace=namespace,
                                                                           business_object_definition_name=bdef_name,
                                                                           update_request=json)
            LOGGER.info('Success')
            LOGGER.info(resp)
            message = 'Change in row. Old Descriptive Info:\nNone'.format(json)
            self.update_run_summary_batch([index], message, Summary.CHANGES.value)

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
            json = {
                'description': description,
                'displayName': logical_name,
                'formatUsage': usage,
                'fileType': file_type
            }
            LOGGER.info('Updating BDef Descriptive Info')
            resp = self.update_business_object_definition_descriptive_info(namespace=namespace,
                                                                           business_object_definition_name=bdef_name,
                                                                           update_request=json)
            LOGGER.info('Success')
            LOGGER.info(resp)
            message = 'Change in row. Old Descriptive Info:\n{}'.format(old_data)
            self.update_run_summary_batch([index], message, Summary.CHANGES.value)

    ############################################################################
    def update_sme(self, index, row):
        """
        Updates existing business object definition subject matter experts for a specific business object definition

        :param index: Row index in Excel worksheet
        :param row: A row inside the Pandas DataFrame

        """
        namespace, _, _, bdef_name = row[:4]

        LOGGER.info('Getting SME for {}'.format((namespace, bdef_name)))
        resp = self.get_subject_matter_experts(namespace, bdef_name)
        LOGGER.info('Success')
        LOGGER.info(resp)

        user = row[Objects.SME.value]
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

        LOGGER.info('Current expert list: {}'.format(', '.join(current_smes)))
        row_change = False

        for sme in remove_sme_list:
            user_id = '{}{}{}rp.{}.{}sd.{}'.format(sme, chr(64), 'co', 'root', 'na', 'com')
            LOGGER.info('Deleting SME: {}'.format(sme))
            self.delete_subject_matter_expert(namespace, bdef_name, user_id)
            LOGGER.info('SME deleted')
            row_change = True

        if user:
            for user_id in user:
                if not '@' in user_id:
                    user_id = '{}{}{}rp.{}.{}sd.{}'.format(user_id, chr(64), 'co', 'root', 'na', 'com')
                LOGGER.info('Adding SME: {}'.format(user_id))
                self.create_subject_matter_expert(namespace, bdef_name, user_id)
                LOGGER.info('SME Added')
                row_change = True

        if row_change:
            message = 'Change in row. Old SME list:\n{}'.format(', '.join(current_smes))
            self.update_run_summary_batch([index], message, Summary.CHANGES.value)

    ############################################################################
    def update_bdef_tags(self, index, row):
        """
        Updates business object definition tags

        :param index: Row index in Excel worksheet
        :param row: A row inside the Pandas DataFrame

        """
        namespace, _, _, bdef_name = row[:4]

        LOGGER.info('Checking worksheet for BDef tags to add')
        tags_to_add = {}
        for code in self.tag_types['columns']:
            row_entry = []
            display_name = self.tag_types[code]
            if row[display_name]:
                LOGGER.info('Tag data found in column \'{}\''.format(display_name))
                row_entry = [x.strip() for x in row[display_name].split(',')]
            tags_to_add[code] = row_entry

        LOGGER.info('Tags in worksheet: {}'.format(tags_to_add))
        LOGGER.info('Getting Current Bdef Tags')
        row_change = False
        old_tags = {}
        resp = self.get_bdef_tags(namespace, bdef_name)
        for bdef_tag in resp.business_object_definition_tag_keys:
            tag_key = bdef_tag.tag_key
            LOGGER.info('Found Tag Key: {}'.format(tag_key))
            tag_type_code, tag_code = tag_key.tag_type_code, tag_key.tag_code
            old_tags[tag_type_code] = tag_code
            if tag_type_code in tags_to_add and tag_code in tags_to_add[tag_type_code]:
                tags_to_add[tag_type_code].remove(tag_code)
            else:
                LOGGER.info('Deleting Tag Key: {}'.format(tag_key))
                self.delete_bdef_tags(namespace, bdef_name, tag_type_code, tag_code)
                LOGGER.info('Deleted')
                row_change = True

        for tag_type_code, row_entry in tags_to_add.items():
            for tag_code in row_entry:
                LOGGER.info('Adding {}'.format(tag_code))
                self.create_bdef_tags(namespace, bdef_name, tag_type_code, tag_code)
                LOGGER.info('Added')
                row_change = True

        if row_change:
            message = 'Change in row. Old tags:\n{}'.format(old_tags)
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
            LOGGER.info('Getting BDef')
            resp = self.get_business_object_definition(namespace, bdef_name)
            if not resp.descriptive_business_object_format:
                message = 'No Descriptive Format defined for {}'.format(key)
                LOGGER.error(message)
                self.update_run_summary_batch(index_array, message, Summary.ERRORS.value)
                return

            LOGGER.info('Success')
            LOGGER.info(resp.descriptive_business_object_format)
            LOGGER.info('Getting Format')
            format_resp = self.get_format(namespace, bdef_name,
                                          resp.descriptive_business_object_format.business_object_format_usage,
                                          resp.descriptive_business_object_format.business_object_format_file_type,
                                          resp.descriptive_business_object_format.business_object_format_version)
            if not (format_resp.schema and format_resp.schema.columns):
                message = 'No Schema Columns found for {}'.format(key)
                LOGGER.error(message)
                self.update_run_summary_batch(index_array, message, Summary.ERRORS.value)
                return

            # Get schema columns and bdef columns as dataframes. Merge the two to check if both contain schema name
            LOGGER.info('Success')
            schema_df = pd.DataFrame(
                [{Columns.SCHEMA_NAME.value: str.upper(x.name).strip()} for x in format_resp.schema.columns])
            LOGGER.info('Getting BDef Columns')
            col_resp = self.post_bdef_column_search(namespace, bdef_name)
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

            # Delete column with no schema name
            LOGGER.info('Checking for column names with no schema name')
            empty_schema_filter = self.format_columns[key][Columns.SCHEMA_NAME.value] == ''
            empty_schema_df = self.format_columns[key][empty_schema_filter]
            self.format_columns[key] = self.format_columns[key][~empty_schema_filter]
            for index, row in empty_schema_df.iterrows():
                try:
                    LOGGER.warning(
                        'Schema Name not found. Deleting Column Name: {}'.format(row[Columns.COLUMN_NAME.value]))
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
                message = 'Could not find a schema name for the following columns:\n{}'.format(
                    pprint.pformat(empty_schema_list, width=120, compact=True))
                self.update_run_summary_batch([ERROR_CODE], message, Summary.WARNINGS.value)

            # Compare excel data to UDC data
            LOGGER.info('Comparing Excel worksheet with UDC data')
            for index in index_array:
                row_change = False
                try:
                    xls_schema_name = str.upper(self.data_frame.at[index, Columns.SCHEMA_NAME.value]).strip()
                    xls_column_name = self.data_frame.at[index, Columns.COLUMN_NAME.value]
                    xls_description = self.data_frame.at[index, Columns.DESCRIPTION.value]

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
                        if not row['Found']:
                            LOGGER.info('Adding bdef column name: {}'.format(xls_column_name))
                            self.create_bdef_column(namespace, bdef_name, xls_column_name, xls_schema_name,
                                                    xls_description)
                            LOGGER.info('Success')
                            self.format_columns[key].at[i, 'Found'] = True
                            row_change = True
                        elif column_name != xls_column_name or description != xls_description:
                            LOGGER.info('Changing bdef column name: {}'.format(xls_column_name))
                            self.delete_bdef_column(namespace, bdef_name, row[Columns.COLUMN_NAME.value])
                            self.create_bdef_column(namespace, bdef_name, xls_column_name, xls_schema_name,
                                                    xls_description)
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
                        self.update_run_summary_batch([index], message, Summary.CHANGES.value)

                except ApiException as e:
                    LOGGER.error(e)
                    self.update_run_summary_batch([index], e, Summary.ERRORS.value)
                except Exception:
                    LOGGER.error(traceback.format_exc())
                    self.update_run_summary_batch([index], traceback.format_exc(), Summary.ERRORS.value)

            not_found_filter = self.format_columns[key]['Found'] == False
            not_found_df = self.format_columns[key][not_found_filter]
            not_found_list = not_found_df[Columns.SCHEMA_NAME.value].tolist()
            if len(not_found_list) > 0:
                message = 'Could not find column info for the following schema columns:\n{}'.format(
                    pprint.pformat(not_found_list, width=120, compact=True))
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
        LOGGER.info('Success')

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
            if list(empty_df.index.values) == list(df.index.values):
                if format_parents:
                    LOGGER.info('Removing all parents')
                    self.update_format_parents(namespace, bdef_name, usage, file_type, [])
                    LOGGER.info('Success')

                    message = 'Change in rows: {}\nAll parents removed. Old Parents:\n{}'.format(
                        empty_df.index.tolist(), format_parents)
                    self.update_run_summary_batch([empty_df.index[0]], message, Summary.CHANGES.value)
                else:
                    LOGGER.info('No parent changes made')
                self.run_summary['success_rows'] += len(df.index)
                return
            else:
                skip_found = True

        if skip_found:
            message = 'Mix of empty and nonempty rows found. Skipping empty rows: {}'.format(empty_df.index.tolist())
            self.update_run_summary_batch([empty_df.index[0]], message, Summary.WARNINGS.value)

        xls_parent_list = []

        filled_df.apply(lambda row: xls_parent_list.append({
            Lineage.NAMESPACE.value: str.upper(row[parent_columns[0]]).strip(),
            Lineage.DEFINITION_NAME.value: str.upper(row[parent_columns[1]]).strip(),
            Lineage.USAGE.value: str.upper(row[parent_columns[2]]).strip(),
            Lineage.FILE_TYPE.value: str.upper(row[parent_columns[3]]).strip()
        }), 1)

        if format_parents:
            format_parents, xls_parent_list = [
                sorted(l, key=lambda x: (x[columns[0]], x[columns[1]], x[columns[2]], x[columns[3]]))
                for l in (format_parents, xls_parent_list)
            ]
            pairs = zip(format_parents, xls_parent_list)
            if all(x == y for x, y in pairs):
                LOGGER.info('No parent changes made')
                self.run_summary['success_rows'] += len(df.index)
                return

        LOGGER.info('Updating parents')
        self.update_format_parents(namespace, bdef_name, usage, file_type, xls_parent_list)
        LOGGER.info('Success')
        message = 'Change in rows: {}\nUpdated parents. Old Parents:\n{}'.format(filled_df.index.tolist(),
                                                                                 format_parents)
        self.update_run_summary_batch([filled_df.index[0]], message, Summary.CHANGES.value)
        self.run_summary['success_rows'] += len(df.index)

    ############################################################################
    def check_sample_files(self):
        """
        Checks Excel worksheet for rows with no sample file

        """
        LOGGER.info('Checking samples worksheet for empty values')
        empty_sample_filter = self.data_frame[Objects.SAMPLE.value] == ''
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
            LOGGER.info('Getting BDef for {}'.format(key))
            resp = self.get_business_object_definition(namespace, bdef_name)
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
                    file = self.data_frame.at[index, Objects.SAMPLE.value]
                    path = self.sample_dir + os.sep + file

                    if not os.path.exists(path):
                        message = 'File not found. Please double check path: {}'.format(path)
                        LOGGER.error(message)
                        self.update_run_summary_batch([index], message, Summary.ERRORS.value)
                        continue

                    if file in uploaded_files:
                        LOGGER.info('File already uploaded. Skipping: {}'.format(file))
                        self.run_summary['success_rows'] += 1
                        continue

                    if file in sample_files:
                        LOGGER.info('Matched File: {}\nChecking if contents changed'.format(file))

                        LOGGER.info('Getting download request')
                        download_resp = self.download_sample_file(namespace, bdef_name, sample_files[file], file)
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
                    self.update_run_summary_batch([index], message, Summary.CHANGES.value)

                except ApiException as e:
                    LOGGER.error(e)
                    self.update_run_summary_batch([index], e, Summary.ERRORS.value)
                except Exception:
                    LOGGER.error(traceback.format_exc())
                    self.update_run_summary_batch([index], traceback.format_exc(), Summary.ERRORS.value)

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
    def test_api(self):
        """
        One of the controller actions. Calls Get Build Info. Quick way to check api

        :return: Run Summary dict

        """
        self.run_summary['total_rows'] = 1
        self.get_current_user()
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
        api_instance = herdsdk.CurrentUserApi(herdsdk.ApiClient(self.configuration))

        api_response = api_instance.current_user_get_current_user()
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
        for key in ['lastUpdatedOn', 'awsSessionExpirationTime']:
            if key in data:
                import time
                data[key] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(data[key] / 1000))

        return self._ApiClient__deserialize(data, response_type)
