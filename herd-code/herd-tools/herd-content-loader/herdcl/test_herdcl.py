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
import configparser, string, random
import unittest
from unittest import mock

# Third party imports
import xlrd
import pandas as pd

# Herd imports
from herdsdk import rest

# Local imports
try:
    import otags
    from constants import Objects, Columns
except ImportError:
    from herdcl import otags
    from herdcl.constants import Objects, Columns


def string_generator(string_length=10):
    """Generate a random string of letters, digits and special characters """

    password_characters = string.ascii_letters + string.digits + string.punctuation.replace(',', '').replace('@', '')
    new_string = ''.join(random.choice(password_characters) for _ in range(string_length))
    rand = random.randint(161, 563)
    return new_string + chr(rand)


class TestUtilityMethods(unittest.TestCase):
    """
    Test Suite for Utility Methods
    """

    def setUp(self):
        """
        The setup method that will be called before each test.
        """
        self.controller = otags.Controller()

    @mock.patch('os.path')
    @mock.patch('configparser.ConfigParser')
    def test_load_config(self, mock_config, mock_path):
        """
        Test of the load config function

        """
        # Mock config. Check configparser.ConfigParser() is called
        mock_path.exists.return_value = True
        self.controller.load_config()
        mock_config.assert_called_once()

    @mock.patch('os.path')
    def test_load_config_no_file(self, mock_path):
        """
        Test of the load config function with no config file

        """
        # Mocks that the config file doesn't exist, raising a FileNotFoundError
        mock_path.exists.return_value = False
        with self.assertRaises(FileNotFoundError):
            self.controller.load_config()

    @mock.patch('configparser.ConfigParser')
    def test_setup_run_gui(self, mock_config):
        """
        Test of the setup config function for GUI app

        """

        config = {
            'gui_enabled': True,
            'env': 'testenv',
            'action': 'SaMpLes',
            'excel_file': 'testfile',
            'sample_dir': 'testdir',
            'userName': 'testuser',
            'userPwd': 'testpwd'
        }

        # Mock config
        self.controller.config = mock_config

        # Run scenario and check values
        self.controller.setup_run(config)
        mock_config.get.assert_called_once()
        self.assertEqual(self.controller.action, str.lower(config['action']))
        self.assertEqual(self.controller.excel_file, config['excel_file'])
        self.assertEqual(self.controller.sample_dir, config['sample_dir'])
        self.assertEqual(self.controller.configuration.username, config['userName'])
        self.assertEqual(self.controller.configuration.password, config['userPwd'])

    @mock.patch('configparser.ConfigParser')
    def test_setup_run_console(self, mock_config):
        """
        Test of the setup config function for Console app

        """

        config = {
            'gui_enabled': False
        }

        # Mock config.get so each call returns a different value
        test_vars = ['SaMpLes', 'testexcel', 'testdir', 'testenv', 'testurl', 'testusername', 'dGVzdHBhc3N3b3Jk']
        mock_config.get.side_effect = test_vars
        self.controller.config = mock_config
        self.controller.setup_run(config)

        # Run scenario and check values
        self.assertEqual(mock_config.get.call_count, 7)
        self.assertEqual(self.controller.action, str.lower(test_vars[0]))
        self.assertEqual(self.controller.excel_file, test_vars[1])
        self.assertEqual(self.controller.sample_dir, test_vars[2])
        self.assertEqual(self.controller.configuration.host, test_vars[4])
        self.assertEqual(self.controller.configuration.username, test_vars[5])
        self.assertEqual(self.controller.configuration.password, 'testpassword')

    def test_setup_run_console_missing_config_section(self):
        """
        Test of the setup config function for Console app if no config section is found

        """

        config = {
            'gui_enabled': False,
        }

        self.controller.config = configparser.ConfigParser()

        # Run scenario and check values
        with self.assertRaises(configparser.NoSectionError):
            self.controller.setup_run(config)

    def test_setup_run_console_missing_config_key(self):
        """
        Test of the setup config function for Console app if no config key is found

        """

        config = {
            'gui_enabled': False,
        }

        self.controller.config = configparser.ConfigParser()
        self.controller.config.add_section('console')

        # Run scenario and check values
        with self.assertRaises(configparser.NoOptionError):
            self.controller.setup_run(config)

    def test_get_key(self):
        """
        Test making sure self.acts keys exist

        """
        for act in self.controller.actions:
            self.controller.action = str.lower(act)
            self.controller.get_action()

    def test_load_worksheet_no_file(self):
        """
        Test of the load worksheet with no file found

        """
        # Run scenario and check values
        with self.assertRaises(FileNotFoundError):
            self.controller.load_worksheet('Sheet')

    @mock.patch('pandas.read_excel')
    def test_load_worksheet_no_sheet(self, mock_pd):
        """
        Test of the load worksheet with no worksheet found

        """
        mock_pd.side_effect = xlrd.biffh.XLRDError()

        # Run scenario and check values
        with self.assertRaises(xlrd.biffh.XLRDError):
            self.controller.load_worksheet('Sheet')


class TestObjectAction(unittest.TestCase):
    """
    Test Suite for Action Objects
    """

    def setUp(self):
        """
        The setup method that will be called before each test.
        """
        self.controller = otags.Controller()

    def test_load_object(self):
        """
        Test of the main load object action

        """
        self.controller.load_worksheet = mock.Mock(
            return_value=pd.DataFrame(data=[['item1']], columns=['column1'])
        )
        self.controller.load_worksheet_tag_types = mock.Mock()
        self.controller.update_bdef_descriptive_info = mock.Mock()
        self.controller.update_sme = mock.Mock()
        self.controller.update_bdef_tags = mock.Mock()

        # Run scenario and check values
        self.controller.load_object()
        self.assertEqual(self.controller.run_summary['total_rows'], 1)
        self.assertEqual(self.controller.run_summary['success_rows'] + self.controller.run_summary['fail_rows'],
                         self.controller.run_summary['total_rows'])
        self.assertEqual(self.controller.run_summary['success_rows'], 1)
        self.assertEqual(self.controller.run_summary['fail_rows'], 0)
        self.assertEqual(self.controller.run_summary['fail_index'], [])
        self.assertEqual(len(self.controller.run_summary['errors']), 0)

    def test_load_object_exception(self):
        """
        Test of the main load object action with exceptions

        """
        self.controller.load_worksheet = mock.Mock(
            return_value=pd.DataFrame(data=[['item1'], ['item2'], ['item3']], columns=['column1'])
        )
        self.controller.load_worksheet_tag_types = mock.Mock()
        self.controller.update_bdef_descriptive_info = mock.Mock(
            side_effect=[mock.DEFAULT, rest.ApiException(reason='Error'), mock.DEFAULT]
        )
        self.controller.update_sme = mock.Mock(
            side_effect=[Exception('Exception Thrown 1'), mock.DEFAULT]
        )
        self.controller.update_bdef_tags = mock.Mock()

        # Run scenario and check values
        self.controller.load_object()
        self.assertEqual(self.controller.run_summary['total_rows'], 3)
        self.assertEqual(self.controller.run_summary['success_rows'] + self.controller.run_summary['fail_rows'],
                         self.controller.run_summary['total_rows'])
        self.assertEqual(self.controller.run_summary['success_rows'], 1)
        self.assertEqual(self.controller.run_summary['fail_rows'], 2)
        self.assertEqual(self.controller.run_summary['fail_index'], [2, 3])
        self.assertEqual(len(self.controller.run_summary['errors']), 2)
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary['errors'][0]['message'])
        self.assertTrue('Reason: Error' in str(self.controller.run_summary['errors'][1]['message']))

    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_types')
    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_type')
    def test_load_worksheet_tag_types(self, mock_tag_api, mock_tag_types_api):
        """
        Test of loading tag types

        """
        code_1 = string_generator()
        code_2 = string_generator()

        column_1 = string_generator()
        column_2 = string_generator() + 'A'

        mock_tag_types_api.return_value = mock.Mock(
            tag_type_keys=[
                mock.Mock(tag_type_code=code_1),
                mock.Mock(tag_type_code=code_2)
            ]
        )

        # Also checking that leading and trailing characters are removed
        # Currently case-sensitive
        mock_tag_api.side_effect = [
            mock.Mock(display_name='  ' + column_1 + '   '),
            mock.Mock(display_name=column_2)
        ]

        self.controller.data_frame = pd.DataFrame(data=[['item1', 'item2']],
                                                  columns=[column_1, str.lower(column_2)])

        # Run scenario and check values
        self.controller.load_worksheet_tag_types()
        mock_tag_types_api.assert_called_once()
        self.assertEqual(mock_tag_api.call_count, 2)
        self.assertEqual(self.controller.tag_types['columns'], [code_1])

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_update_business_object_definition_descriptive_information')
    def test_update_bdef_descriptive_info(self, mock_descr_info, mock_bdef):
        """
        Test of updating business object definition descriptive info

        """
        mock_bdef.return_value = mock.Mock(
            description=string_generator(string_length=10),
            descriptive_business_object_format=mock.Mock(
                business_object_format_file_type=string_generator(string_length=11),
                business_object_format_usage=string_generator(string_length=12)
            ),
            display_name=string_generator(string_length=13)
        )
        row = ['namespace', string_generator(), string_generator(), 'bdef_name', string_generator(), string_generator()]

        # Run scenario and check values
        self.controller.update_bdef_descriptive_info(row)
        mock_bdef.assert_called_once()
        mock_descr_info.assert_called_once()

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_update_business_object_definition_descriptive_information')
    def test_update_bdef_descriptive_info_no_update(self, mock_descr_info, mock_bdef):
        """
        Test of no update to business object definition descriptive info

        """
        mock_bdef.return_value = mock.Mock(
            description='description',
            descriptive_business_object_format=mock.Mock(
                business_object_format_file_type='file_type',
                business_object_format_usage='usage'
            ),
            display_name='logical_name',
            namespace='namespace'
        )
        row = ['namespace', 'usage', 'file_type', 'bdef_name', 'logical_name', 'description']

        # Run scenario and check values
        self.controller.update_bdef_descriptive_info(row)
        mock_bdef.assert_called_once()
        self.assertEqual(mock_descr_info.get.call_count, 0)

    @mock.patch(
        'herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi.'
        'business_object_definition_subject_matter_expert_get_business_object_definition_subject_matter_experts_by_business_object_definition')
    @mock.patch(
        'herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi.'
        'business_object_definition_subject_matter_expert_delete_business_object_definition_subject_matter_expert')
    @mock.patch(
        'herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi.'
        'business_object_definition_subject_matter_expert_create_business_object_definition_subject_matter_expert')
    def test_update_sme(self, mock_create_sme, mock_delete_sme, mock_get_sme):
        """
        Test of updating business object definition subject matter expert

        """
        mock_get_sme.return_value = mock.Mock(
            business_object_definition_subject_matter_expert_keys=[
                mock.Mock(user_id=string_generator(string_length=9))
            ]
        )

        df = pd.DataFrame(data=[['item1', 'item2', 'item3', 'item4', string_generator()]],
                          columns=['column1', 'column2', 'column3', 'column4', Objects.SME.value])
        row = df.iloc[0]

        # Run scenario and check values
        self.controller.update_sme(row)
        mock_get_sme.assert_called_once()
        mock_delete_sme.assert_called_once()
        mock_create_sme.assert_called_once()

    @mock.patch(
        'herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi.'
        'business_object_definition_subject_matter_expert_get_business_object_definition_subject_matter_experts_by_business_object_definition')
    @mock.patch(
        'herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi.'
        'business_object_definition_subject_matter_expert_delete_business_object_definition_subject_matter_expert')
    @mock.patch(
        'herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi.'
        'business_object_definition_subject_matter_expert_create_business_object_definition_subject_matter_expert')
    def test_update_sme_no_delete(self, mock_create_sme, mock_delete_sme, mock_get_sme):
        """
        Test of updating business object definition subject matter expert. No user deleted

        """
        mock_get_sme.return_value = mock.Mock(
            business_object_definition_subject_matter_expert_keys=[
                mock.Mock(user_id='user')
            ]
        )

        df = pd.DataFrame(data=[['item1', 'item2', 'item3', 'item4', 'user, ' + string_generator()]],
                          columns=['column1', 'column2', 'column3', 'column4', Objects.SME.value])
        row = df.iloc[0]

        # Run scenario and check values
        self.controller.update_sme(row)
        mock_get_sme.assert_called_once()
        self.assertEqual(mock_delete_sme.call_count, 0)
        mock_create_sme.assert_called_once()

    @mock.patch(
        'herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi.'
        'business_object_definition_subject_matter_expert_get_business_object_definition_subject_matter_experts_by_business_object_definition')
    @mock.patch(
        'herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi.'
        'business_object_definition_subject_matter_expert_delete_business_object_definition_subject_matter_expert')
    @mock.patch(
        'herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi.'
        'business_object_definition_subject_matter_expert_create_business_object_definition_subject_matter_expert')
    def test_update_sme_no_create(self, mock_create_sme, mock_delete_sme, mock_get_sme):
        """
        Test of updating business object definition subject matter expert. No new user created

        """
        mock_get_sme.return_value = mock.Mock(
            business_object_definition_subject_matter_expert_keys=[
                mock.Mock(user_id=string_generator())
            ]
        )

        df = pd.DataFrame(data=[['item1', 'item2', 'item3', 'item4', '']],
                          columns=['column1', 'column2', 'column3', 'column4', Objects.SME.value])
        row = df.iloc[0]

        # Run scenario and check values
        self.controller.update_sme(row)
        mock_get_sme.assert_called_once()
        mock_delete_sme.assert_called_once()
        self.assertEqual(mock_create_sme.call_count, 0)

    @mock.patch(
        'herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi.'
        'business_object_definition_subject_matter_expert_get_business_object_definition_subject_matter_experts_by_business_object_definition')
    @mock.patch(
        'herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi.'
        'business_object_definition_subject_matter_expert_delete_business_object_definition_subject_matter_expert')
    @mock.patch(
        'herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi.'
        'business_object_definition_subject_matter_expert_create_business_object_definition_subject_matter_expert')
    def test_update_sme_no_update(self, mock_create_sme, mock_delete_sme, mock_get_sme):
        """
        Test of no update to business object definition subject matter expert

        """

        mock_get_sme.return_value = mock.Mock(
            business_object_definition_subject_matter_expert_keys=[
                mock.Mock(user_id='user@something.com')
            ]
        )

        df = pd.DataFrame(data=[['item1', 'item2', 'item3', 'item4', 'user']],
                          columns=['column1', 'column2', 'column3', 'column4', Objects.SME.value])
        row = df.iloc[0]

        # Run scenario and check values
        self.controller.update_sme(row)
        mock_get_sme.assert_called_once()
        self.assertEqual(mock_delete_sme.call_count, 0)
        self.assertEqual(mock_create_sme.call_count, 0)

    @mock.patch(
        'herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi.'
        'business_object_definition_subject_matter_expert_get_business_object_definition_subject_matter_experts_by_business_object_definition')
    @mock.patch(
        'herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi.'
        'business_object_definition_subject_matter_expert_delete_business_object_definition_subject_matter_expert')
    @mock.patch(
        'herdsdk.BusinessObjectDefinitionSubjectMatterExpertApi.'
        'business_object_definition_subject_matter_expert_create_business_object_definition_subject_matter_expert')
    def test_update_sme_special_characters(self, mock_create_sme, mock_delete_sme, mock_get_sme):
        """
        Test of updating business object definition subject matter expert with special characters

        """
        mock_get_sme.return_value = mock.Mock(
            business_object_definition_subject_matter_expert_keys=[
                mock.Mock(user_id='–user_deleted')
            ]
        )

        users = ', '.join([string_generator(), string_generator(), '‘–’', u'\xa0', u'\u2026', u'\u2014'])
        df = pd.DataFrame(data=[['item1', 'item2', 'item3', 'item4', users]],
                          columns=['column1', 'column2', 'column3', 'column4', Objects.SME.value])
        row = df.iloc[0]

        # Run scenario and check values
        self.controller.update_sme(row)
        mock_get_sme.assert_called_once()
        mock_delete_sme.assert_called_once()
        self.assertEqual(mock_create_sme.call_count, 6)

    @mock.patch('herdsdk.BusinessObjectDefinitionTagApi.'
                'business_object_definition_tag_get_business_object_definition_tags_by_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectDefinitionTagApi.'
                'business_object_definition_tag_delete_business_object_definition_tag')
    @mock.patch('herdsdk.BusinessObjectDefinitionTagApi.'
                'business_object_definition_tag_create_business_object_definition_tag')
    def test_update_bdef_tags(self, mock_create_tag, mock_delete_tag, mock_get_bdef_tag):
        """
        Test of updating business object definition tags

        """
        tag_type_code_1 = string_generator()
        tag_type_code_2 = string_generator()

        tag_code_1 = string_generator()
        mock_get_bdef_tag.return_value = mock.Mock(
            business_object_definition_tag_keys=[
                mock.Mock(tag_key=mock.Mock(
                    tag_type_code=tag_type_code_1,
                    tag_code='tag1'
                )),
                mock.Mock(tag_key=mock.Mock(
                    tag_type_code=tag_type_code_2,
                    tag_code=tag_code_1
                ))
            ]
        )

        df = pd.DataFrame(data=[['item1', 'item2', 'item3', 'item4', 'tag1, tag2 ']],
                          columns=['Column1', 'column2', 'column3', 'column4', 'Display Name 1'])
        row = df.iloc[0]

        # Tag Type Code has a corresponding Excel Worksheel Display Column Name
        # Inside the column are comma separated tag codes
        self.controller.tag_types['columns'] = [tag_type_code_1]
        self.controller.tag_types[tag_type_code_1] = 'Display Name 1'

        # Run scenario and check values
        self.controller.update_bdef_tags(row)
        mock_get_bdef_tag.assert_called_once()
        mock_delete_tag.assert_called_once()
        mock_create_tag.assert_called_once()

    @mock.patch('herdsdk.BusinessObjectDefinitionTagApi.'
                'business_object_definition_tag_get_business_object_definition_tags_by_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectDefinitionTagApi.'
                'business_object_definition_tag_delete_business_object_definition_tag')
    @mock.patch('herdsdk.BusinessObjectDefinitionTagApi.'
                'business_object_definition_tag_create_business_object_definition_tag')
    def test_update_bdef_tags_no_delete(self, mock_create_tag, mock_delete_tag, mock_get_bdef_tag):
        """
        Test of updating business object definition tags. Tag will be created but no deletion

        """
        tag_type_code_1 = string_generator()
        tag_type_code_2 = string_generator()

        tag_code_1 = string_generator()
        mock_get_bdef_tag.return_value = mock.Mock(
            business_object_definition_tag_keys=[
                mock.Mock(tag_key=mock.Mock(
                    tag_type_code=tag_type_code_2,
                    tag_code=tag_code_1
                ))
            ]
        )

        df = pd.DataFrame(data=[['item1', 'item2', 'item3', 'item4', 'tag1, tag2 ', tag_code_1]],
                          columns=['Column1', 'column2', 'column3', 'column4', 'Display Name 1', 'Display Name 2'])
        row = df.iloc[0]

        # Tag Type Code has a corresponding Excel Worksheel Display Column Name
        # Inside the column are comma separated tag codes
        self.controller.tag_types['columns'] = [tag_type_code_1, tag_type_code_2]
        self.controller.tag_types[tag_type_code_1] = 'Display Name 1'
        self.controller.tag_types[tag_type_code_2] = 'Display Name 2'

        # Run scenario and check values
        self.controller.update_bdef_tags(row)
        mock_get_bdef_tag.assert_called_once()
        self.assertEqual(mock_delete_tag.call_count, 0)
        self.assertEqual(mock_create_tag.call_count, 2)

    @mock.patch('herdsdk.BusinessObjectDefinitionTagApi.'
                'business_object_definition_tag_get_business_object_definition_tags_by_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectDefinitionTagApi.'
                'business_object_definition_tag_delete_business_object_definition_tag')
    @mock.patch('herdsdk.BusinessObjectDefinitionTagApi.'
                'business_object_definition_tag_create_business_object_definition_tag')
    def test_update_bdef_tags_no_create(self, mock_create_tag, mock_delete_tag, mock_get_bdef_tag):
        """
        Test of updating business object definition tags. Tag will be deleted but no creation

        """
        tag_type_code_1 = string_generator()
        tag_type_code_2 = string_generator()

        tag_code_1 = string_generator()
        mock_get_bdef_tag.return_value = mock.Mock(
            business_object_definition_tag_keys=[
                mock.Mock(tag_key=mock.Mock(
                    tag_type_code=tag_type_code_1,
                    tag_code='tag1'
                )),
                mock.Mock(tag_key=mock.Mock(
                    tag_type_code=tag_type_code_1,
                    tag_code='tag2'
                )),
                mock.Mock(tag_key=mock.Mock(
                    tag_type_code=tag_type_code_2,
                    tag_code=tag_code_1
                ))
            ]
        )

        df = pd.DataFrame(data=[['item1', 'item2', 'item3', 'item4', 'tag1, tag2 ']],
                          columns=['Column1', 'column2', 'column3', 'column4', 'Display Name 1'])
        row = df.iloc[0]

        # Tag Type Code has a corresponding Excel Worksheel Display Column Name
        # Inside the column are comma separated tag codes
        self.controller.tag_types['columns'] = [tag_type_code_1]
        self.controller.tag_types[tag_type_code_1] = 'Display Name 1'

        # Run scenario and check values
        self.controller.update_bdef_tags(row)
        mock_get_bdef_tag.assert_called_once()
        mock_delete_tag.assert_called_once()
        self.assertEqual(mock_create_tag.call_count, 0)


class TestColumnAction(unittest.TestCase):
    """
    Test Suite for Action Column
    """

    def setUp(self):
        """
        The setup method that will be called before each test.
        """
        self.controller = otags.Controller()

    def test_load_columns(self):
        """
        Test of the main load column action

        """
        self.controller.load_worksheet = mock.Mock(
            return_value=pd.DataFrame(data=[['namespace', 'definition']], columns=[Columns.NAMESPACE.value, Columns.DEFINITION_NAME.value])
        )
        self.controller.check_format_schema_columns = mock.Mock()
        self.controller.get_bdef_columns = mock.Mock()
        self.controller.update_bdef_columns = mock.Mock()

        # Run scenario and check values
        self.controller.load_columns()
        self.assertEqual(self.controller.run_summary['total_rows'], 1)

    def test_load_columns_exception(self):
        """
        Test of the main load column action with exceptions

        """
        self.controller.load_worksheet = mock.Mock(
            return_value=pd.DataFrame(
                data=[['namespace1', 'definition1', '', ''], ['namespace2', 'definition2', '', '']],
                columns=[
                    Columns.NAMESPACE.value, Columns.DEFINITION_NAME.value, Columns.SCHEMA_NAME.value,
                    Columns.COLUMN_NAME.value
                ]
                )
        )

        self.controller.check_format_schema_columns = mock.Mock()
        self.controller.get_business_object_definition = mock.Mock(
            side_effect=[rest.ApiException(reason='Error'), Exception('Exception Thrown 2')]
        )
        self.controller.delete_bdef_column = mock.Mock()

        # Run scenario and check values
        self.controller.load_columns()
        self.assertEqual(self.controller.run_summary['total_rows'], 2)
        self.assertEqual(self.controller.run_summary['success_rows'] + self.controller.run_summary['fail_rows'],
                         self.controller.run_summary['total_rows'])
        self.assertEqual(self.controller.run_summary['success_rows'], 0)
        self.assertEqual(self.controller.run_summary['fail_rows'], 2)
        self.assertEqual(self.controller.run_summary['fail_index'], [2, 3])
        self.assertEqual(len(self.controller.run_summary['errors']), 2)
        self.assertTrue('Reason: Error' in str(self.controller.run_summary['errors'][0]['message']))
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary['errors'][1]['message'])

    def test_check_format_schema_columns(self):
        """
        Test of checking Excel worksheet for empty cells

        """
        self.controller.data_frame = pd.DataFrame(
            data=[[string_generator(), string_generator()], [string_generator(), ''], ['', string_generator()],
                  ['', '']],
            columns=[Columns.SCHEMA_NAME.value, Columns.COLUMN_NAME.value])

        # Run scenario and check values
        self.controller.check_format_schema_columns()
        self.assertEqual(self.controller.run_summary['fail_rows'], 3)
        self.assertEqual(self.controller.run_summary['fail_index'], [3, 4, 5])

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectFormatApi.'
                'business_object_format_get_business_object_format')
    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_search_business_object_definition_columns')
    def test_get_bdef_columns(self, mock_bdef_columns, mock_format, mock_bdef):
        """
        Test of getting schema and bdef columns

        """
        mock_bdef.return_value = mock.Mock(
            descriptive_business_object_format=mock.Mock(
                business_object_format_usage=string_generator(),
                business_object_format_file_type=string_generator(),
                business_object_format_version=string_generator()
            )
        )

        schema_column_list = []
        bdef_column_list = []
        column_names = [string_generator(), string_generator(), string_generator()]
        for name in column_names:
            mock_column = mock.Mock()
            # Need to mock name attribute with a PropertyMock
            type(mock_column).name = mock.PropertyMock(return_value=name)
            schema_column_list.append(mock_column)

            bdef_column = mock.Mock(
                schema_column_name=name,
                business_object_definition_column_key=mock.Mock(
                    business_object_definition_column_name=string_generator()
                ),
                description=string_generator(20)
            )
            bdef_column_list.append(bdef_column)

        mock_format.return_value = mock.Mock(
            schema=mock.Mock(
                columns=schema_column_list
            )
        )

        mock_bdef_columns.return_value = mock.Mock(
            business_object_definition_columns=bdef_column_list
        )

        key = (string_generator(), string_generator())
        index_array = [0, 1]

        # Run scenario and check values
        self.controller.get_bdef_columns(key, index_array)
        mock_bdef.assert_called_once()
        mock_format.assert_called_once()
        mock_bdef_columns.assert_called_once()
        self.assertEqual(len(self.controller.format_columns[key].index), len(column_names))
        self.assertTrue(all(self.controller.format_columns[key]['Found']))
        self.assertEqual(self.controller.run_summary['fail_rows'], 0)

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectFormatApi.'
                'business_object_format_get_business_object_format')
    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_search_business_object_definition_columns')
    def test_get_bdef_columns_error_no_schema(self, mock_bdef_columns, mock_format, mock_bdef):
        """
        Test of getting schema and bdef columns. Run summary will have error message 'No Schema Columns found'

        """
        mock_bdef.return_value = mock.Mock(
            descriptive_business_object_format=mock.Mock(
                business_object_format_usage=string_generator(),
                business_object_format_file_type=string_generator(),
                business_object_format_version=string_generator()
            )
        )

        mock_format.return_value = mock.Mock(
            schema=mock.Mock(
                columns=[]
            )
        )

        key = (string_generator(), string_generator())
        index_array = [0, 1]

        # Run scenario and check values
        self.controller.get_bdef_columns(key, index_array)
        mock_bdef.assert_called_once()
        mock_format.assert_called_once()
        self.assertEqual(mock_bdef_columns.call_count, 0)
        self.assertEqual(self.controller.run_summary['fail_rows'], 2)
        self.assertEqual(self.controller.run_summary['fail_index'], [2, 3])
        self.assertTrue('No Schema Columns found' in self.controller.run_summary['errors'][0]['message'])

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectFormatApi.'
                'business_object_format_get_business_object_format')
    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_search_business_object_definition_columns')
    def test_get_bdef_columns_error_no_descriptive_format(self, mock_bdef_columns, mock_format, mock_bdef):
        """
        Test of getting schema and bdef columns. Run summary will have error message 'No Descriptive Format defined'

        """
        mock_bdef.return_value = mock.Mock(
            descriptive_business_object_format=''
        )

        key = (string_generator(), string_generator())
        index_array = [0, 1]

        # Run scenario and check values
        self.controller.get_bdef_columns(key, index_array)
        mock_bdef.assert_called_once()
        self.assertEqual(mock_format.call_count, 0)
        self.assertEqual(mock_bdef_columns.call_count, 0)
        self.assertEqual(self.controller.run_summary['fail_rows'], 2)
        self.assertEqual(self.controller.run_summary['fail_index'], [2, 3])
        self.assertTrue('No Descriptive Format defined' in self.controller.run_summary['errors'][0]['message'])

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectFormatApi.'
                'business_object_format_get_business_object_format')
    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_search_business_object_definition_columns')
    def test_get_bdef_columns_api_exception(self, mock_bdef_columns, mock_format, mock_bdef):
        """
        Test of getting schema and bdef columns with ApiException

        """
        mock_bdef.return_value = mock.Mock(
            descriptive_business_object_format=mock.Mock(
                business_object_format_usage=string_generator(),
                business_object_format_file_type=string_generator(),
                business_object_format_version=string_generator()
            )
        )

        schema_column_list = []
        bdef_column_list = []
        column_names = [string_generator(), string_generator(), string_generator()]
        for name in column_names:
            mock_column = mock.Mock()
            # Need to mock name attribute with a PropertyMock
            type(mock_column).name = mock.PropertyMock(return_value=name)
            schema_column_list.append(mock_column)

            bdef_column = mock.Mock(
                schema_column_name=name,
                business_object_definition_column_key=mock.Mock(
                    business_object_definition_column_name=string_generator()
                ),
                description=string_generator(20)
            )
            bdef_column_list.append(bdef_column)

        mock_format.return_value = mock.Mock(
            schema=mock.Mock(
                columns=schema_column_list
            )
        )

        mock_bdef_columns.side_effect = [rest.ApiException(reason='Error')]

        key = (string_generator(), string_generator())
        index_array = [0, 1]

        # Run scenario and check values
        self.controller.get_bdef_columns(key, index_array)
        mock_bdef.assert_called_once()
        mock_format.assert_called_once()
        mock_bdef_columns.assert_called_once()
        self.assertEqual(self.controller.run_summary['fail_rows'], 2)
        self.assertEqual(self.controller.run_summary['fail_index'], [2, 3])
        self.assertTrue('Reason: Error' in str(self.controller.run_summary['errors'][0]['message']))

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectFormatApi.'
                'business_object_format_get_business_object_format')
    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_search_business_object_definition_columns')
    def test_get_bdef_columns_empty_bdef_columns(self, mock_bdef_columns, mock_format, mock_bdef):
        """
        Test of getting schema and bdef columns. There will be extra bdef columns with no schema

        """
        mock_bdef.return_value = mock.Mock(
            descriptive_business_object_format=mock.Mock(
                business_object_format_usage=string_generator(),
                business_object_format_file_type=string_generator(),
                business_object_format_version=string_generator()
            )
        )

        schema_column_list = []
        bdef_column_list = []
        column_names = [string_generator(), string_generator(), string_generator()]
        for name in column_names:
            mock_column = mock.Mock()
            # Need to mock name attribute with a PropertyMock
            type(mock_column).name = mock.PropertyMock(return_value=name)
            schema_column_list.append(mock_column)

            bdef_column = mock.Mock(
                schema_column_name=name,
                business_object_definition_column_key=mock.Mock(
                    business_object_definition_column_name=string_generator()
                ),
                description=string_generator(20)
            )
            bdef_column_list.append(bdef_column)

        mock_format.return_value = mock.Mock(
            schema=mock.Mock(
                columns=schema_column_list
            )
        )

        # Extra Bdef Column
        bdef_column = mock.Mock(
            schema_column_name='',
            business_object_definition_column_key=mock.Mock(
                business_object_definition_column_name=string_generator()
            ),
            description=string_generator(20)
        )
        bdef_column_list.append(bdef_column)

        mock_bdef_columns.return_value = mock.Mock(
            business_object_definition_columns=bdef_column_list
        )

        key = (string_generator(), string_generator())
        index_array = [0, 1]

        # Run scenario and check values
        self.controller.get_bdef_columns(key, index_array)
        mock_bdef.assert_called_once()
        mock_format.assert_called_once()
        mock_bdef_columns.assert_called_once()
        self.assertEqual(len(self.controller.format_columns[key].index), len(column_names) + 1)
        self.assertFalse(all(self.controller.format_columns[key]['Found']))
        self.assertEqual(self.controller.run_summary['fail_rows'], 0)

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectFormatApi.'
                'business_object_format_get_business_object_format')
    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_search_business_object_definition_columns')
    def test_get_bdef_columns_no_bdef_columns(self, mock_bdef_columns, mock_format, mock_bdef):
        """
        Test of getting schema with no bdef columns

        """
        mock_bdef.return_value = mock.Mock(
            descriptive_business_object_format=mock.Mock(
                business_object_format_usage=string_generator(),
                business_object_format_file_type=string_generator(),
                business_object_format_version=string_generator()
            )
        )

        schema_column_list = []
        bdef_column_list = []
        column_names = [string_generator(), string_generator(), string_generator()]
        for name in column_names:
            mock_column = mock.Mock()
            # Need to mock name attribute with a PropertyMock
            type(mock_column).name = mock.PropertyMock(return_value=name)
            schema_column_list.append(mock_column)

        mock_format.return_value = mock.Mock(
            schema=mock.Mock(
                columns=schema_column_list
            )
        )

        mock_bdef_columns.return_value = mock.Mock(
            business_object_definition_columns=bdef_column_list
        )

        key = (string_generator(), string_generator())
        index_array = [0, 1]

        # Run scenario and check values
        self.controller.get_bdef_columns(key, index_array)
        mock_bdef.assert_called_once()
        mock_format.assert_called_once()
        mock_bdef_columns.assert_called_once()
        self.assertEqual(len(self.controller.format_columns[key].index), len(column_names))
        self.assertTrue(all(x == False for x in self.controller.format_columns[key]['Found']))
        self.assertTrue(all(x == '' for x in self.controller.format_columns[key][Columns.COLUMN_NAME.value]))
        self.assertEqual(self.controller.run_summary['fail_rows'], 0)

    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_delete_business_object_definition_column')
    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_create_business_object_definition_column')
    def test_update_bdef_columns(self, mock_create_column, mock_delete_column):
        """
        Test of updating schema columns. Difference in column name will cause a delete followed by a create call

        """
        key = (string_generator(), string_generator())
        schema = str.upper(string_generator()).strip()
        column = string_generator()
        description = string_generator()
        self.controller.format_columns[key] = pd.DataFrame(
            data=[[schema, column, description, True], ['', column, description, False],
                  ['', column, description, False]],
            columns=[Columns.SCHEMA_NAME.value, Columns.COLUMN_NAME.value, Columns.DESCRIPTION.value, 'Found'])

        self.controller.data_frame = pd.DataFrame(
            data=[[schema, column + 'A', description]],
            columns=[Columns.SCHEMA_NAME.value, Columns.COLUMN_NAME.value, Columns.DESCRIPTION.value])
        index_array = self.controller.data_frame.index.tolist()

        # Run scenario and check values
        self.controller.update_bdef_columns(key, index_array)
        mock_create_column.assert_called_once()
        self.assertEqual(mock_delete_column.call_count, 3)
        self.assertFalse(all(self.controller.format_columns[key][Columns.SCHEMA_NAME.value].isna()))
        self.assertEqual(self.controller.run_summary['warnings'][0]['index'], otags.ERROR_CODE)
        self.assertTrue('Could not find a schema name for the following columns' in
                        self.controller.run_summary['warnings'][0]['message'])
        self.assertEqual(self.controller.run_summary['success_rows'] + self.controller.run_summary['fail_rows'],
                         len(index_array))
        self.assertEqual(self.controller.run_summary['success_rows'], 1)
        self.assertEqual(self.controller.run_summary['fail_rows'], 0)

    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_delete_business_object_definition_column')
    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_create_business_object_definition_column')
    def test_update_bdef_columns_add_column(self, mock_create_column, mock_delete_column):
        """
        Test of updating schema columns by adding a new bdef column

        """
        key = (string_generator(), string_generator())
        schema = str.upper(string_generator()).strip()
        schema_2 = str.upper(string_generator()).strip()
        column = string_generator()
        description = string_generator()
        self.controller.format_columns[key] = pd.DataFrame(
            data=[[schema, column, description, True], [schema_2, '', '', False], ['', column, description, False]],
            columns=[Columns.SCHEMA_NAME.value, Columns.COLUMN_NAME.value, Columns.DESCRIPTION.value, 'Found'])

        self.controller.data_frame = pd.DataFrame(
            data=[[schema, column, description], [schema_2, column, description]],
            columns=[Columns.SCHEMA_NAME.value, Columns.COLUMN_NAME.value, Columns.DESCRIPTION.value])
        index_array = self.controller.data_frame.index.tolist()

        # Run scenario and check values
        self.controller.update_bdef_columns(key, index_array)
        mock_create_column.assert_called_once()
        mock_delete_column.assert_called_once()
        self.assertTrue(all(x for x in self.controller.format_columns[key]['Found']))
        self.assertEqual(self.controller.run_summary['warnings'][0]['index'], otags.ERROR_CODE)
        self.assertTrue('Could not find a schema name for the following columns' in
                        self.controller.run_summary['warnings'][0]['message'])
        self.assertEqual(self.controller.run_summary['success_rows'] + self.controller.run_summary['fail_rows'],
                         len(index_array))
        self.assertEqual(self.controller.run_summary['success_rows'], 2)
        self.assertEqual(self.controller.run_summary['fail_rows'], 0)

    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_delete_business_object_definition_column')
    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_create_business_object_definition_column')
    def test_update_bdef_columns_not_found_excel_columns(self, mock_create_column, mock_delete_column):
        """
        Test of updating schema columns with excel schema names not in UDC

        """
        key = (string_generator(), string_generator())
        schema = str.upper(string_generator()).strip()
        schema_2 = str.upper(string_generator()).strip()
        column = string_generator()
        description = string_generator()
        self.controller.format_columns[key] = pd.DataFrame(
            data=[[schema, column, description, True]],
            columns=[Columns.SCHEMA_NAME.value, Columns.COLUMN_NAME.value, Columns.DESCRIPTION.value, 'Found'])

        self.controller.data_frame = pd.DataFrame(
            data=[[schema, column, description], [schema_2, column, description]],
            columns=[Columns.SCHEMA_NAME.value, Columns.COLUMN_NAME.value, Columns.DESCRIPTION.value])
        index_array = self.controller.data_frame.index.tolist()

        # Run scenario and check values
        self.controller.update_bdef_columns(key, index_array)
        self.assertEqual(mock_create_column.call_count, 0)
        self.assertEqual(mock_delete_column.call_count, 0)
        self.assertEqual(self.controller.run_summary['warnings'][0]['index'], index_array[1] + 2)
        self.assertTrue('Could not find schema column for bdef column name' in
                        self.controller.run_summary['warnings'][0]['message'])
        self.assertEqual(self.controller.run_summary['success_rows'] + self.controller.run_summary['fail_rows'],
                         len(index_array))
        self.assertEqual(self.controller.run_summary['success_rows'], 2)
        self.assertEqual(self.controller.run_summary['fail_rows'], 0)

    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_delete_business_object_definition_column')
    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_create_business_object_definition_column')
    def test_update_bdef_columns_not_found_schema_columns(self, mock_create_column, mock_delete_column):
        """
        Test of updating schema columns where there are schema columns with corresponding no bdef column

        """
        key = (string_generator(), string_generator())
        schema = str.upper(string_generator()).strip()
        schema_2 = str.upper(string_generator()).strip()
        column = string_generator()
        description = string_generator()
        self.controller.format_columns[key] = pd.DataFrame(
            data=[[schema, column, description, True], [schema_2, '', description, False]],
            columns=[Columns.SCHEMA_NAME.value, Columns.COLUMN_NAME.value, Columns.DESCRIPTION.value, 'Found'])

        self.controller.data_frame = pd.DataFrame(
            data=[[schema, column, description]],
            columns=[Columns.SCHEMA_NAME.value, Columns.COLUMN_NAME.value, Columns.DESCRIPTION.value])
        index_array = self.controller.data_frame.index.tolist()

        # Run scenario and check values
        self.controller.update_bdef_columns(key, index_array)
        self.assertEqual(mock_create_column.call_count, 0)
        self.assertEqual(mock_delete_column.call_count, 0)
        self.assertEqual(self.controller.run_summary['warnings'][0]['index'], otags.ERROR_CODE)
        self.assertTrue('Could not find column info for the following schema columns' in
                        self.controller.run_summary['warnings'][0]['message'])
        self.assertEqual(self.controller.run_summary['success_rows'] + self.controller.run_summary['fail_rows'],
                         len(index_array))
        self.assertEqual(self.controller.run_summary['success_rows'], 1)
        self.assertEqual(self.controller.run_summary['fail_rows'], 0)

    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_delete_business_object_definition_column')
    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_create_business_object_definition_column')
    def test_update_bdef_columns_api_exception(self, mock_create_column, mock_delete_column):
        """
        Test of updating schema columns with ApiException

        """
        mock_delete_column.side_effect = [rest.ApiException('Error during deleting empty schema names')]
        mock_create_column.side_effect = [rest.ApiException('Error during creating bdef column names')]

        key = (string_generator(), string_generator())
        schema = str.upper(string_generator()).strip()
        schema_2 = str.upper(string_generator()).strip()
        column = string_generator()
        description = string_generator()
        self.controller.format_columns[key] = pd.DataFrame(
            data=[[schema, column, description, True], [schema_2, '', description, False],
                  ['', column, description, False]],
            columns=[Columns.SCHEMA_NAME.value, Columns.COLUMN_NAME.value, Columns.DESCRIPTION.value, 'Found'])

        self.controller.data_frame = pd.DataFrame(
            data=[[schema, column, description], [schema_2, column, description]],
            columns=[Columns.SCHEMA_NAME.value, Columns.COLUMN_NAME.value, Columns.DESCRIPTION.value])
        index_array = self.controller.data_frame.index.tolist()

        # Run scenario and check values
        self.controller.update_bdef_columns(key, index_array)
        self.assertEqual(mock_create_column.call_count, 1)
        self.assertEqual(mock_delete_column.call_count, 1)
        self.assertEqual(self.controller.run_summary['warnings'][0]['index'], otags.ERROR_CODE)
        self.assertTrue('Error during deleting empty schema names' in
                        str(self.controller.run_summary['warnings'][0]['message']))
        self.assertEqual(len(self.controller.run_summary['errors']), 1)
        self.assertEqual(self.controller.run_summary['errors'][0]['index'], index_array[1] + 2)
        self.assertTrue('Error during creating bdef column names' in
                        str(self.controller.run_summary['errors'][0]['message']))
        self.assertEqual(self.controller.run_summary['success_rows'] + self.controller.run_summary['fail_rows'],
                         len(index_array))
        self.assertEqual(self.controller.run_summary['success_rows'], 1)
        self.assertEqual(self.controller.run_summary['fail_rows'], 1)


class TestSampleAction(unittest.TestCase):
    """
    Test Suite for Action Sample
    """

    def setUp(self):
        """
        The setup method that will be called before each test.
        """
        self.controller = otags.Controller()

    def test_load_samples(self):
        """
        Test of the main load sample action

        """
        self.controller.load_worksheet = mock.Mock(
            return_value=pd.DataFrame(data=[['namespace', 'definition']], columns=[Objects.NAMESPACE.value, Objects.DEFINITION_NAME.value])
        )
        self.controller.check_sample_files = mock.Mock()
        self.controller.get_bdef_sample_files = mock.Mock()
        self.controller.upload_download_sample_files = mock.Mock()

        # Run scenario and check values
        self.controller.load_samples()
        self.assertEqual(self.controller.run_summary['total_rows'], 1)

    def test_load_samples_exception(self):
        """
        Test of the main load sample action with exceptions

        """
        self.controller.load_worksheet = mock.Mock(
            return_value=pd.DataFrame(data=[['namespace1', 'definition1'], ['namespace2', 'definition2']],
                                      columns=[Objects.NAMESPACE.value, Objects.DEFINITION_NAME.value]
                                      )
        )

        self.controller.check_sample_files = mock.Mock()
        self.controller.get_business_object_definition = mock.Mock(
            side_effect=[rest.ApiException(reason='Error'), Exception('Exception Thrown 2')]
        )
        self.controller.upload_download_sample_files = mock.Mock()

        # Run scenario and check values
        self.controller.load_samples()
        self.assertEqual(self.controller.run_summary['total_rows'], 2)
        self.assertEqual(self.controller.run_summary['success_rows'] + self.controller.run_summary['fail_rows'],
                         self.controller.run_summary['total_rows'])
        self.assertEqual(self.controller.run_summary['success_rows'], 0)
        self.assertEqual(self.controller.run_summary['fail_rows'], 2)
        self.assertEqual(self.controller.run_summary['fail_index'], [2, 3])
        self.assertEqual(len(self.controller.run_summary['errors']), 2)
        self.assertTrue('Reason: Error' in str(self.controller.run_summary['errors'][0]['message']))
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary['errors'][1]['message'])


    def test_check_sample_files(self):
        """
        Test of checking Excel worksheet for empty cells

        """
        self.controller.data_frame = pd.DataFrame(
            data=[[string_generator()], [''], ['']],
            columns=[Objects.SAMPLE.value])

        # Run scenario and check values
        self.controller.check_sample_files()
        self.assertEqual(self.controller.run_summary['success_rows'], 2)

    def test_run_aws_command_no_command_found(self):
        """
        Test of getting aws method with no commmand found

        """
        resp = mock.Mock(
            aws_access_key='access',
            aws_secret_key='secret',
            aws_session_token='token',
            aws_s3_bucket_name='bucket',
            s3_key_prefix='prefix'
        )

        return_value = self.controller.run_aws_command('s3_err', resp, 'path', 'file')
        self.assertTrue('Command s3_err not found' in return_value)

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definition')
    def test_get_bdef_sample_files(self, mock_bdef):
        """
        Test of getting sample files for a given business object definition

        """
        file_name = 'test_file'
        directory_path = 'path/'

        mock_bdef.return_value = mock.Mock(
            sample_data_files=[mock.Mock(
                directory_path=directory_path,
                file_name=file_name
            )]
        )

        key = (string_generator(), string_generator())

        # Run scenario and check values
        self.controller.get_bdef_sample_files(key, [0])
        self.assertTrue(key in self.controller.sample_files)
        self.assertEqual(self.controller.sample_files[key][file_name], directory_path)
        self.assertEqual(len(self.controller.sample_files[key].keys()), 1)

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definition')
    def test_get_bdef_sample_files_no_files(self, mock_bdef):
        """
        Test of getting sample files for a given business object definition with no files

        """
        mock_bdef.return_value = mock.Mock(
            sample_data_files=[]
        )

        key = (string_generator(), string_generator())

        # Run scenario and check values
        self.controller.get_bdef_sample_files(key, [0])
        self.assertFalse(key in self.controller.sample_files)

    @mock.patch('herdsdk.UploadAndDownloadApi.'
                'uploadand_download_initiate_download_single_sample_file')
    @mock.patch('herdsdk.UploadAndDownloadApi.'
                'uploadand_download_initiate_upload_sample_file')
    @mock.patch('os.path.exists')
    @mock.patch('os.remove')
    @mock.patch('filecmp.cmp')
    def test_upload_download_sample_files(self, mock_cmp, mock_remove, mock_os_exists, mock_upload, mock_download):
        """
        Test of uploading and downloading sample files

        """
        mock_cmp.return_value = False
        mock_remove.return_value = mock.DEFAULT
        mock_os_exists.return_value = True

        file_name = 'test_file'
        directory_path = 'path/'

        mock_download.return_value = mock.Mock(
            business_object_definition_sample_data_file_key=mock.Mock(
                directory_path=directory_path
            )
        )
        mock_upload.return_value = mock.DEFAULT

        self.controller.data_frame = pd.DataFrame(
            data=[[file_name]],
            columns=[Objects.SAMPLE.value])
        key = (string_generator(), string_generator())
        index_array = self.controller.data_frame.index.tolist()

        self.controller.run_aws_command = mock.Mock(return_value=False)
        self.controller.sample_files[key] = {
            file_name: directory_path
        }

        # Run scenario and check values
        self.controller.upload_download_sample_files(key, index_array)
        self.assertEqual(self.controller.run_summary['success_rows'] + self.controller.run_summary['fail_rows'],
                         len(index_array))
        self.assertEqual(self.controller.run_summary['success_rows'], len(index_array))
        self.assertEqual(self.controller.run_summary['fail_rows'], 0)

        mock_cmp.assert_called_once()
        mock_remove.assert_called_once()
        mock_os_exists.assert_called_once()
        mock_upload.assert_called_once()
        mock_download.assert_called_once()

    @mock.patch('herdsdk.UploadAndDownloadApi.'
                'uploadand_download_initiate_download_single_sample_file')
    @mock.patch('herdsdk.UploadAndDownloadApi.'
                'uploadand_download_initiate_upload_sample_file')
    @mock.patch('os.path.exists')
    @mock.patch('os.remove')
    @mock.patch('filecmp.cmp')
    def test_upload_download_sample_files_no_file_found(self, mock_cmp, mock_remove, mock_os_exists, mock_upload, mock_download):
        """
        Test of uploading and downloading sample files with no file found

        """
        mock_cmp.return_value = False
        mock_remove.return_value = mock.DEFAULT
        mock_os_exists.return_value = False

        file_name = 'test_file'
        directory_path = 'path/'

        mock_download.return_value = mock.Mock(
            business_object_definition_sample_data_file_key=mock.Mock(
                directory_path=directory_path
            )
        )
        mock_upload.return_value = mock.DEFAULT

        self.controller.data_frame = pd.DataFrame(
            data=[[file_name]],
            columns=[Objects.SAMPLE.value])
        key = (string_generator(), string_generator())
        index_array = self.controller.data_frame.index.tolist()

        self.controller.run_aws_command = mock.Mock(return_value=False)
        self.controller.sample_files[key] = {
            file_name: directory_path
        }

        # Run scenario and check values
        self.controller.upload_download_sample_files(key, index_array)
        self.assertEqual(self.controller.run_summary['success_rows'] + self.controller.run_summary['fail_rows'],
                         len(index_array))
        self.assertEqual(self.controller.run_summary['success_rows'], 0)
        self.assertEqual(self.controller.run_summary['fail_index'], [i + 2 for i in index_array])
        self.assertEqual(self.controller.run_summary['fail_rows'], len(index_array))

        mock_os_exists.assert_called_once()
        self.assertEqual(mock_cmp.call_count, 0)
        self.assertEqual(mock_remove.call_count, 0)
        self.assertEqual(mock_upload.call_count, 0)
        self.assertEqual(mock_download.call_count, 0)


    @mock.patch('herdsdk.UploadAndDownloadApi.'
                'uploadand_download_initiate_download_single_sample_file')
    @mock.patch('herdsdk.UploadAndDownloadApi.'
                'uploadand_download_initiate_upload_sample_file')
    @mock.patch('os.path.exists')
    @mock.patch('os.remove')
    @mock.patch('filecmp.cmp')
    def test_upload_download_sample_files_skip_upload(self, mock_cmp, mock_remove, mock_os_exists, mock_upload, mock_download):
        """
        Test of uploading and downloading sample files with file already uploaded

        """
        mock_cmp.return_value = False
        mock_remove.return_value = mock.DEFAULT
        mock_os_exists.return_value = True

        file_name = 'test_file'
        directory_path = 'path/'

        mock_download.return_value = mock.Mock(
            business_object_definition_sample_data_file_key=mock.Mock(
                directory_path=directory_path
            )
        )
        mock_upload.return_value = mock.DEFAULT

        self.controller.data_frame = pd.DataFrame(
            data=[[file_name], [file_name]],
            columns=[Objects.SAMPLE.value])
        key = (string_generator(), string_generator())
        index_array = self.controller.data_frame.index.tolist()

        self.controller.run_aws_command = mock.Mock(return_value=False)
        self.controller.sample_files[key] = {
            file_name: directory_path
        }

        # Run scenario and check values
        self.controller.upload_download_sample_files(key, index_array)
        self.assertEqual(self.controller.run_summary['success_rows'] + self.controller.run_summary['fail_rows'],
                         len(index_array))
        self.assertEqual(self.controller.run_summary['success_rows'], len(index_array))
        self.assertEqual(self.controller.run_summary['fail_rows'], 0)

        self.assertEqual(mock_os_exists.call_count, 2)
        self.assertEqual(mock_cmp.call_count, 1)
        self.assertEqual(mock_remove.call_count, 1)
        self.assertEqual(mock_upload.call_count, 1)
        self.assertEqual(mock_download.call_count, 1)

    @mock.patch('herdsdk.UploadAndDownloadApi.'
                'uploadand_download_initiate_download_single_sample_file')
    @mock.patch('herdsdk.UploadAndDownloadApi.'
                'uploadand_download_initiate_upload_sample_file')
    @mock.patch('os.path.exists')
    @mock.patch('os.remove')
    @mock.patch('filecmp.cmp')
    def test_upload_download_sample_files_same_contents(self, mock_cmp, mock_remove, mock_os_exists, mock_upload, mock_download):
        """
        Test of uploading and downloading sample files with downloaded file being same as uploaded

        """
        mock_cmp.return_value = True
        mock_remove.return_value = mock.DEFAULT
        mock_os_exists.return_value = True

        file_name = 'test_file'
        directory_path = 'path/'

        mock_download.return_value = mock.Mock(
            business_object_definition_sample_data_file_key=mock.Mock(
                directory_path=directory_path
            )
        )
        mock_upload.return_value = mock.DEFAULT

        self.controller.data_frame = pd.DataFrame(
            data=[[file_name]],
            columns=[Objects.SAMPLE.value])
        key = (string_generator(), string_generator())
        index_array = self.controller.data_frame.index.tolist()

        self.controller.run_aws_command = mock.Mock(return_value=False)
        self.controller.sample_files[key] = {
            file_name: directory_path
        }

        # Run scenario and check values
        self.controller.upload_download_sample_files(key, index_array)
        self.assertEqual(self.controller.run_summary['success_rows'] + self.controller.run_summary['fail_rows'],
                         len(index_array))
        self.assertEqual(self.controller.run_summary['success_rows'], len(index_array))
        self.assertEqual(self.controller.run_summary['fail_rows'], 0)

        self.assertEqual(mock_os_exists.call_count, 1)
        self.assertEqual(mock_cmp.call_count, 1)
        self.assertEqual(mock_remove.call_count, 1)
        self.assertEqual(mock_upload.call_count, 0)
        self.assertEqual(mock_download.call_count, 1)

    @mock.patch('herdsdk.UploadAndDownloadApi.'
                'uploadand_download_initiate_download_single_sample_file')
    @mock.patch('herdsdk.UploadAndDownloadApi.'
                'uploadand_download_initiate_upload_sample_file')
    @mock.patch('os.path.exists')
    @mock.patch('os.remove')
    @mock.patch('filecmp.cmp')
    def test_upload_download_sample_files_aws_error(self, mock_cmp, mock_remove, mock_os_exists, mock_upload,
                                                        mock_download):
        """
        Test of uploading and downloading sample files with aws error

        """
        mock_cmp.return_value = True
        mock_remove.return_value = mock.DEFAULT
        mock_os_exists.return_value = True

        file_name = 'test_file'
        directory_path = 'path/'

        mock_download.return_value = mock.Mock(
            business_object_definition_sample_data_file_key=mock.Mock(
                directory_path=directory_path
            )
        )
        mock_upload.return_value = mock.DEFAULT

        self.controller.data_frame = pd.DataFrame(
            data=[[file_name]],
            columns=[Objects.SAMPLE.value])
        key = (string_generator(), string_generator())
        index_array = self.controller.data_frame.index.tolist()

        self.controller.run_aws_command = mock.Mock(return_value=True)
        self.controller.sample_files[key] = {
            file_name: directory_path
        }

        # Run scenario and check values
        self.controller.upload_download_sample_files(key, index_array)
        self.assertEqual(self.controller.run_summary['success_rows'] + self.controller.run_summary['fail_rows'],
                         len(index_array))
        self.assertEqual(self.controller.run_summary['success_rows'], 0)
        self.assertEqual(self.controller.run_summary['fail_index'], [i + 2 for i in index_array])
        self.assertEqual(self.controller.run_summary['fail_rows'], len(index_array))

        self.assertEqual(mock_os_exists.call_count, 1)
        self.assertEqual(mock_cmp.call_count, 0)
        self.assertEqual(mock_remove.call_count, 0)
        self.assertEqual(mock_upload.call_count, 0)
        self.assertEqual(mock_download.call_count, 1)
