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
import base64
import configparser
import random
import string
import unittest
import os
from unittest import mock

# Third party imports
import pandas as pd

# Herd imports
from herdsdk import rest

# Local imports
try:
    import otags
    from constants import *
except ImportError:
    from herdcl import otags
    from herdcl.constants import *


def string_generator(string_length=10):
    """
    Generate a random string of letters, digits and special characters

    """

    password_characters = string.ascii_letters + string.digits + string.punctuation.replace(',', '').replace('@', '')
    new_string = ''.join(random.choice(password_characters) for _ in range(string_length))
    rand = random.randint(161, 563)
    return new_string + chr(rand)


def string_generator_base64(string_length=10):
    return base64.b64encode(string_generator(string_length).encode('utf-8')).decode('utf-8')


class TestUtilityMethods(unittest.TestCase):
    """
    Test Suite for Utility Methods

    """
    temp_file = 'temp.xlsx'

    @classmethod
    def setUpClass(cls):
        """
        The setup method that will be called once before all tests start

        """
        pd.DataFrame().to_excel(TestUtilityMethods.temp_file)

    @classmethod
    def tearDownClass(cls):
        """
        The tear down method that will be called once after all tests complete

        """
        os.remove(TestUtilityMethods.temp_file)

    def setUp(self):
        """
        The setup method that will be called before each test

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
            'debug_mode': False,
            'env': 'testenv',
            'action': 'SaMpLes',
            'excelFile': 'testfile',
            'sampleDir': 'testdir',
            'namespace': 'testnamespace',
            'exportDir': 'exportdir',
            'userName': 'testuser',
            'userPwd': 'testpwd'
        }

        # Mock config
        self.controller.config = mock_config

        # Run scenario and check values
        self.controller.setup_run(config)
        self.assertEqual(mock_config.get.call_count, 2)
        self.assertFalse(self.controller.debug_mode)
        self.assertEqual(self.controller.action, str.lower(config['action']))
        self.assertEqual(self.controller.excel_file, config['excelFile'])
        self.assertEqual(self.controller.sample_dir, config['sampleDir'])
        self.assertEqual(self.controller.export_namespace, config['namespace'])
        self.assertEqual(self.controller.export_dir, config['exportDir'])
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
        test_vars = ['testdomain', 'SaMpLes', 'testexcel', 'testdir', 'testnamespace', 'testdir', 'testenv',
                     'testurl', 'testusername', 'dGVzdHBhc3N3b3Jk']
        mock_config.get.side_effect = test_vars
        self.controller.config = mock_config
        self.controller.setup_run(config)

        # Run scenario and check values
        self.assertEqual(mock_config.get.call_count, len(test_vars))
        self.assertEqual(self.controller.domain, test_vars[0])
        self.assertEqual(self.controller.action, str.lower(test_vars[1]))
        self.assertEqual(self.controller.excel_file, test_vars[2])
        self.assertEqual(self.controller.sample_dir, test_vars[3])
        self.assertEqual(self.controller.export_namespace, test_vars[4])
        self.assertEqual(self.controller.export_dir, test_vars[5])
        self.assertEqual(self.controller.configuration.host, test_vars[7])
        self.assertEqual(self.controller.configuration.username, test_vars[8])
        self.assertEqual(self.controller.configuration.password, 'testpassword')

        # Check other actions
        actions = ['descriptive info', 'contacts', 'data entity tags', 'columns', 'lineage', 'sample data', 'tags',
                   'relational table']
        self.assertEqual(actions, [str.lower(x) for x in self.controller.actions])

        mock_config.reset_mock(side_effect=True)
        test_vars = [
            'testdomain', 'descriptive info', 'testexcel', 'testdir', 'testnamespace', 'testdir', 'testenv',
            'testurl', 'testusername', 'dGVzdHBhc3N3b3Jk',

            'testdomain', 'SmE', 'testexcel', 'testdir', 'testnamespace', 'testdir', 'testenv',
            'testurl', string_generator(), string_generator_base64(),

            'testdomain', 'Bdef Tags', 'testexcel', 'testdir', 'testnamespace', 'testdir', 'testenv',
            'testurl', string_generator(), string_generator_base64(),

            'testdomain', 'CoLuMns', 'testexcel', 'testdir', 'testnamespace', 'testdir', 'testenv',
            'testurl', string_generator(), string_generator_base64(),

            'testdomain', 'LiNeage', 'testexcel', 'testdir', 'testnamespace', 'testdir', 'testenv',
            'testurl', string_generator(), string_generator_base64(),

            'testdomain', 'TaGs', 'testexcel', 'testdir', 'testnamespace', 'testdir', 'testenv',
            'testurl', string_generator(), string_generator_base64(),

            'testdomain', 'reLATIONal tABLE', 'testexcel', 'testdir', 'testnamespace', 'testdir', 'testenv',
            'testurl', string_generator(), string_generator_base64()
        ]
        mock_config.get.side_effect = test_vars
        self.controller.config = mock_config
        for x in range(len(actions) - 1):
            self.controller.setup_run(config)
        self.assertEqual(mock_config.get.call_count, len(test_vars))

        # Check export actions
        actions = ['latest descriptive info', 'latest columns', 'latest data entity tags']
        self.assertEqual(actions, [str.lower(x) for x in self.controller.export_actions])

        mock_config.reset_mock(side_effect=True)
        test_vars = [
            'testdomain', 'latest descriptive info', 'testexcel', 'testdir', 'testnamespace', 'testdir', 'testenv',
            'testurl', 'testusername', 'dGVzdHBhc3N3b3Jk',

            'testdomain', 'latest CoLuMns', 'testexcel', 'testdir', 'testnamespace', 'testdir', 'testenv',
            'testurl', string_generator(), string_generator_base64(),

            'testdomain', 'latest Bdef Tags', 'testexcel', 'testdir', 'testnamespace', 'testdir', 'testenv',
            'testurl', string_generator(), string_generator_base64(),
        ]
        mock_config.get.side_effect = test_vars
        self.controller.config = mock_config
        for x in range(len(actions)):
            self.controller.setup_run(config)
        self.assertEqual(mock_config.get.call_count, len(test_vars))

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
        self.controller.config.add_section('url')

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

        for act in self.controller.export_actions:
            self.controller.action = str.lower(act)
            self.controller.get_action()

    def test_load_worksheet_no_file(self):
        """
        Test of the load worksheet with no file found

        """
        self.controller.excel_file = 'not_found.xlsx'

        # Run scenario and check values
        with self.assertRaises(FileNotFoundError):
            self.controller.load_worksheet('Sheet')

    def test_load_worksheet_no_sheet(self):
        """
        Test of the load worksheet with no worksheet found

        """
        self.controller.excel_file = TestUtilityMethods.temp_file

        # Run scenario and check values
        with self.assertRaises(ValueError):
            self.controller.load_worksheet('Sheet')

    def test_save_worksheet(self):
        """
        Test of the save worksheet with export directory

        """
        self.controller.get_current_directory()
        self.controller.export_dir = self.controller.path
        self.controller.data_frame = pd.DataFrame([], columns=['col1', 'col2'])

        # Run scenario and check values
        self.controller.save_worksheet('Sheet', TestUtilityMethods.temp_file)
        os.remove(self.controller.export_path)

    def test_save_worksheet_no_export_dir(self):
        """
        Test of the save worksheet with no data and no export directory

        """
        self.controller.get_current_directory()
        self.controller.data_frame = pd.DataFrame([], columns=['col1', 'col2'])

        # Run scenario and check values
        self.controller.save_worksheet('Sheet', TestUtilityMethods.temp_file)
        self.assertEqual(self.controller.export_dir, '')
        os.remove(self.controller.export_path)

    @mock.patch('herdsdk.CurrentUserApi.current_user_get_current_user')
    def test_run(self, mock_user):
        """
        Test of test api

        """
        self.controller.action = 'test_api'
        method = self.controller.get_action()

        # Run scenario and check values
        method()
        mock_user.assert_called_once()
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 1)


class TestObjectAction(unittest.TestCase):
    """
    Test Suite for Action Descriptive Info

    """

    def setUp(self):
        """
        The setup method that will be called before each test

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
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 1)
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            self.controller.run_summary[Summary.TOTAL.value])
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 1)
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 0)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [])
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 0)

    def test_load_object_exception(self):
        """
        Test of the main load object action with exceptions

        """
        self.controller.load_worksheet = mock.Mock(
            return_value=pd.DataFrame(data=[['item1'], ['item2'], ['item3']], columns=['column1'])
        )
        self.controller.update_bdef_descriptive_info = mock.Mock(
            side_effect=[mock.DEFAULT, rest.ApiException(reason='Error'), Exception('Exception Thrown 1')]
        )

        # Run scenario and check values
        self.controller.load_object()
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 3)
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            self.controller.run_summary[Summary.TOTAL.value])
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 1)
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [3, 4])
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 2)
        self.assertTrue(
            'Reason: Error' in str(self.controller.run_summary[Summary.ERRORS.value][0][Summary.MESSAGE.value]))
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary[Summary.ERRORS.value][1][
            Summary.MESSAGE.value])

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
        row = ['namespace', 'bdef_name', string_generator(), string_generator(), string_generator(), string_generator()]

        # Run scenario and check values
        self.controller.update_bdef_descriptive_info(0, row)
        mock_bdef.assert_called_once()
        mock_descr_info.assert_called_once()

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_update_business_object_definition_descriptive_information')
    def test_update_bdef_descriptive_info_no_format(self, mock_descr_info, mock_bdef):
        """
        Test of updating business object definition with no descriptive info

        """
        mock_bdef.return_value = mock.Mock(
            description=string_generator(string_length=10),
            descriptive_business_object_format=None,
            display_name=string_generator(string_length=13)
        )
        row = ['namespace', 'bdef_name', string_generator(), string_generator(), string_generator(), string_generator()]

        # Run scenario and check values
        self.controller.update_bdef_descriptive_info(0, row)
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
        row = ['namespace', 'bdef_name', 'usage', 'file_type', 'logical_name', 'description']

        # Run scenario and check values
        self.controller.update_bdef_descriptive_info(0, row)
        mock_bdef.assert_called_once()
        mock_descr_info.assert_not_called()

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_update_business_object_definition_descriptive_information')
    def test_update_bdef_descriptive_info_newline_no_update(self, mock_descr_info, mock_bdef):
        """
        Test of no update to business object definition descriptive info with new line in description

        """
        mock_bdef.return_value = mock.Mock(
            description='description<br>',
            descriptive_business_object_format=mock.Mock(
                business_object_format_file_type='file_type',
                business_object_format_usage='usage'
            ),
            display_name='logical_name',
            namespace='namespace'
        )
        row = ['namespace', 'bdef_name', 'usage', 'file_type', 'logical_name', 'description\n']

        # Run scenario and check values
        self.controller.update_bdef_descriptive_info(0, row)
        mock_bdef.assert_called_once()
        mock_descr_info.assert_not_called()


class TestSMEAction(unittest.TestCase):
    """
    Test Suite for Action Contacts

    """

    def setUp(self):
        """
        The setup method that will be called before each test

        """
        self.controller = otags.Controller()

    def test_load_sme_exception(self):
        """
        Test of the main load object action with exceptions

        """
        self.controller.load_worksheet = mock.Mock(
            return_value=pd.DataFrame(data=[['item1'], ['item2'], ['item3']], columns=['column1'])
        )
        self.controller.update_sme = mock.Mock(
            side_effect=[Exception('Exception Thrown 1'), mock.DEFAULT, rest.ApiException(reason='Error')]
        )

        # Run scenario and check values
        self.controller.load_sme()
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 3)
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            self.controller.run_summary[Summary.TOTAL.value])
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 1)
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [2, 4])
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 2)
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary[Summary.ERRORS.value][0][
            Summary.MESSAGE.value])
        self.assertTrue(
            'Reason: Error' in str(self.controller.run_summary[Summary.ERRORS.value][1][Summary.MESSAGE.value]))

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

        df = pd.DataFrame(data=[['item1', 'item2', string_generator()]],
                          columns=['column1', 'column2', SubjectMatterExpert.SME.value])
        row = df.iloc[0]

        # Run scenario and check values
        self.controller.update_sme(0, row)
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

        df = pd.DataFrame(data=[['item1', 'item2', 'user, ' + string_generator()]],
                          columns=['column1', 'column2', SubjectMatterExpert.SME.value])
        row = df.iloc[0]

        # Run scenario and check values
        self.controller.update_sme(0, row)
        mock_get_sme.assert_called_once()
        mock_delete_sme.assert_not_called()
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

        df = pd.DataFrame(data=[['item1', 'item2', '']],
                          columns=['column1', 'column2', SubjectMatterExpert.SME.value])
        row = df.iloc[0]

        # Run scenario and check values
        self.controller.update_sme(0, row)
        mock_get_sme.assert_called_once()
        mock_delete_sme.assert_called_once()
        mock_create_sme.assert_not_called()

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

        df = pd.DataFrame(data=[['item1', 'item2', 'user']],
                          columns=['column1', 'column2', SubjectMatterExpert.SME.value])
        row = df.iloc[0]

        # Run scenario and check values
        self.controller.update_sme(0, row)
        mock_get_sme.assert_called_once()
        mock_delete_sme.assert_not_called()
        mock_create_sme.assert_not_called()

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
        df = pd.DataFrame(data=[['item1', 'item2', users]],
                          columns=['column1', 'column2', SubjectMatterExpert.SME.value])
        row = df.iloc[0]

        # Run scenario and check values
        self.controller.update_sme(0, row)
        mock_get_sme.assert_called_once()
        mock_delete_sme.assert_called_once()
        self.assertEqual(mock_create_sme.call_count, 6)


class TestObjectTagAction(unittest.TestCase):
    """
    Test Suite for Action Bus Obj Def Tags

    """

    def setUp(self):
        """
        The setup method that will be called before each test

        """
        self.controller = otags.Controller()

    def test_load_object_tag_exception(self):
        """
        Test of the main load object action with exceptions

        """
        self.controller.load_worksheet = mock.Mock(
            return_value=pd.DataFrame(data=[['item1'], ['item2'], ['item3']], columns=['column1'])
        )
        self.controller.load_worksheet_tag_types = mock.Mock()
        self.controller.update_bdef_tags = mock.Mock(
            side_effect=[Exception('Exception Thrown 1'), rest.ApiException(reason='Error'), mock.DEFAULT]
        )
        # Run scenario and check values
        self.controller.load_object_tags()
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 3)
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            self.controller.run_summary[Summary.TOTAL.value])
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 1)
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [2, 3])
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 2)
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary[Summary.ERRORS.value][0][
            Summary.MESSAGE.value])
        self.assertTrue(
            'Reason: Error' in str(self.controller.run_summary[Summary.ERRORS.value][1][Summary.MESSAGE.value]))

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

        df = pd.DataFrame(data=[['item1', 'item2', 'tag1, tag2 ']],
                          columns=['Column1', 'column2', 'Display Name 1'])
        row = df.iloc[0]

        # Tag Type Code has a corresponding Excel Worksheel Display Column Name
        # Inside the column are comma separated tag codes
        self.controller.tag_types['columns'] = [tag_type_code_1]
        self.controller.tag_types[tag_type_code_1] = 'Display Name 1'

        # Run scenario and check values
        self.controller.update_bdef_tags(0, row)
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

        df = pd.DataFrame(data=[['item1', 'item2', 'tag1, tag2 ', tag_code_1]],
                          columns=['Column1', 'column2', 'Display Name 1', 'Display Name 2'])
        row = df.iloc[0]

        # Tag Type Code has a corresponding Excel Worksheel Display Column Name
        # Inside the column are comma separated tag codes
        self.controller.tag_types['columns'] = [tag_type_code_1, tag_type_code_2]
        self.controller.tag_types[tag_type_code_1] = 'Display Name 1'
        self.controller.tag_types[tag_type_code_2] = 'Display Name 2'

        # Run scenario and check values
        self.controller.update_bdef_tags(0, row)
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

        df = pd.DataFrame(data=[['item1', 'item2', 'tag1, tag2 ']],
                          columns=['Column1', 'column2', 'Display Name 1'])
        row = df.iloc[0]

        # Tag Type Code has a corresponding Excel Worksheel Display Column Name
        # Inside the column are comma separated tag codes
        self.controller.tag_types['columns'] = [tag_type_code_1]
        self.controller.tag_types[tag_type_code_1] = 'Display Name 1'

        # Run scenario and check values
        self.controller.update_bdef_tags(0, row)
        mock_get_bdef_tag.assert_called_once()
        mock_delete_tag.assert_called_once()
        self.assertEqual(mock_create_tag.call_count, 0)


class TestColumnAction(unittest.TestCase):
    """
    Test Suite for Action Columns

    """

    def setUp(self):
        """
        The setup method that will be called before each test

        """
        self.controller = otags.Controller()

    def test_load_columns(self):
        """
        Test of the main load column action

        """
        self.controller.load_worksheet = mock.Mock(
            return_value=pd.DataFrame(data=[['namespace', 'definition']],
                                      columns=[Columns.NAMESPACE.value, Columns.DEFINITION_NAME.value])
        )
        self.controller.check_format_schema_columns = mock.Mock()
        self.controller.get_bdef_columns = mock.Mock()
        self.controller.update_bdef_columns = mock.Mock()

        # Run scenario and check values
        self.controller.load_columns()
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 1)

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
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 2)
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            self.controller.run_summary[Summary.TOTAL.value])
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 0)
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [2, 3])
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 2)
        self.assertTrue(
            'Reason: Error' in str(self.controller.run_summary[Summary.ERRORS.value][0][Summary.MESSAGE.value]))
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary[Summary.ERRORS.value][1][
            Summary.MESSAGE.value])

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
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 3)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [3, 4, 5])

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
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 0)

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
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [2, 3])
        self.assertTrue(
            'No Schema Columns found' in self.controller.run_summary[Summary.ERRORS.value][0][Summary.MESSAGE.value])

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
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [2, 3])
        self.assertTrue('No Descriptive Format defined' in self.controller.run_summary[Summary.ERRORS.value][0][
            Summary.MESSAGE.value])

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
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [2, 3])
        self.assertTrue(
            'Reason: Error' in str(self.controller.run_summary[Summary.ERRORS.value][0][Summary.MESSAGE.value]))

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
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 0)

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
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 0)

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
        self.assertEqual(self.controller.run_summary[Summary.WARNINGS.value][0]['index'], otags.ERROR_CODE)
        self.assertTrue('Could not find a schema name for the following bdef columns' in
                        self.controller.run_summary[Summary.WARNINGS.value][0][Summary.MESSAGE.value])
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            len(index_array))
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 1)
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 0)

    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_delete_business_object_definition_column')
    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_create_business_object_definition_column')
    def test_update_bdef_columns_newline_no_update(self, mock_create_column, mock_delete_column):
        """
        Test of no update to schema columns with new line in description

        """
        key = (string_generator(), string_generator())
        schema = str.upper(string_generator()).strip()
        column = string_generator()
        description = string_generator()
        self.controller.format_columns[key] = pd.DataFrame(
            data=[[schema, column, description + '<br>', True]],
            columns=[Columns.SCHEMA_NAME.value, Columns.COLUMN_NAME.value, Columns.DESCRIPTION.value, 'Found'])

        self.controller.data_frame = pd.DataFrame(
            data=[[schema, column, description + '\n']],
            columns=[Columns.SCHEMA_NAME.value, Columns.COLUMN_NAME.value, Columns.DESCRIPTION.value])
        index_array = self.controller.data_frame.index.tolist()

        # Run scenario and check values
        self.controller.update_bdef_columns(key, index_array)
        self.assertEqual(mock_create_column.call_count, 0)
        self.assertEqual(mock_delete_column.call_count, 0)
        self.assertFalse(all(self.controller.format_columns[key][Columns.SCHEMA_NAME.value].isna()))
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            len(index_array))
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 1)
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 0)

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
        self.assertEqual(self.controller.run_summary[Summary.WARNINGS.value][0]['index'], otags.ERROR_CODE)
        self.assertTrue('Could not find a schema name for the following bdef columns' in
                        self.controller.run_summary[Summary.WARNINGS.value][0][Summary.MESSAGE.value])
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            len(index_array))
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 0)

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
        self.assertEqual(self.controller.run_summary[Summary.WARNINGS.value][0]['index'], index_array[1] + 2)
        self.assertTrue('Could not find schema column for bdef column name' in
                        self.controller.run_summary[Summary.WARNINGS.value][0][Summary.MESSAGE.value])
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            len(index_array))
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 0)

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
        self.assertEqual(self.controller.run_summary[Summary.WARNINGS.value][0]['index'], otags.ERROR_CODE)
        self.assertTrue('Could not find bdef column info for the following schema columns' in
                        self.controller.run_summary[Summary.WARNINGS.value][0][Summary.MESSAGE.value])
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            len(index_array))
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 1)
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 0)

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
        self.assertEqual(self.controller.run_summary[Summary.WARNINGS.value][0]['index'], otags.ERROR_CODE)
        self.assertTrue('Error during deleting empty schema names' in str(
            self.controller.run_summary[Summary.WARNINGS.value][0][Summary.MESSAGE.value]))
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 1)
        self.assertEqual(self.controller.run_summary[Summary.ERRORS.value][0]['index'], index_array[1] + 2)
        self.assertTrue('Error during creating bdef column names' in str(
            self.controller.run_summary[Summary.ERRORS.value][0][Summary.MESSAGE.value]))
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            len(index_array))
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 1)
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 1)


class TestLineageAction(unittest.TestCase):
    """
    Test Suite for Action Lineage

    """

    def setUp(self):
        """
        The setup method that will be called before each test

        """
        self.controller = otags.Controller()
        self.columns = [Lineage.NAMESPACE.value, Lineage.DEFINITION_NAME.value, Lineage.USAGE.value,
                        Lineage.FILE_TYPE.value]
        self.parent_columns = self.columns + ['Parent ' + x for x in self.columns]

    def test_load_lineage(self):
        """
        Test of the main load lineage action

        """
        self.controller.load_worksheet = mock.Mock(
            return_value=pd.DataFrame(data=[['namespace', 'definition', 'usage', 'file type']],
                                      columns=self.columns)
        )
        self.controller.check_lineage = mock.Mock()
        self.controller.update_lineage = mock.Mock()

        # Run scenario and check values
        self.controller.load_lineage()
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 1)

    def test_load_lineage_exception(self):
        """
        Test of the main load lineage action with exceptions

        """
        self.controller.load_worksheet = mock.Mock(
            return_value=pd.DataFrame(data=[['namespace', 'definition', 'usage', 'file type']],
                                      columns=self.columns)
        )

        self.controller.check_lineage = mock.Mock()
        self.controller.update_lineage = mock.Mock(
            side_effect=[rest.ApiException(reason='Error'), Exception('Exception Thrown 2')]
        )

        # Run scenario and check values
        self.controller.load_lineage()
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 1)
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            self.controller.run_summary[Summary.TOTAL.value])
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 0)
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 1)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [2])
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 1)
        self.assertTrue(
            'Reason: Error' in str(self.controller.run_summary[Summary.ERRORS.value][0][Summary.MESSAGE.value]))
        self.controller.load_lineage()
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary[Summary.ERRORS.value][1][
            Summary.MESSAGE.value])

    def test_check_lineage(self):
        """
        Test of checking Excel worksheet for empty cells

        """
        self.controller.data_frame = pd.DataFrame(data=[
            ['namespace', 'definition', 'usage', 'file type', string_generator(), string_generator(),
             string_generator(), string_generator()]],
            columns=self.parent_columns)

        # Run scenario and check values
        self.controller.check_lineage()
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 0)

    def test_check_lineage_empty(self):
        """
        Test of checking Excel worksheet with empty cells

        """
        self.controller.data_frame = pd.DataFrame(data=[
            ['namespace', 'definition', 'usage', 'file type', string_generator(), string_generator(),
             string_generator(), string_generator()],
            ['namespace', 'definition', 'usage', '', string_generator(), string_generator(),
             string_generator(), string_generator()],
            ['namespace', 'definition', 'usage', 'file type', '', string_generator(),
             string_generator(), string_generator()],
            ['namespace', 'definition', 'usage', 'file type', '', '', '', '']],
            columns=self.parent_columns)

        # Run scenario and check values
        self.controller.check_lineage()
        self.assertEqual(len(self.controller.data_frame.index), 2)
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 2)

    @mock.patch('herdsdk.BusinessObjectFormatApi.'
                'business_object_format_update_business_object_format_parents')
    @mock.patch('herdsdk.BusinessObjectFormatApi.'
                'business_object_format_get_business_object_format')
    def test_update_lineage(self, mock_format, mock_update):
        """
        Test of updating business object format lineage

        """
        mock_format.return_value = mock.Mock(
            business_object_format_parents=[mock.Mock(
                namespace=string_generator(),
                business_object_definition_name=string_generator(),
                business_object_format_usage=string_generator(),
                business_object_format_file_type=string_generator()
            )]
        )

        self.controller.data_frame = pd.DataFrame(data=[
            ['namespace', 'definition', 'usage', 'file type', string_generator(), string_generator(),
             string_generator(), string_generator()]],
            columns=self.parent_columns)

        key = ('namespace', 'definition', 'usage', 'file type')
        index_array = self.controller.data_frame.index.tolist()

        # Run scenario and check values
        self.controller.update_lineage(key, index_array)
        self.assertEqual(mock_update.call_count, 1)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], len(index_array))
        self.assertEqual(len(self.controller.run_summary[Summary.CHANGES.value]), 1)
        self.assertTrue(
            'Updated parents' in self.controller.run_summary[Summary.CHANGES.value][0][Summary.MESSAGE.value])

    @mock.patch('herdsdk.BusinessObjectFormatApi.'
                'business_object_format_update_business_object_format_parents')
    @mock.patch('herdsdk.BusinessObjectFormatApi.'
                'business_object_format_get_business_object_format')
    def test_update_lineage_no_update(self, mock_format, mock_update):
        """
        Test of business object format lineage with no update. Mixing order of list of parents and adding empty row

        """
        namespace = ['namespace_a', 'namespace_b']
        bdef_name = ['bdef_name_a', 'bdef_name_b']
        usage = ['usage_a', 'usage_b']
        file_type = ['file_type_a', 'file_type_b']

        mock_format.return_value = mock.Mock(
            business_object_format_parents=[
                mock.Mock(
                    namespace=namespace[0],
                    business_object_definition_name=bdef_name[0],
                    business_object_format_usage=usage[0],
                    business_object_format_file_type=file_type[0]
                ),
                mock.Mock(
                    namespace=namespace[1],
                    business_object_definition_name=bdef_name[1],
                    business_object_format_usage=usage[1],
                    business_object_format_file_type=file_type[1]
                )]
        )

        self.controller.data_frame = pd.DataFrame(
            data=[['namespace', 'definition', 'usage', 'file type', namespace[1], bdef_name[1], usage[1], file_type[1]],
                  ['namespace', 'definition', 'usage', 'file type', '', '', '', ''],
                  ['namespace', 'definition', 'usage', 'file type', namespace[0], bdef_name[0], usage[0], file_type[0]],
                  ['namespace', 'definition', 'usage', 'file type', '', '', '', '']],
            columns=self.parent_columns)

        key = ('namespace', 'definition', 'usage', 'file type')
        index_array = self.controller.data_frame.index.tolist()

        # Run scenario and check values
        self.controller.update_lineage(key, index_array)
        self.assertEqual(mock_update.call_count, 0)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], len(index_array))
        self.assertEqual(len(self.controller.run_summary[Summary.CHANGES.value]), 0)
        self.assertEqual(len(self.controller.run_summary[Summary.WARNINGS.value]), 1)
        self.assertEqual(self.controller.run_summary[Summary.WARNINGS.value][0]['index'], [3])
        self.assertTrue(
            'Skipping empty rows' in self.controller.run_summary[Summary.WARNINGS.value][0][Summary.MESSAGE.value])

    @mock.patch('herdsdk.BusinessObjectFormatApi.'
                'business_object_format_update_business_object_format_parents')
    @mock.patch('herdsdk.BusinessObjectFormatApi.'
                'business_object_format_get_business_object_format')
    def test_update_lineage_row_difference_bdata(self, mock_format, mock_update):
        """
        Test of business object format lineage with difference in list length between return value and dataframe

        """
        namespace = ['namespace_a', 'namespace_b', 'namespace_c']
        bdef_name = ['bdef_name_a', 'bdef_name_b', 'bdef_name_c']
        usage = ['usage_a', 'usage_b', 'usage_c']
        file_type = ['file_type_a', 'file_type_b', 'file_type_c']

        mock_format.return_value = mock.Mock(
            business_object_format_parents=[
                mock.Mock(
                    namespace=namespace[0],
                    business_object_definition_name=bdef_name[0],
                    business_object_format_usage=usage[0],
                    business_object_format_file_type=file_type[0]
                ),
                mock.Mock(
                    namespace=namespace[1],
                    business_object_definition_name=bdef_name[1],
                    business_object_format_usage=usage[1],
                    business_object_format_file_type=file_type[1]
                ),
                mock.Mock(
                    namespace=namespace[2],
                    business_object_definition_name=bdef_name[2],
                    business_object_format_usage=usage[2],
                    business_object_format_file_type=file_type[2]
                )]
        )

        self.controller.data_frame = pd.DataFrame(
            data=[['namespace', 'definition', 'usage', 'file type', namespace[1], bdef_name[1], usage[1],
                   file_type[1]],
                  ['namespace', 'definition', 'usage', 'file type', '', '', '', ''],
                  ['namespace', 'definition', 'usage', 'file type', namespace[0], bdef_name[0], usage[0],
                   file_type[0]],
                  ['namespace', 'definition', 'usage', 'file type', '', '', '', '']],
            columns=self.parent_columns)

        key = ('namespace', 'definition', 'usage', 'file type')
        index_array = self.controller.data_frame.index.tolist()

        # Run scenario and check values
        self.controller.update_lineage(key, index_array)
        self.assertEqual(mock_update.call_count, 1)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], len(index_array))
        self.assertEqual(len(self.controller.run_summary[Summary.CHANGES.value]), 1)
        self.assertTrue(
            'Updated parents' in self.controller.run_summary[Summary.CHANGES.value][0][Summary.MESSAGE.value])

    @mock.patch('herdsdk.BusinessObjectFormatApi.'
                'business_object_format_update_business_object_format_parents')
    @mock.patch('herdsdk.BusinessObjectFormatApi.'
                'business_object_format_get_business_object_format')
    def test_update_lineage_row_difference_excel(self, mock_format, mock_update):
        """
        Test of business object format lineage with difference in list length between return value and dataframe

        """
        namespace = ['namespace_a', 'namespace_b', 'namespace_c']
        bdef_name = ['bdef_name_a', 'bdef_name_b', 'bdef_name_c']
        usage = ['usage_a', 'usage_b', 'usage_c']
        file_type = ['file_type_a', 'file_type_b', 'file_type_c']

        mock_format.return_value = mock.Mock(
            business_object_format_parents=[
                mock.Mock(
                    namespace=namespace[0],
                    business_object_definition_name=bdef_name[0],
                    business_object_format_usage=usage[0],
                    business_object_format_file_type=file_type[0]
                ),
                mock.Mock(
                    namespace=namespace[1],
                    business_object_definition_name=bdef_name[1],
                    business_object_format_usage=usage[1],
                    business_object_format_file_type=file_type[1]
                )]
        )

        self.controller.data_frame = pd.DataFrame(
            data=[['namespace', 'definition', 'usage', 'file type', namespace[1], bdef_name[1], usage[1],
                   file_type[1]],
                  ['namespace', 'definition', 'usage', 'file type', '', '', '', ''],
                  ['namespace', 'definition', 'usage', 'file type', namespace[0], bdef_name[0], usage[0],
                   file_type[0]],
                  ['namespace', 'definition', 'usage', 'file type', '', '', '', ''],
                  ['namespace', 'definition', 'usage', 'file type', namespace[2], bdef_name[2], usage[2],
                   file_type[2]]],
            columns=self.parent_columns)

        key = ('namespace', 'definition', 'usage', 'file type')
        index_array = self.controller.data_frame.index.tolist()

        # Run scenario and check values
        self.controller.update_lineage(key, index_array)
        self.assertEqual(mock_update.call_count, 1)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], len(index_array))
        self.assertEqual(len(self.controller.run_summary[Summary.CHANGES.value]), 1)
        self.assertTrue(
            'Updated parents' in self.controller.run_summary[Summary.CHANGES.value][0][Summary.MESSAGE.value])

    @mock.patch('herdsdk.BusinessObjectFormatApi.'
                'business_object_format_update_business_object_format_parents')
    @mock.patch('herdsdk.BusinessObjectFormatApi.'
                'business_object_format_get_business_object_format')
    def test_update_lineage_empty(self, mock_format, mock_update):
        """
        Test of updating business object format lineage removing all parents

        """
        mock_format.return_value = mock.Mock(
            business_object_format_parents=[mock.Mock(
                namespace=string_generator(),
                business_object_definition_name=string_generator(),
                business_object_format_usage=string_generator(),
                business_object_format_file_type=string_generator()
            )]
        )

        self.controller.data_frame = pd.DataFrame(data=[
            ['namespace', 'definition', 'usage', 'file type', '', '', '', '']],
            columns=self.parent_columns)

        key = ('namespace', 'definition', 'usage', 'file type')
        index_array = self.controller.data_frame.index.tolist()

        # Run scenario and check values
        self.controller.update_lineage(key, index_array)
        self.assertEqual(mock_update.call_count, 1)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], len(index_array))
        self.assertEqual(len(self.controller.run_summary[Summary.CHANGES.value]), 1)
        self.assertTrue(
            'All parents removed' in self.controller.run_summary[Summary.CHANGES.value][0][Summary.MESSAGE.value])

    @mock.patch('herdsdk.BusinessObjectFormatApi.'
                'business_object_format_update_business_object_format_parents')
    @mock.patch('herdsdk.BusinessObjectFormatApi.'
                'business_object_format_get_business_object_format')
    def test_update_lineage_empty_no_update(self, mock_format, mock_update):
        """
        Test of business object format lineage no update

        """
        mock_format.return_value = mock.Mock(
            business_object_format_parents=[]
        )

        self.controller.data_frame = pd.DataFrame(data=[
            ['namespace', 'definition', 'usage', 'file type', '', '', '', ''],
            ['namespace', 'definition', 'usage', 'file type', '', '', '', '']],
            columns=self.parent_columns)

        key = ('namespace', 'definition', 'usage', 'file type')
        index_array = self.controller.data_frame.index.tolist()

        # Run scenario and check values
        self.controller.update_lineage(key, index_array)
        self.assertEqual(mock_update.call_count, 0)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], len(index_array))
        self.assertEqual(len(self.controller.run_summary[Summary.CHANGES.value]), 0)


class TestSampleAction(unittest.TestCase):
    """
    Test Suite for Action Sample Data

    """

    def setUp(self):
        """
        The setup method that will be called before each test

        """
        self.controller = otags.Controller()

    def test_load_samples(self):
        """
        Test of the main load sample action

        """
        self.controller.load_worksheet = mock.Mock(
            return_value=pd.DataFrame(data=[['namespace', 'definition']],
                                      columns=[Samples.NAMESPACE.value, Samples.DEFINITION_NAME.value])
        )
        self.controller.check_sample_files = mock.Mock()
        self.controller.get_bdef_sample_files = mock.Mock()
        self.controller.upload_download_sample_files = mock.Mock()

        # Run scenario and check values
        self.controller.load_samples()
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 1)

    def test_load_samples_exception(self):
        """
        Test of the main load sample action with exceptions

        """
        self.controller.load_worksheet = mock.Mock(
            return_value=pd.DataFrame(data=[['namespace1', 'definition1'], ['namespace2', 'definition2']],
                                      columns=[Samples.NAMESPACE.value, Samples.DEFINITION_NAME.value]
                                      )
        )

        self.controller.check_sample_files = mock.Mock()
        self.controller.get_business_object_definition = mock.Mock(
            side_effect=[rest.ApiException(reason='Error'), Exception('Exception Thrown 2')]
        )
        self.controller.upload_download_sample_files = mock.Mock()

        # Run scenario and check values
        self.controller.load_samples()
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 2)
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            self.controller.run_summary[Summary.TOTAL.value])
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 0)
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [2, 3])
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 2)
        self.assertTrue(
            'Reason: Error' in str(self.controller.run_summary[Summary.ERRORS.value][0][Summary.MESSAGE.value]))
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary[Summary.ERRORS.value][1][
            Summary.MESSAGE.value])

    def test_check_sample_files(self):
        """
        Test of checking Excel worksheet for empty cells

        """
        self.controller.data_frame = pd.DataFrame(
            data=[[string_generator()], [''], ['']],
            columns=[Samples.SAMPLE.value])

        # Run scenario and check values
        self.controller.check_sample_files()
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 2)

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
        self.assertEqual(self.controller.sample_files[key], {})

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
            columns=[Samples.SAMPLE.value])
        key = (string_generator(), string_generator())
        index_array = self.controller.data_frame.index.tolist()

        self.controller.run_aws_command = mock.Mock(return_value=False)
        self.controller.sample_files[key] = {
            file_name: directory_path
        }

        # Run scenario and check values
        self.controller.upload_download_sample_files(key, index_array)
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            len(index_array))
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], len(index_array))
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 0)

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
    def test_upload_download_sample_files_no_file_found(self, mock_cmp, mock_remove, mock_os_exists, mock_upload,
                                                        mock_download):
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
            columns=[Samples.SAMPLE.value])
        key = (string_generator(), string_generator())
        index_array = self.controller.data_frame.index.tolist()

        self.controller.run_aws_command = mock.Mock(return_value=False)
        self.controller.sample_files[key] = {
            file_name: directory_path
        }

        # Run scenario and check values
        self.controller.upload_download_sample_files(key, index_array)
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            len(index_array))
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 0)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [i + 2 for i in index_array])
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], len(index_array))

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
    def test_upload_download_sample_files_skip_upload(self, mock_cmp, mock_remove, mock_os_exists, mock_upload,
                                                      mock_download):
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
            columns=[Samples.SAMPLE.value])
        key = (string_generator(), string_generator())
        index_array = self.controller.data_frame.index.tolist()

        self.controller.run_aws_command = mock.Mock(return_value=False)
        self.controller.sample_files[key] = {
            file_name: directory_path
        }

        # Run scenario and check values
        self.controller.upload_download_sample_files(key, index_array)
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            len(index_array))
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], len(index_array))
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 0)

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
    def test_upload_download_sample_files_same_contents(self, mock_cmp, mock_remove, mock_os_exists, mock_upload,
                                                        mock_download):
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
            columns=[Samples.SAMPLE.value])
        key = (string_generator(), string_generator())
        index_array = self.controller.data_frame.index.tolist()

        self.controller.run_aws_command = mock.Mock(return_value=False)
        self.controller.sample_files[key] = {
            file_name: directory_path
        }

        # Run scenario and check values
        self.controller.upload_download_sample_files(key, index_array)
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            len(index_array))
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], len(index_array))
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 0)

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
            columns=[Samples.SAMPLE.value])
        key = (string_generator(), string_generator())
        index_array = self.controller.data_frame.index.tolist()

        self.controller.run_aws_command = mock.Mock(return_value=True)
        self.controller.sample_files[key] = {
            file_name: directory_path
        }

        # Run scenario and check values
        self.controller.upload_download_sample_files(key, index_array)
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            len(index_array))
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 0)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [i + 2 for i in index_array])
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], len(index_array))

        self.assertEqual(mock_os_exists.call_count, 1)
        self.assertEqual(mock_cmp.call_count, 0)
        self.assertEqual(mock_remove.call_count, 0)
        self.assertEqual(mock_upload.call_count, 0)
        self.assertEqual(mock_download.call_count, 1)

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

    @mock.patch('boto3.s3.transfer.S3Transfer.upload_file')
    def test_upload_file(self, mock_upload):
        """
        Test of boto3 upload_file method getting correct args

        """
        resp = mock.Mock(
            aws_access_key='access',
            aws_secret_key='secret',
            aws_session_token='token',
            aws_s3_bucket_name='bucket',
            s3_key_prefix='prefix',
            aws_kms_key_id=''
        )
        args = {
            'ServerSideEncryption': 'AES256'
        }
        mock_upload.return_value = mock.Mock()

        self.controller.run_aws_command('s3_upload', resp, 'path', 'file')
        mock_upload.assert_called_with(filename='path',
                                       bucket=resp.aws_s3_bucket_name,
                                       key=resp.s3_key_prefix + 'file',
                                       extra_args=args)
        mock_upload.assert_called_once()

    @mock.patch('boto3.s3.transfer.S3Transfer.upload_file')
    def test_upload_file_kms(self, mock_upload):
        """
        Test of boto3 upload_file method with kms key getting correct args

        """
        resp = mock.Mock(
            aws_access_key='access',
            aws_secret_key='secret',
            aws_session_token='token',
            aws_s3_bucket_name='bucket',
            s3_key_prefix='prefix',
            aws_kms_key_id='id'
        )
        args = {
            'ServerSideEncryption': 'aws:kms',
            'SSEKMSKeyId': resp.aws_kms_key_id
        }
        mock_upload.return_value = mock.Mock()

        self.controller.run_aws_command('s3_upload', resp, 'path', 'file')
        mock_upload.assert_called_with(filename='path',
                                       bucket=resp.aws_s3_bucket_name,
                                       key=resp.s3_key_prefix + 'file',
                                       extra_args=args)
        mock_upload.assert_called_once()

    @mock.patch('boto3.s3.transfer.S3Transfer.download_file')
    def test_download_file(self, mock_download):
        """
        Test of boto3 download_file method getting correct args

        """
        resp = mock.Mock(
            aws_access_key='access',
            aws_secret_key='secret',
            aws_session_token='token',
            aws_s3_bucket_name='bucket',
            s3_key_prefix='prefix',
            aws_kms_key_id='id'
        )
        mock_download.return_value = mock.Mock()

        self.controller.run_aws_command('s3_download', resp, 'path', 'file')
        mock_download.assert_called_with(bucket=resp.aws_s3_bucket_name,
                                         key=resp.s3_key_prefix + 'file',
                                         filename='path')
        mock_download.assert_called_once()


class TestTagAction(unittest.TestCase):
    """
    Test Suite for Action Tags

    """

    def setUp(self):
        """
        The setup method that will be called before each test

        """
        self.controller = otags.Controller()

    def test_load_tags(self):
        """
        Test of the main load tag action

        """
        self.controller.load_worksheet = mock.Mock(
            side_effect=[pd.DataFrame(data=[[string_generator(), string_generator()]],
                                      columns=[TagTypes.NAME.value, TagTypes.CODE.value]),
                         pd.DataFrame(data=[[string_generator(), string_generator(), string_generator()]],
                                      columns=[Tags.NAME.value, Tags.TAGTYPE.value, Tags.TAG.value])]
        )
        self.controller.get_tag_type_code_list = mock.Mock(return_value=False)
        self.controller.update_tag_type_code_list = mock.Mock(return_value=False)
        self.controller.delete_tag_type_code_list = mock.Mock(return_value=False)
        self.controller.update_tag_list = mock.Mock(return_value=False)
        self.controller.delete_tag_list = mock.Mock(return_value=False)

        # Run scenario and check values
        self.controller.load_tags()
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 2)

    @mock.patch('herdsdk.TagTypeApi.tag_type_create_tag_type')
    def test_load_tags_fail(self, mock_create):
        """
        Test of the main load tag action with run fail in update_tag_type_code_list step

        """
        self.controller.load_worksheet = mock.Mock(
            side_effect=[pd.DataFrame(data=[[string_generator(), string_generator(), string_generator()],
                                            [string_generator(), string_generator(), string_generator()]],
                                      columns=[TagTypes.NAME.value, TagTypes.CODE.value, TagTypes.DESCRIPTION.value]),
                         pd.DataFrame(data=[[string_generator(), string_generator(), string_generator(), '', '']],
                                      columns=[Tags.NAME.value, Tags.TAGTYPE.value, Tags.TAG.value,
                                               Tags.DESCRIPTION.value, Tags.PARENT.value])]
        )
        self.controller.get_tag_type_code_list = mock.Mock(return_value=False)
        mock_create.side_effect = [rest.ApiException(reason='Error'), Exception('Exception Thrown 2')]
        self.controller.delete_tag_type_code_list = mock.Mock()
        self.controller.update_tag_list = mock.Mock()
        self.controller.delete_tag_list = mock.Mock()

        # Run scenario and check values
        self.controller.load_tags()
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 3)
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [2, 3])
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 2)
        self.assertTrue(
            'Reason: Error' in str(self.controller.run_summary[Summary.ERRORS.value][0][Summary.MESSAGE.value]))
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary[Summary.ERRORS.value][1][
            Summary.MESSAGE.value])
        self.assertEqual(self.controller.delete_tag_type_code_list.call_count, 0)
        self.assertEqual(self.controller.update_tag_list.call_count, 0)
        self.assertEqual(self.controller.delete_tag_list.call_count, 0)

    def test_check_tag_types(self):
        """
        Test of checking Excel worksheet for empty cells

        """
        self.controller.data_frame = pd.DataFrame(
            data=[[string_generator(), string_generator()], [string_generator(), ''], ['', string_generator()],
                  ['', '']],
            columns=[TagTypes.NAME.value, TagTypes.CODE.value])

        # Run scenario and check values
        self.controller.check_tag_types()
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 3)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [3, 4, 5])

    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_type')
    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_types')
    def test_get_tag_type_code_list(self, mock_tag_types, mock_tag_type):
        """
        Test of the getting tag type code list

        """
        mock_tag_types.return_value = mock.Mock(
            tag_type_keys=[mock.Mock(tag_type_code=string_generator())]
        )

        mock_tag_type.return_value = mock.Mock(
            display_name=string_generator(),
            description=string_generator(),
            tag_type_order=0
        )

        # Run scenario and check values
        run_fail = self.controller.get_tag_type_code_list()
        self.assertEqual(mock_tag_types.call_count, 1)
        self.assertEqual(mock_tag_type.call_count, 1)
        self.assertEqual(run_fail, None)

    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_type')
    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_types')
    def test_get_tag_type_code_list_with_error(self, mock_tag_types, mock_tag_type):
        """
        Test of the getting tag type code list with exception thrown

        """
        mock_tag_types.return_value = mock.Mock(
            tag_type_keys=[mock.Mock(tag_type_code=string_generator())]
        )

        mock_tag_type.side_effect = [rest.ApiException(reason='Error'), Exception('Exception Thrown 2')]

        # Run scenario and check values
        run_fail = self.controller.get_tag_type_code_list()
        self.assertTrue(run_fail)
        run_fail = self.controller.get_tag_type_code_list()
        self.assertTrue(run_fail)
        self.assertEqual(mock_tag_types.call_count, 2)
        self.assertEqual(mock_tag_type.call_count, 2)
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 2)
        self.assertTrue(
            'Reason: Error' in str(self.controller.run_summary[Summary.ERRORS.value][0][Summary.MESSAGE.value]))
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary[Summary.ERRORS.value][1][
            Summary.MESSAGE.value])

    @mock.patch('herdsdk.TagTypeApi.tag_type_update_tag_type')
    @mock.patch('herdsdk.TagApi.tag_get_tags')
    def test_update_tag_type_code_list(self, mock_tags, mock_update):
        """
        Test of the updating tag type code list

        """

        tag_type_code = str.upper(string_generator()).strip()
        tag_code_1 = string_generator()
        tag_code_2 = string_generator()
        name = string_generator()
        description = string_generator()

        self.controller.tag_types = {
            tag_type_code: {
                'name': name,
                'description': description,
                'order': 1
            },
            'CODE': {
                'name': 'name',
                'description': 'description',
                'order': 2
            },
            'columns': []
        }

        self.controller.data_frame = pd.DataFrame(
            data=[[name, tag_type_code, description + 'A'], ['name2', 'CODE', 'description']],
            columns=[TagTypes.NAME.value, TagTypes.CODE.value, TagTypes.DESCRIPTION.value])

        mock_tags.side_effect = [mock.Mock(
            tag_children=[mock.Mock(
                has_children=False,
                tag_key=mock.Mock(tag_type_code=tag_type_code, tag_code=tag_code_1)
            )]
        ), mock.Mock(
            tag_children=[mock.Mock(
                has_children=False,
                tag_key=mock.Mock(tag_type_code=tag_type_code, tag_code=tag_code_2)
            )]
        )]

        # Run scenario and check values
        run_fail = self.controller.update_tag_type_code_list()
        self.assertFalse(run_fail)
        self.assertEqual(mock_tags.call_count, 2)
        self.assertEqual(mock_update.call_count, 2)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 2)

        self.assertEqual(self.controller.tag_types['remove'], [])
        remove_tags = [(tag_type_code, tag_code_1), ('CODE', tag_code_2)]
        self.assertEqual(self.controller.tag_list['remove'], remove_tags)

    @mock.patch('herdsdk.TagTypeApi.tag_type_create_tag_type')
    @mock.patch('herdsdk.TagApi.tag_get_tags')
    def test_update_tag_type_code_list_new_code(self, mock_tags, mock_create):
        """
        Test of the updating tag type code list with new code

        """

        tag_type_code = str.upper(string_generator()).strip()
        name = string_generator()
        description = string_generator()

        self.controller.tag_types = {
            'columns': []
        }

        self.controller.data_frame = pd.DataFrame(data=[[name, tag_type_code, description]],
                                                  columns=[TagTypes.NAME.value, TagTypes.CODE.value,
                                                           TagTypes.DESCRIPTION.value])

        # Run scenario and check values
        run_fail = self.controller.update_tag_type_code_list()
        self.assertFalse(run_fail)
        self.assertEqual(mock_tags.call_count, 0)
        self.assertEqual(mock_create.call_count, 1)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 1)
        self.assertTrue(
            'Change in row. Old Tag Type Code:\nNone' in self.controller.run_summary[Summary.CHANGES.value][0][
                Summary.MESSAGE.value])

    @mock.patch('herdsdk.TagTypeApi.tag_type_create_tag_type')
    @mock.patch('herdsdk.TagTypeApi.tag_type_update_tag_type')
    @mock.patch('herdsdk.TagApi.tag_get_tags')
    def test_update_tag_type_code_list_no_update(self, mock_tags, mock_update, mock_create):
        """
        Test of the updating tag type code list no update

        """

        tag_type_code = str.upper(string_generator()).strip()
        name = string_generator()
        description = string_generator()

        self.controller.tag_types = {
            tag_type_code: {
                'name': name,
                'description': description,
                'order': 1
            },
            'columns': []
        }

        self.controller.data_frame = pd.DataFrame(data=[[name, tag_type_code, description]],
                                                  columns=[TagTypes.NAME.value, TagTypes.CODE.value,
                                                           TagTypes.DESCRIPTION.value])

        mock_tags.side_effect = [
            mock.Mock(tag_children=[
                mock.Mock(
                    has_children=True,
                    tag_key=mock.Mock(tag_type_code=tag_type_code, tag_code=string_generator())
                )]
            ), mock.Mock(tag_children=[
                mock.Mock(
                    has_children=False,
                    tag_key=mock.Mock(tag_type_code=tag_type_code, tag_code=string_generator())
                )]
            )]

        # Run scenario and check values
        run_fail = self.controller.update_tag_type_code_list()
        self.assertFalse(run_fail)
        self.assertEqual(mock_tags.call_count, 2)
        self.assertEqual(mock_update.call_count, 0)
        self.assertEqual(mock_create.call_count, 0)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 1)

    @mock.patch('herdsdk.TagTypeApi.tag_type_create_tag_type')
    @mock.patch('herdsdk.TagApi.tag_get_tags')
    def test_update_tag_type_code_list_error(self, mock_tags, mock_create):
        """
        Test of the updating tag type code list with exception thrown

        """

        tag_type_code = str.upper(string_generator()).strip()
        name = string_generator()
        description = string_generator()

        self.controller.tag_types = {
            'columns': []
        }

        self.controller.data_frame = pd.DataFrame(
            data=[[name, tag_type_code, description], ['name', 'CODE', 'description']],
            columns=[TagTypes.NAME.value, TagTypes.CODE.value, TagTypes.DESCRIPTION.value])

        mock_create.side_effect = [rest.ApiException(reason='Error'), Exception('Exception Thrown 2')]

        # Run scenario and check values
        run_fail = self.controller.update_tag_type_code_list()
        self.assertTrue(run_fail)
        self.assertEqual(mock_tags.call_count, 0)
        self.assertEqual(mock_create.call_count, 2)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 0)
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [2, 3])
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 2)
        self.assertTrue(
            'Reason: Error' in str(self.controller.run_summary[Summary.ERRORS.value][0][Summary.MESSAGE.value]))
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary[Summary.ERRORS.value][1][
            Summary.MESSAGE.value])

    @mock.patch('herdsdk.TagApi.tag_delete_tag')
    @mock.patch('herdsdk.TagTypeApi.tag_type_delete_tag_type')
    @mock.patch('herdsdk.TagApi.tag_get_tags')
    def test_delete_tag_type_code_list(self, mock_tags, mock_delete_tag_type, mock_delete_tag):
        """
        Test of the deleting tag type code list

        """
        tag_type_code = string_generator()
        tag = string_generator()
        child = 'child'
        self.controller.tag_types['remove'] = [tag_type_code]

        mock_tags.side_effect = [
            mock.Mock(tag_children=[
                mock.Mock(
                    has_children=True,
                    tag_key=mock.Mock(tag_type_code=tag_type_code, tag_code=tag)
                )]
            ), mock.Mock(tag_children=[
                mock.Mock(
                    has_children=False,
                    tag_key=mock.Mock(tag_type_code=tag_type_code, tag_code=child)
                )]
            )]

        # Run scenario and check values
        run_fail = self.controller.delete_tag_type_code_list()
        self.assertFalse(run_fail)
        self.assertEqual(mock_tags.call_count, 2)
        self.assertEqual(mock_delete_tag_type.call_count, 1)
        self.assertEqual(mock_delete_tag.call_count, 2)
        self.assertTrue('Tag Type Codes not found in Excel and Children deleted' in
                        self.controller.run_summary[Summary.CHANGES.value][0][Summary.MESSAGE.value])

        deleted = {
            tag_type_code: [tag],
            tag: [child]
        }
        self.assertEqual(self.controller.delete_tag_children, deleted)

    @mock.patch('herdsdk.TagApi.tag_delete_tag')
    @mock.patch('herdsdk.TagTypeApi.tag_type_delete_tag_type')
    @mock.patch('herdsdk.TagApi.tag_get_tags')
    def test_delete_tag_type_code_list_error(self, mock_tags, mock_delete_tag_type, mock_delete_tag):
        """
        Test of the deleting tag type code list with exception thrown

        """
        self.controller.tag_types['remove'] = [string_generator(), string_generator()]

        mock_tags.side_effect = [rest.ApiException(reason='Error'), Exception('Exception Thrown 2')]

        # Run scenario and check values
        run_fail = self.controller.delete_tag_type_code_list()
        self.assertTrue(run_fail)
        self.assertEqual(mock_tags.call_count, 2)
        self.assertEqual(mock_delete_tag_type.call_count, 0)
        self.assertEqual(mock_delete_tag.call_count, 0)
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 2)
        self.assertTrue(
            'Reason: Error' in str(self.controller.run_summary[Summary.ERRORS.value][0][Summary.MESSAGE.value]))
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary[Summary.ERRORS.value][1][
            Summary.MESSAGE.value])

    def test_check_tags(self):
        """
        Test of checking Excel worksheet for empty cells

        """
        self.controller.data_frame = pd.DataFrame(
            data=[[string_generator(), string_generator(), string_generator()],
                  [string_generator(), string_generator(), ''],
                  [string_generator(), '', string_generator()],
                  ['', string_generator(), string_generator()],
                  ['', '', '']],
            columns=[Tags.NAME.value, Tags.TAGTYPE.value, Tags.TAG.value])

        # Run scenario and check values
        self.controller.check_tags()
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 4)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [3, 4, 5, 6])

    @mock.patch('herdsdk.TagApi.tag_create_tag')
    @mock.patch('herdsdk.TagApi.tag_update_tag')
    @mock.patch('herdsdk.TagApi.tag_get_tag')
    def test_update_tag_list(self, mock_tags, mock_update, mock_create):
        """
        Test of the updating tag list

        """
        name_1 = 'name 1'
        name_2 = 'name 2'
        name_3 = 'name 3'
        tag_type = str.upper(string_generator()).strip()
        parent_tag = str.upper(string_generator()).strip()
        child_tag = str.upper(string_generator()).strip()

        self.controller.tag_list = {
            tag_type: [parent_tag, child_tag],
            'remove': []
        }

        self.controller.data_frame = pd.DataFrame(
            data=[[name_1, tag_type, parent_tag, 'description', ''], [name_2 + 'A', tag_type, child_tag, '', name_1],
                  [name_3, tag_type, child_tag, '', 'parent missing']],
            columns=[Tags.NAME.value, Tags.TAGTYPE.value, Tags.TAG.value, Tags.DESCRIPTION.value, Tags.PARENT.value])

        mock_tags.side_effect = [
            mock.Mock(
                display_name=name_1,
                description=None,
                tag_key=mock.Mock(tag_type_code=tag_type, tag_code=parent_tag),
                parent_tag_key=None,
                search_score_multiplier=1.0
            ),
            mock.Mock(
                display_name=name_2,
                description=None,
                tag_key=mock.Mock(tag_type_code=tag_type, tag_code=child_tag),
                parent_tag_key=mock.Mock(tag_type_code=tag_type, tag_code=parent_tag),
                search_score_multiplier=1.0
            ),
            mock.Mock(
                display_name=name_3,
                description=None,
                tag_key=mock.Mock(tag_type_code=tag_type, tag_code=child_tag),
                parent_tag_key=mock.Mock(tag_type_code=tag_type, tag_code=parent_tag),
                search_score_multiplier=1.0
            )
        ]

        # Run scenario and check values
        run_fail = self.controller.update_tag_list()
        self.assertFalse(run_fail)
        self.assertEqual(mock_tags.call_count, 3)
        self.assertEqual(mock_update.call_count, 3)
        self.assertEqual(mock_create.call_count, 0)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 3)
        self.assertEqual(len(self.controller.run_summary[Summary.CHANGES.value]), 3)
        self.assertTrue(
            'Change in row. Old Tag' in self.controller.run_summary[Summary.CHANGES.value][0][Summary.MESSAGE.value])
        self.assertEqual(len(self.controller.run_summary[Summary.WARNINGS.value]), 1)
        self.assertTrue('Please double check spelling' in self.controller.run_summary[Summary.WARNINGS.value][0][
            Summary.MESSAGE.value])

    @mock.patch('herdsdk.TagApi.tag_create_tag')
    @mock.patch('herdsdk.TagApi.tag_update_tag')
    @mock.patch('herdsdk.TagApi.tag_get_tag')
    def test_update_tag_list_new_tag(self, mock_tags, mock_update, mock_create):
        """
        Test of the updating tag list

        """
        name_1 = 'name'
        name_2 = 'name 2'
        tag_type = str.upper(string_generator()).strip()
        parent_tag = str.upper(string_generator()).strip()
        child_tag = str.upper(string_generator()).strip()

        self.controller.tag_list = {
            tag_type: [],
            'remove': []
        }

        self.controller.data_frame = pd.DataFrame(
            data=[[name_1, tag_type, parent_tag, '', ''], [name_2, tag_type, child_tag, '', name_1]],
            columns=[Tags.NAME.value, Tags.TAGTYPE.value, Tags.TAG.value, Tags.DESCRIPTION.value, Tags.PARENT.value])

        # Run scenario and check values
        run_fail = self.controller.update_tag_list()
        self.assertFalse(run_fail)
        self.assertEqual(mock_tags.call_count, 0)
        self.assertEqual(mock_update.call_count, 0)
        self.assertEqual(mock_create.call_count, 2)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 2)
        self.assertEqual(len(self.controller.run_summary[Summary.CHANGES.value]), 2)
        self.assertTrue('Change in row. Old Tag:\nNone' in self.controller.run_summary[Summary.CHANGES.value][0][
            Summary.MESSAGE.value])

    @mock.patch('herdsdk.TagApi.tag_create_tag')
    @mock.patch('herdsdk.TagApi.tag_update_tag')
    @mock.patch('herdsdk.TagApi.tag_get_tag')
    def test_update_tag_list_no_update(self, mock_tags, mock_update, mock_create):
        """
        Test of the updating tag list with no update

        """
        name_1 = 'name'
        name_2 = 'name 2'
        tag_type = str.upper(string_generator()).strip()
        parent_tag = str.upper(string_generator()).strip()
        child_tag = str.upper(string_generator()).strip()

        self.controller.tag_list = {
            tag_type: [parent_tag, child_tag],
            'remove': []
        }

        self.controller.data_frame = pd.DataFrame(
            data=[[name_1, tag_type, parent_tag, '', ''], [name_2, tag_type, child_tag, '', name_1]],
            columns=[Tags.NAME.value, Tags.TAGTYPE.value, Tags.TAG.value, Tags.DESCRIPTION.value, Tags.PARENT.value])

        mock_tags.side_effect = [
            mock.Mock(
                display_name=name_1,
                description=None,
                tag_key=mock.Mock(tag_type_code=tag_type, tag_code=parent_tag),
                parent_tag_key=None,
                search_score_multiplier=None
            ),
            mock.Mock(
                display_name=name_2,
                description=None,
                tag_key=mock.Mock(tag_type_code=tag_type, tag_code=child_tag),
                parent_tag_key=mock.Mock(tag_type_code=tag_type, tag_code=parent_tag),
                search_score_multiplier=None
            )
        ]

        # Run scenario and check values
        run_fail = self.controller.update_tag_list()
        self.assertFalse(run_fail)
        self.assertEqual(mock_tags.call_count, 2)
        self.assertEqual(mock_update.call_count, 0)
        self.assertEqual(mock_create.call_count, 0)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 2)

    @mock.patch('herdsdk.TagApi.tag_create_tag')
    @mock.patch('herdsdk.TagApi.tag_update_tag')
    @mock.patch('herdsdk.TagApi.tag_get_tag')
    def test_update_tag_list_newline_no_update(self, mock_tags, mock_update, mock_create):
        """
        Test of the updating tag list with no update to description with new line

        """
        name_1 = 'name'
        name_2 = 'name 2'
        tag_type = str.upper(string_generator()).strip()
        parent_tag = str.upper(string_generator()).strip()
        child_tag = str.upper(string_generator()).strip()
        description = string_generator()

        self.controller.tag_list = {
            tag_type: [parent_tag, child_tag],
            'remove': []
        }

        self.controller.data_frame = pd.DataFrame(
            data=[[name_1, tag_type, parent_tag, description + '\n', ''],
                  [name_2, tag_type, child_tag, description + '\n', name_1]],
            columns=[Tags.NAME.value, Tags.TAGTYPE.value, Tags.TAG.value, Tags.DESCRIPTION.value, Tags.PARENT.value])

        mock_tags.side_effect = [
            mock.Mock(
                display_name=name_1,
                description=description + '<br>',
                tag_key=mock.Mock(tag_type_code=tag_type, tag_code=parent_tag),
                parent_tag_key=None,
                search_score_multiplier=None
            ),
            mock.Mock(
                display_name=name_2,
                description=description + '<br>',
                tag_key=mock.Mock(tag_type_code=tag_type, tag_code=child_tag),
                parent_tag_key=mock.Mock(tag_type_code=tag_type, tag_code=parent_tag),
                search_score_multiplier=None
            )
        ]

        # Run scenario and check values
        run_fail = self.controller.update_tag_list()
        self.assertFalse(run_fail)
        self.assertEqual(mock_tags.call_count, 2)
        self.assertEqual(mock_update.call_count, 0)
        self.assertEqual(mock_create.call_count, 0)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 2)

    @mock.patch('herdsdk.TagApi.tag_delete_tag')
    def test_delete_tag_list(self, mock_delete):
        """
        Test of the deleting tag type code list

        """
        tag_type_code = string_generator()
        tag_1 = string_generator()
        tag_2 = string_generator()
        self.controller.tag_list['remove'] = [(tag_type_code, tag_1), (tag_type_code, tag_2)]

        # Run scenario and check values
        self.controller.delete_tag_list()
        self.assertEqual(mock_delete.call_count, 2)
        self.assertTrue('Tags not found in Excel and deleted' in self.controller.run_summary[Summary.CHANGES.value][0][
            Summary.MESSAGE.value])

        deleted = {
            tag_type_code: [tag_1, tag_2]
        }
        self.assertEqual(self.controller.delete_tag_children, deleted)

    @mock.patch('herdsdk.TagApi.tag_delete_tag')
    def test_delete_tag_list_error(self, mock_delete):
        """
        Test of the deleting tag type code list with exception thrown

        """
        tag_type_code = string_generator()
        tag_1 = string_generator()
        tag_2 = string_generator()
        self.controller.tag_list['remove'] = [(tag_type_code, tag_1), (tag_type_code, tag_2)]

        mock_delete.side_effect = [rest.ApiException(reason='Error'), Exception('Exception Thrown 2')]

        # Run scenario and check values
        self.controller.delete_tag_list()
        self.assertEqual(mock_delete.call_count, 2)
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 2)
        self.assertTrue(
            'Reason: Error' in str(self.controller.run_summary[Summary.ERRORS.value][0][Summary.MESSAGE.value]))
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary[Summary.ERRORS.value][1][
            Summary.MESSAGE.value])


class TestRelationalAction(unittest.TestCase):
    """
    Test Suite for Action Relational Table

    """

    def setUp(self):
        """
        The setup method that will be called before each test

        """
        self.controller = otags.Controller()

    @mock.patch('herdsdk.RelationalTableRegistrationApi.'
                'relational_table_registration_create_relational_table_registration')
    def test_create_relational_table(self, mock_create):
        """
        Test of the registering a relational table

        """
        self.controller.load_worksheet = mock.Mock(side_effect=[
            pd.DataFrame(data=[[string_generator(), string_generator(), string_generator(), string_generator(),
                                string_generator(), string_generator(), string_generator(), True],
                               [string_generator(), string_generator(), string_generator(), string_generator(),
                                string_generator(), string_generator(), string_generator(), 'TRUE']],
                         columns=[Relational.NAMESPACE.value, Relational.DEFINITION_NAME.value,
                                  Relational.FORMAT_USAGE.value, Relational.DATA_PROVIDER_NAME.value,
                                  Relational.SCHEMA_NAME.value, Relational.TABLE_NAME.value,
                                  Relational.STORAGE_NAME.value, Relational.APPEND.value])
        ])

        # Run scenario and check values
        self.controller.load_relational()
        self.assertEqual(mock_create.call_count, 2)
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 2)
        self.assertEqual(
            self.controller.run_summary[Summary.SUCCESS.value] + self.controller.run_summary[Summary.FAIL.value],
            self.controller.run_summary[Summary.TOTAL.value])
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 0)

    @mock.patch('herdsdk.RelationalTableRegistrationApi.'
                'relational_table_registration_create_relational_table_registration')
    def test_create_relational_table_fail(self, mock_create):
        """
        Test of the registering a relational table with error

        """
        self.controller.load_worksheet = mock.Mock(side_effect=[
            pd.DataFrame(data=[[string_generator(), string_generator(), string_generator(), string_generator(),
                                string_generator(), string_generator(), string_generator(), True],
                               [string_generator(), string_generator(), string_generator(), string_generator(),
                                string_generator(), string_generator(), string_generator(), False]],
                         columns=[Relational.NAMESPACE.value, Relational.DEFINITION_NAME.value,
                                  Relational.FORMAT_USAGE.value, Relational.DATA_PROVIDER_NAME.value,
                                  Relational.SCHEMA_NAME.value, Relational.TABLE_NAME.value,
                                  Relational.STORAGE_NAME.value, Relational.APPEND.value])
        ])
        mock_create.side_effect = [rest.ApiException(reason='Error'), Exception('Exception Thrown 2')]

        # Run scenario and check values
        self.controller.load_relational()
        self.assertEqual(mock_create.call_count, 2)
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [2, 3])
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 2)
        self.assertTrue(
            'Reason: Error' in str(self.controller.run_summary[Summary.ERRORS.value][0][Summary.MESSAGE.value]))
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary[Summary.ERRORS.value][1][
            Summary.MESSAGE.value])


class TestExportDescInfoAction(unittest.TestCase):
    """
    Test Suite for Action Latest Descriptive Info

    """

    def setUp(self):
        """
        The setup method that will be called before each test

        """
        self.controller = otags.Controller()

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definitions1')
    def test_get_all_bdefs(self, mock_bdefs):
        """
        Test of the herdsdk call get all business object definitions

        """
        bdef = string_generator()
        mock_bdefs.return_value = mock.Mock(
            business_object_definition_keys=[bdef]
        )

        bdef_keys = self.controller.get_all_bdefs()
        self.assertEqual(mock_bdefs.call_count, 1)
        self.assertEqual(bdef_keys[0], bdef)

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definitions1')
    def test_get_all_bdefs_fail(self, mock_bdefs):
        """
        Test of the herdsdk call get all business object definitions with errors

        """
        mock_bdefs.side_effect = [rest.ApiException(reason='Error'), Exception('Exception Thrown 2')]

        # Run scenario and check values
        self.controller.export_descriptive()
        self.controller.export_descriptive()
        self.assertEqual(mock_bdefs.call_count, 2)
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 0)
        self.assertEqual(self.controller.run_summary[Summary.FAIL.value], 0)
        self.assertEqual(self.controller.run_summary[Summary.FAIL_INDEX.value], [])
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 2)
        self.assertTrue(
            'Reason: Error' in str(self.controller.run_summary[Summary.ERRORS.value][0][Summary.MESSAGE.value]))
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary[Summary.ERRORS.value][1][
            Summary.MESSAGE.value])

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definitions1')
    def test_get_all_bdefs_empty(self, mock_bdefs):
        """
        Test of the herdsdk call get all business object definitions with no bdefs found

        """
        mock_bdefs.return_value = mock.Mock(
            business_object_definition_keys=[]
        )

        # Run scenario and check values
        self.controller.export_descriptive()
        self.assertEqual(mock_bdefs.call_count, 1)
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 0)
        self.assertTrue('No data entities found' in self.controller.run_summary[Summary.COMMENTS.value])

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definitions1')
    def test_export_descriptive_info(self, mock_bdefs, mock_bdef):
        """
        Test of exporting business object definition descriptive info

        """
        bdef = string_generator()
        mock_bdefs.return_value = mock.Mock(
            business_object_definition_keys=[
                mock.Mock(
                    namespace='ns1',
                    business_object_definition_name=bdef
                ),
                mock.Mock(
                    namespace='ns2',
                    business_object_definition_name=bdef
                ),
            ]
        )

        description = string_generator()
        file_type = string_generator()
        format_usage = string_generator()
        display_name = string_generator()

        mock_bdef.side_effect = [
            mock.Mock(
                description=description,
                descriptive_business_object_format=mock.Mock(
                    business_object_format_file_type=file_type,
                    business_object_format_usage=format_usage
                ),
                display_name=display_name
            ),
            mock.Mock(
                description='description',
                descriptive_business_object_format=None,
                display_name='display_name'
            )
        ]

        self.controller.save_worksheet = mock.Mock()

        # Run scenario and check values
        self.controller.export_descriptive()
        self.assertEqual(mock_bdefs.call_count, 1)
        self.assertEqual(mock_bdef.call_count, 2)
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 2)
        self.assertEqual(len(self.controller.data_frame.index), 2)
        self.assertEqual(self.controller.data_frame.iloc[0][Objects.DEFINITION_NAME.value], bdef)
        self.assertEqual(self.controller.data_frame.iloc[0][Objects.FORMAT_USAGE.value], format_usage)
        self.assertEqual(self.controller.data_frame.iloc[0][Objects.FILE_TYPE.value], file_type)
        self.assertEqual(self.controller.data_frame.iloc[0][Objects.DISPLAY_NAME.value], display_name)
        self.assertEqual(self.controller.data_frame.iloc[0][Objects.DESCRIPTION.value], description)
        self.assertEqual(self.controller.data_frame.iloc[1][Objects.DEFINITION_NAME.value], bdef)
        self.assertEqual(self.controller.data_frame.iloc[1][Objects.FORMAT_USAGE.value], '')
        self.assertEqual(self.controller.data_frame.iloc[1][Objects.FILE_TYPE.value], '')
        self.assertEqual(self.controller.data_frame.iloc[1][Objects.DISPLAY_NAME.value], 'display_name')
        self.assertEqual(self.controller.data_frame.iloc[1][Objects.DESCRIPTION.value], 'description')

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definitions1')
    def test_export_descriptive_info_fail(self, mock_bdefs, mock_bdef):
        """
        Test of exporting business object definition descriptive info with error

        """
        bdef = string_generator()
        mock_bdefs.return_value = mock.Mock(
            business_object_definition_keys=[
                mock.Mock(
                    namespace='ns1',
                    business_object_definition_name=bdef
                ),
                mock.Mock(
                    namespace='ns2',
                    business_object_definition_name=bdef
                ),
            ]
        )

        mock_bdef.side_effect = [rest.ApiException(reason='Error'), Exception('Exception Thrown 2')]

        self.controller.save_worksheet = mock.Mock()

        # Run scenario and check values
        self.controller.export_descriptive()
        self.assertEqual(mock_bdefs.call_count, 1)
        self.assertEqual(mock_bdef.call_count, 2)
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 0)
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 2)
        self.assertTrue(
            'Reason: Error' in str(self.controller.run_summary[Summary.ERRORS.value][0][Summary.MESSAGE.value]))
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary[Summary.ERRORS.value][1][
            Summary.MESSAGE.value])


class TestExportColumns(unittest.TestCase):
    """
    Test Suite for Action Latest Columns

    """

    def setUp(self):
        """
        The setup method that will be called before each test

        """
        self.controller = otags.Controller()

    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_search_business_object_definition_columns')
    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definitions1')
    def test_export_columns(self, mock_bdefs, mock_column):
        """
        Test of exporting business object definition columns

        """
        bdef = string_generator()
        mock_bdefs.return_value = mock.Mock(
            business_object_definition_keys=[
                mock.Mock(
                    namespace='ns1',
                    business_object_definition_name=bdef
                ),
                mock.Mock(
                    namespace='ns2',
                    business_object_definition_name=bdef
                ),
            ]
        )

        schema_name = string_generator()
        column_name = string_generator()
        description = string_generator()

        mock_column.side_effect = [
            mock.Mock(
                business_object_definition_columns=[
                    mock.Mock(
                        schema_column_name=schema_name,
                        business_object_definition_column_key=mock.Mock(
                            business_object_definition_column_name=column_name),
                        description=description
                    ),
                ]
            ),
            mock.Mock(
                business_object_definition_columns=[]
            )
        ]

        self.controller.save_worksheet = mock.Mock()

        # Run scenario and check values
        self.controller.export_columns()
        self.assertEqual(mock_bdefs.call_count, 1)
        self.assertEqual(mock_column.call_count, 2)
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 1)
        self.assertEqual(len(self.controller.data_frame.index), 1)
        self.assertEqual(self.controller.data_frame.iloc[0][Columns.DEFINITION_NAME.value], bdef)
        self.assertEqual(self.controller.data_frame.iloc[0][Columns.SCHEMA_NAME.value], schema_name)
        self.assertEqual(self.controller.data_frame.iloc[0][Columns.COLUMN_NAME.value], column_name)
        self.assertEqual(self.controller.data_frame.iloc[0][Columns.DESCRIPTION.value], description)

    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_search_business_object_definition_columns')
    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definitions1')
    def test_export_columns_fail(self, mock_bdefs, mock_columns):
        """
        Test of exporting business object definition columns with error

        """
        bdef = string_generator()
        mock_bdefs.return_value = mock.Mock(
            business_object_definition_keys=[
                mock.Mock(
                    namespace='ns1',
                    business_object_definition_name=bdef
                ),
                mock.Mock(
                    namespace='ns2',
                    business_object_definition_name=bdef
                ),
            ]
        )

        mock_columns.side_effect = [rest.ApiException(reason='Error'), Exception('Exception Thrown 2')]

        self.controller.save_worksheet = mock.Mock()

        # Run scenario and check values
        self.controller.export_columns()
        self.assertEqual(mock_bdefs.call_count, 1)
        self.assertEqual(mock_columns.call_count, 2)
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 2)
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 2)
        self.assertTrue(
            'Reason: Error' in str(self.controller.run_summary[Summary.ERRORS.value][0][Summary.MESSAGE.value]))
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary[Summary.ERRORS.value][1][
            Summary.MESSAGE.value])

    @mock.patch('herdsdk.BusinessObjectDefinitionColumnApi.'
                'business_object_definition_column_search_business_object_definition_columns')
    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definitions1')
    def test_export_columns_no_bdef(self, mock_bdefs, mock_columns):
        """
        Test of exporting business object definition columns with no bdef

        """
        mock_bdefs.return_value = mock.Mock(
            business_object_definition_keys=[]
        )

        # Run scenario and check values
        self.controller.export_columns()
        self.assertEqual(mock_bdefs.call_count, 1)
        self.assertEqual(mock_columns.call_count, 0)
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 0)


class TestExportBdefTagsAction(unittest.TestCase):
    """
    Test Suite for Action Latest Data Entity Tags

    """

    def setUp(self):
        """
        The setup method that will be called before each test

        """
        self.controller = otags.Controller()

    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_types')
    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_type')
    def test_get_all_tags(self, mock_tag_api, mock_tag_types_api):
        """
        Test of herdsdk api get tag types and tag type codes

        """
        code_1 = string_generator()
        code_2 = string_generator()
        column_1 = 'display name 1'
        column_2 = 'display name 2'

        mock_tag_types_api.return_value = mock.Mock(
            tag_type_keys=[
                mock.Mock(tag_type_code=code_1),
                mock.Mock(tag_type_code=code_2)
            ]
        )

        mock_tag_api.side_effect = [
            mock.Mock(display_name=column_1),
            mock.Mock(display_name=column_2),
        ]

        # Run scenario and check values
        self.controller.get_all_tags()
        mock_tag_types_api.assert_called_once()
        self.assertEqual(mock_tag_api.call_count, 2)
        self.assertEqual(self.controller.tag_types['columns'], [column_1, column_2])

    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_types')
    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_type')
    def test_get_all_tags_fail(self, mock_tag_api, mock_tag_types_api):
        """
        Test of herdsdk api get tag types and tag type codes with error

        """
        code_1 = string_generator()

        mock_tag_types_api.return_value = mock.Mock(
            tag_type_keys=[mock.Mock(tag_type_code=code_1)]
        )

        mock_tag_api.side_effect = [rest.ApiException(reason='Error'), Exception('Exception Thrown 2')]

        # Run scenario and check values
        self.controller.get_all_tags()
        self.controller.get_all_tags()
        self.assertEqual(mock_tag_types_api.call_count, 2)
        self.assertEqual(mock_tag_api.call_count, 2)
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 2)
        self.assertTrue(
            'Reason: Error' in str(self.controller.run_summary[Summary.ERRORS.value][0][Summary.MESSAGE.value]))
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary[Summary.ERRORS.value][1][
            Summary.MESSAGE.value])

    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_types')
    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_type')
    @mock.patch('herdsdk.BusinessObjectDefinitionTagApi.'
                'business_object_definition_tag_get_business_object_definition_tags_by_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definitions1')
    def test_export_bdef_tags(self, mock_bdefs, mock_bdef_tag, mock_tag, mock_tag_types):
        """
        Test of exporting business object definition tags

        """
        tag_type_code_1 = string_generator()
        tag_type_code_2 = string_generator()
        tag_code_1 = string_generator()
        tag_code_2 = string_generator()
        tag_code_3 = string_generator()
        column_1 = 'display name 1'
        column_2 = 'display name 2'

        mock_tag_types.return_value = mock.Mock(
            tag_type_keys=[
                mock.Mock(tag_type_code=tag_type_code_1),
                mock.Mock(tag_type_code=tag_type_code_2)
            ]
        )

        mock_tag.side_effect = [
            mock.Mock(display_name=column_1),
            mock.Mock(display_name=column_2),
        ]

        bdef = string_generator()
        mock_bdefs.return_value = mock.Mock(
            business_object_definition_keys=[
                mock.Mock(
                    namespace='ns1',
                    business_object_definition_name=bdef
                ),
                mock.Mock(
                    namespace='ns2',
                    business_object_definition_name=bdef
                ),
            ]
        )

        mock_bdef_tag.side_effect = [
            mock.Mock(
                business_object_definition_tag_keys=[
                    mock.Mock(tag_key=mock.Mock(tag_type_code=tag_type_code_1, tag_code=tag_code_1)),
                    mock.Mock(tag_key=mock.Mock(tag_type_code=tag_type_code_2, tag_code=tag_code_2)),
                    mock.Mock(tag_key=mock.Mock(tag_type_code=tag_type_code_2, tag_code=tag_code_3)),
                ]
            ),
            mock.Mock(
                business_object_definition_tag_keys=[]
            ),
        ]

        self.controller.save_worksheet = mock.Mock()

        # Run scenario and check values
        self.controller.export_bdef_tags()
        self.assertEqual(mock_tag_types.call_count, 1)
        self.assertEqual(mock_tag.call_count, 2)
        self.assertEqual(mock_bdefs.call_count, 1)
        self.assertEqual(mock_bdef_tag.call_count, 2)
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 1)
        self.assertEqual(len(self.controller.data_frame.index), 1)
        self.assertEqual(self.controller.data_frame.iloc[0][ObjectTags.DEFINITION_NAME.value], bdef)
        self.assertEqual(self.controller.data_frame.iloc[0][column_1], tag_code_1)
        self.assertEqual(self.controller.data_frame.iloc[0][column_2], '{},{}'.format(tag_code_2, tag_code_3))

    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_types')
    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_type')
    @mock.patch('herdsdk.BusinessObjectDefinitionTagApi.'
                'business_object_definition_tag_get_business_object_definition_tags_by_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definitions1')
    def test_export_bdef_tags_fail(self, mock_bdefs, mock_bdef_tag, mock_tag, mock_tag_types):
        """
        Test of exporting business object definition tags with error

        """
        tag_type_code_1 = string_generator()
        tag_type_code_2 = string_generator()
        column_1 = 'display name 1'
        column_2 = 'display name 2'

        mock_tag_types.return_value = mock.Mock(
            tag_type_keys=[
                mock.Mock(tag_type_code=tag_type_code_1),
                mock.Mock(tag_type_code=tag_type_code_2)
            ]
        )

        mock_tag.side_effect = [
            mock.Mock(display_name=column_1),
            mock.Mock(display_name=column_2),
        ]

        bdef = string_generator()
        mock_bdefs.return_value = mock.Mock(
            business_object_definition_keys=[
                mock.Mock(
                    namespace='ns1',
                    business_object_definition_name=bdef
                ),
                mock.Mock(
                    namespace='ns2',
                    business_object_definition_name=bdef
                ),
            ]
        )

        mock_bdef_tag.side_effect = [rest.ApiException(reason='Error'), Exception('Exception Thrown 2')]

        self.controller.save_worksheet = mock.Mock()

        # Run scenario and check values
        self.controller.export_bdef_tags()
        self.assertEqual(mock_tag_types.call_count, 1)
        self.assertEqual(mock_tag.call_count, 2)
        self.assertEqual(mock_bdefs.call_count, 1)
        self.assertEqual(mock_bdef_tag.call_count, 2)
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 2)
        self.assertEqual(self.controller.run_summary[Summary.SUCCESS.value], 0)
        self.assertEqual(len(self.controller.run_summary[Summary.ERRORS.value]), 2)
        self.assertTrue(
            'Reason: Error' in str(self.controller.run_summary[Summary.ERRORS.value][0][Summary.MESSAGE.value]))
        self.assertTrue('Traceback (most recent call last)' in self.controller.run_summary[Summary.ERRORS.value][1][
            Summary.MESSAGE.value])

    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_types')
    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_type')
    @mock.patch('herdsdk.BusinessObjectDefinitionTagApi.'
                'business_object_definition_tag_get_business_object_definition_tags_by_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definitions1')
    def test_export_bdef_tags_no_tags(self, mock_bdefs, mock_bdef_tag, mock_tag, mock_tag_types):
        """
        Test of exporting business object definition tags with no tags

        """
        bdef = string_generator()
        mock_bdefs.return_value = mock.Mock(
            business_object_definition_keys=[
                mock.Mock(
                    namespace='ns1',
                    business_object_definition_name=bdef
                ),
                mock.Mock(
                    namespace='ns2',
                    business_object_definition_name=bdef
                ),
            ]
        )

        mock_tag_types.side_effect = [rest.ApiException(reason='Error')]

        # Run scenario and check values
        self.controller.export_bdef_tags()
        self.assertEqual(mock_bdefs.call_count, 1)
        self.assertEqual(mock_tag_types.call_count, 1)
        self.assertEqual(mock_tag.call_count, 0)
        self.assertEqual(mock_bdef_tag.call_count, 0)
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 0)

    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_types')
    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_type')
    @mock.patch('herdsdk.BusinessObjectDefinitionTagApi.'
                'business_object_definition_tag_get_business_object_definition_tags_by_business_object_definition')
    @mock.patch('herdsdk.BusinessObjectDefinitionApi.'
                'business_object_definition_get_business_object_definitions1')
    def test_export_bdef_tags_no_bdef(self, mock_bdefs, mock_bdef_tag, mock_tag, mock_tag_types):
        """
        Test of exporting business object definition tags with no bdef

        """
        mock_bdefs.return_value = mock.Mock(
            business_object_definition_keys=[]
        )

        # Run scenario and check values
        self.controller.export_bdef_tags()
        self.assertEqual(mock_bdefs.call_count, 1)
        self.assertEqual(mock_tag_types.call_count, 0)
        self.assertEqual(mock_tag.call_count, 0)
        self.assertEqual(mock_bdef_tag.call_count, 0)
        self.assertEqual(self.controller.run_summary[Summary.TOTAL.value], 0)
