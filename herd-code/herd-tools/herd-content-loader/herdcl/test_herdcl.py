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
except ImportError:
    from herdcl import otags


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
            'action': 'TestAction',
            'excel_file': 'testfile',
            'userName': 'testuser',
            'userPwd': 'testpwd'
        }

        # Mock config
        self.controller.config = mock_config

        # Run scenario and check values
        self.controller.setup_run(config)
        mock_config.get.assert_called_once()
        self.assertEqual(self.controller.action, str.lower(config['action']))
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
        test_vars = ['TestAction', 'testenv', 'testurl', 'testusername', 'dGVzdHBhc3N3b3Jk']
        mock_config.get.side_effect = test_vars
        self.controller.config = mock_config
        self.controller.setup_run(config)

        # Run scenario and check values
        self.assertEqual(mock_config.get.call_count, 5)
        self.assertEqual(self.controller.action, str.lower(test_vars[0]))
        self.assertEqual(self.controller.configuration.host, test_vars[2])
        self.assertEqual(self.controller.configuration.username, test_vars[3])
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

    def test_get_run_summary(self):
        """
        Test of the running steps and getting run summary

        """
        self.controller.data_frame = pd.DataFrame(data=[['item1'], ['item2']], columns=['column1'])

        mock_step = mock.Mock()
        run_summary = self.controller.get_run_summary([mock_step])
        self.assertEqual(mock_step.call_count, 2)
        self.assertEqual(run_summary['total_rows'], 2)
        self.assertEqual(run_summary['success_rows'], 2)
        self.assertEqual(run_summary['fail_rows'], 0)

    def test_get_run_summary_apiexception(self):
        """
        Test of the running steps and getting run summary with ApiException

        """
        self.controller.data_frame = pd.DataFrame(data=[['item1'], ['item2']], columns=['column1'])

        mock_step = mock.Mock()
        mock_step.side_effect = ['Pass', rest.ApiException(reason='Error')]
        run_summary = self.controller.get_run_summary([mock_step])
        self.assertEqual(mock_step.call_count, 2)
        self.assertEqual(run_summary['total_rows'], 2)
        self.assertEqual(run_summary['success_rows'], 1)
        self.assertEqual(run_summary['fail_rows'], 1)
        self.assertEqual(run_summary['fail_index'], [2])
        self.assertEqual(len(run_summary['errors']), 1)
        self.assertEqual(run_summary['errors'][0]['index'], 2)
        self.assertEqual(run_summary['errors'][0]['message'].reason, 'Error')

    def test_get_run_summary_exception(self):
        """
        Test of the running steps and getting run summary with traceback

        """
        self.controller.data_frame = pd.DataFrame(data=[['item1'], ['item2']], columns=['column1'])

        mock_step = mock.Mock()
        mock_step.side_effect = ['Pass', Exception('Exception Thrown')]
        run_summary = self.controller.get_run_summary([mock_step])
        self.assertEqual(mock_step.call_count, 2)
        self.assertEqual(run_summary['total_rows'], 2)
        self.assertEqual(run_summary['success_rows'], 1)
        self.assertEqual(run_summary['fail_rows'], 1)
        self.assertEqual(run_summary['fail_index'], [2])
        self.assertEqual(len(run_summary['errors']), 1)
        self.assertEqual(run_summary['errors'][0]['index'], 2)
        self.assertTrue('Exception Thrown' in run_summary['errors'][0]['message'])


class TestObjectAction(unittest.TestCase):
    """
    Test Suite for Action Objects
    """

    def setUp(self):
        """
        The setup method that will be called before each test.
        """
        self.controller = otags.Controller()

    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_types')
    @mock.patch('herdsdk.TagTypeApi.tag_type_get_tag_type')
    def test_load_tag_types(self, mock_tag_api, mock_tag_types_api):
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
        self.controller.load_tag_types()
        mock_tag_types_api.assert_called_once()
        self.assertEqual(mock_tag_api.call_count, 2)
        self.assertEqual(self.controller.tag_types['columns'], [code_1])

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.business_object_definition_get_business_object_definition')
    @mock.patch(
        'herdsdk.BusinessObjectDefinitionApi.business_object_definition_update_business_object_definition_descriptive_information')
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

    @mock.patch('herdsdk.BusinessObjectDefinitionApi.business_object_definition_get_business_object_definition')
    @mock.patch(
        'herdsdk.BusinessObjectDefinitionApi.business_object_definition_update_business_object_definition_descriptive_information')
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
                          columns=['column1', 'column2', 'column3', 'column4', 'Bus Obj Def SME User ID'])
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
                          columns=['column1', 'column2', 'column3', 'column4', 'Bus Obj Def SME User ID'])
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
                          columns=['column1', 'column2', 'column3', 'column4', 'Bus Obj Def SME User ID'])
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
                mock.Mock(user_id='user')
            ]
        )

        df = pd.DataFrame(data=[['item1', 'item2', 'item3', 'item4', 'user']],
                          columns=['column1', 'column2', 'column3', 'column4', 'Bus Obj Def SME User ID'])
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
                          columns=['column1', 'column2', 'column3', 'column4', 'Bus Obj Def SME User ID'])
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
