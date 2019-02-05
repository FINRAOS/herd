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

import unittest
from unittest.mock import patch
from entity import Entity
import herdsdk
import pandas as pd


class TestEntity(unittest.TestCase):
    """
    The unit test class used to the Entity class.
    """

    def setUp(self):
        """
        The setup method that will be called before each test.
        """
        self.entity = Entity(configuration=None,
                             namespace="NAMESPACE",
                             name="BUS_OBJ_DEF_NAME")

    # pylint: disable=line-too-long
    @patch('herd_sdk.BusinessObjectDefinitionColumnApi.business_object_definition_column_search_business_object_definition_columns')
    def test_get_columns(self, column_api_mock):
        """
        Test of the get columns function.

        :param column_api_mock: the mock api
        """

        # Build a mock business object definition column key
        column_key = herd_sdk. \
            BusinessObjectDefinitionColumnKey(namespace="NAMESPACE",
                                              business_object_definition_name="BUS_OBJ_DEF_NAME",
                                              business_object_definition_column_name="COLUMN_NAME")

        # Build a mock business object definition column
        column = herd_sdk.BusinessObjectDefinitionColumn(
            id=123,
            business_object_definition_column_key=column_key,
            schema_column_name="SCHEMA_COLUMN_NAME",
            description="DESCRIPTION",
            business_object_definition_column_change_events=None)

        # Create the mock business object definition column search response
        column_search_response = \
            herd_sdk.BusinessObjectDefinitionColumnSearchResponse(
                [column])

        # Set the mock return value
        column_api_mock.return_value = column_search_response

        # Call the method under test
        columns = self.entity.get_columns()

        # Verify the results of the test
        self.assertEqual('COLUMN_NAME', columns.loc[
            columns['PhysicalName'] == 'SCHEMA_COLUMN_NAME', 'BusinessName'].values[0])
        assert column_api_mock is herd_sdk.BusinessObjectDefinitionColumnApi. \
            business_object_definition_column_search_business_object_definition_columns
        assert column_api_mock.called_once
        column_search_key = \
            herd_sdk.BusinessObjectDefinitionColumnSearchKey(
                "NAMESPACE", "BUS_OBJ_DEF_NAME")
        column_search_filter = \
            herd_sdk.BusinessObjectDefinitionColumnSearchFilter(
                [column_search_key])
        column_search_request = herd_sdk.BusinessObjectDefinitionColumnSearchRequest(
            business_object_definition_column_search_filters=[column_search_filter])
        column_api_mock. \
            assert_called_once_with(column_search_request, fields='schemaColumnName')

    # pylint: disable=line-too-long
    @patch('herd_sdk.BusinessObjectDataApi.business_object_data_get_all_business_object_data_by_business_object_definition')
    def test_list_registered_data(self, business_object_data_api_mock):
        """
        Test of the list registered data function.

        :param business_object_data_api_mock: the mock api
        """

        # Build a mock business object data key
        business_object_data_key = \
            herd_sdk.BusinessObjectDataKey(namespace='NAMESPACE',
                                           business_object_definition_name='BUS_OBJ_DEF_NAME',
                                           business_object_format_usage='USAGE',
                                           business_object_format_file_type='F_TYPE',
                                           business_object_format_version=2,
                                           partition_value='2019-01-30',
                                           sub_partition_values=None,
                                           business_object_data_version=3)

        # Create the mock business object data
        business_object_data = herd_sdk.BusinessObjectDataKeys([business_object_data_key])

        # Set the mock return value
        business_object_data_api_mock.return_value = business_object_data

        # Call the method under test
        registered_data = self.entity.list_registered_data('2019-01-30')

        # Verify the results of the test
        self.assertEqual(1, len(registered_data))
        self.assertEqual(3, registered_data.dataVersion.item())
        assert business_object_data_api_mock is herd_sdk.BusinessObjectDataApi. \
            business_object_data_get_all_business_object_data_by_business_object_definition
        assert business_object_data_api_mock.called_once
        business_object_data_api_mock.assert_called_once_with('NAMESPACE', 'BUS_OBJ_DEF_NAME')

    # pylint: disable=line-too-long
    @patch('herd_sdk.BusinessObjectDataApi.business_object_data_get_all_business_object_data_by_business_object_definition')
    def test_list_registered_data_empty(self, business_object_data_api_mock):
        """
         Test of the list registered data function with no partition value.

         :param business_object_data_api_mock: the mock api
         """

        # Build a mock business object data key
        business_object_data_key = \
            herd_sdk.BusinessObjectDataKey(namespace='NAMESPACE',
                                           business_object_definition_name='BUS_OBJ_DEF_NAME',
                                           business_object_format_usage='USAGE',
                                           business_object_format_file_type='F_TYPE',
                                           business_object_format_version=2,
                                           partition_value='2019-01-30',
                                           sub_partition_values=None,
                                           business_object_data_version=3)

        # Create the mock business object data
        business_object_data = herd_sdk.BusinessObjectDataKeys([business_object_data_key])

        # Set the mock return value
        business_object_data_api_mock.return_value = business_object_data

        # Call the method under test
        registered_data = self.entity.list_registered_data()
        self.assertEqual(2, registered_data.loc[registered_data['partitionValue'] == '2019-01-30',
                                                'formatVersion'].values[0])
        assert business_object_data_api_mock \
            is herd_sdk.BusinessObjectDataApi. \
            business_object_data_get_all_business_object_data_by_business_object_definition
        assert business_object_data_api_mock.called_once
        business_object_data_api_mock.assert_called_once_with('NAMESPACE', 'BUS_OBJ_DEF_NAME')

    # pylint: disable=line-too-long
    @patch('herd_sdk.BusinessObjectDataApi.business_object_data_get_all_business_object_data_by_business_object_definition')
    def test_get_data_version(self, business_object_data_api_mock):
        """
          Test of the get data version function.

          :param business_object_data_api_mock: the mock api
          """

        # Build a mock business object data key
        business_object_data_key = \
            herd_sdk.BusinessObjectDataKey(namespace='NAMESPACE',
                                           business_object_definition_name='BUS_OBJ_DEF_NAME',
                                           business_object_format_usage='USAGE',
                                           business_object_format_file_type='F_TYPE',
                                           business_object_format_version=2,
                                           partition_value='2019-01-30',
                                           sub_partition_values=None,
                                           business_object_data_version=3)

        # Create the mock business object data
        business_object_data = herd_sdk.BusinessObjectDataKeys([business_object_data_key])

        # Set the mock return value
        business_object_data_api_mock.return_value = business_object_data

        # Call the method under test
        data_version = self.entity.get_data_version('2019-01-30')
        self.assertEqual(3, data_version)
        assert business_object_data_api_mock \
            is herd_sdk.BusinessObjectDataApi. \
            business_object_data_get_all_business_object_data_by_business_object_definition
        assert business_object_data_api_mock.called_once
        business_object_data_api_mock.assert_called_once_with('NAMESPACE', 'BUS_OBJ_DEF_NAME')

    def test_string_representation(self):
        """
        Test of the string representation of the entity class.
        """

        self.assertEqual('NAMESPACE/BUS_OBJ_DEF_NAME', self.entity.__repr__())

    @patch('herd_sdk.BusinessObjectDataApi.business_object_data_get_s3_key_prefix')
    @patch('herd_sdk.BusinessObjectFormatApi.business_object_format_get_business_object_format')
    # pylint: disable=line-too-long
    @patch('herd_sdk.BusinessObjectDataApi.business_object_data_get_all_business_object_data_by_business_object_definition')
    def test_get_prefix(self,
                        business_object_data_api_mock,
                        business_object_format_api_mock,
                        get_s3_mock):
        """
        Test of the get prefix function.

        :param business_object_data_api_mock: the business object data mock
        :param business_object_format_api_mock: the business object format mock
        :param get_s3_mock: the business object data api get s3 mock
        """

        # Create the mock s3 key prefix information
        s3_key_prefix_information = herd_sdk.S3KeyPrefixInformation(
            'namespace/dpn/usage/f_type/bus_obj_def_name/schm-v2/data-v3/partition-key=2019-01-30')

        # Set the mock return value
        get_s3_mock.return_value = s3_key_prefix_information

        # Create the mock business object format
        business_object_format = \
            herd_sdk.BusinessObjectFormat(id=123,
                                          namespace='NAMESPACE',
                                          business_object_definition_name='BUS_OBJ_DEF_NAME',
                                          business_object_format_usage='USAGE',
                                          business_object_format_file_type='F_TYPE',
                                          business_object_format_version=2, latest_version=2,
                                          partition_key='partition-key', description=None,
                                          document_schema=None, attributes=None,
                                          attribute_definitions=None, schema=None,
                                          business_object_format_parents=None,
                                          business_object_format_children=None,
                                          business_object_format_external_interfaces=None,
                                          record_flag=None, retention_period_in_days=None,
                                          retention_type=None,
                                          allow_non_backwards_compatible_changes=None)

        # Set the mock return value
        business_object_format_api_mock.return_value = business_object_format

        # Build a mock business object data key
        business_object_data_key = \
            herd_sdk.BusinessObjectDataKey(namespace='NAMESPACE',
                                           business_object_definition_name='BUS_OBJ_DEF_NAME',
                                           business_object_format_usage='USAGE',
                                           business_object_format_file_type='F_TYPE',
                                           business_object_format_version=2,
                                           partition_value='2019-01-30',
                                           sub_partition_values=None,
                                           business_object_data_version=3)

        # Create the mock business object data
        business_object_data = herd_sdk.BusinessObjectDataKeys([business_object_data_key])

        # Set the mock return value
        business_object_data_api_mock.return_value = business_object_data

        # Call the method under test
        prefix = self.entity.get_prefix('USAGE', 'F_TYPE', '2019-01-30')
        self.assertEqual(
            'namespace/dpn/usage/f_type/bus_obj_def_name/schm-v2/data-v3/partition-key=2019-01-30',
            prefix)
        assert business_object_data_api_mock \
            is herd_sdk.BusinessObjectDataApi. \
            business_object_data_get_all_business_object_data_by_business_object_definition
        assert business_object_data_api_mock.called_once
        business_object_data_api_mock.assert_called_once_with('NAMESPACE', 'BUS_OBJ_DEF_NAME')
        assert business_object_format_api_mock \
            is herd_sdk.BusinessObjectFormatApi.business_object_format_get_business_object_format
        assert business_object_format_api_mock.called_once
        business_object_format_api_mock.assert_called_once_with('NAMESPACE',
                                                                'BUS_OBJ_DEF_NAME',
                                                                'USAGE',
                                                                'F_TYPE')

    def test_s3_url(self):
        """
        Test the s3 url function.
        """

        url = self.entity.s3_url('bucket/prefix/file')
        self.assertEqual(url, 's3://bucket/prefix/file')

    @patch('pandas.read_csv')
    # pylint: disable=line-too-long
    @patch('herd_sdk.BusinessObjectDefinitionColumnApi.business_object_definition_column_search_business_object_definition_columns')
    @patch('herd_sdk.BusinessObjectDataApi.business_object_data_get_s3_key_prefix')
    @patch('herd_sdk.BusinessObjectFormatApi.business_object_format_get_business_object_format')
    # pylint: disable=line-too-long
    @patch('herd_sdk.BusinessObjectDataApi.business_object_data_get_all_business_object_data_by_business_object_definition')
    # pylint: disable=too-many-arguments,too-many-locals
    def test_get_file(self,
                      business_object_data_api_mock,
                      business_object_format_api_mock,
                      get_s3_mock,
                      column_api_mock,
                      pandas_mock):
        """
        Test of the get file function.

        :param business_object_data_api_mock: the business object data mock
        :param business_object_format_api_mock: the business object format mock
        :param get_s3_mock: the business object data api get s3 mock
        :param column_api_mock: the column api mock
        :param pandas_mock: the pandas mock
        """

        # Build a mock business object definition column key
        column_key = herd_sdk. \
            BusinessObjectDefinitionColumnKey(namespace="NAMESPACE",
                                              business_object_definition_name="BUS_OBJ_DEF_NAME",
                                              business_object_definition_column_name="COLUMN_NAME")

        # Build a mock business object definition column
        column = herd_sdk.BusinessObjectDefinitionColumn(
            id=123,
            business_object_definition_column_key=column_key,
            schema_column_name="SCHEMA_COLUMN_NAME",
            description="DESCRIPTION",
            business_object_definition_column_change_events=None)

        # Create the mock business object definition column search response
        column_search_response = \
            herd_sdk.BusinessObjectDefinitionColumnSearchResponse(
                [column])

        # Set the mock return value
        column_api_mock.return_value = column_search_response

        # Create the mock s3 key prefix information
        s3_key_prefix_information = herd_sdk.S3KeyPrefixInformation(
            'namespace/dpn/usage/f_type/bus_obj_def_name/schm-v2/data-v3/partition-key=2019-01-30')

        # Set the mock return value
        get_s3_mock.return_value = s3_key_prefix_information

        # Build a mock schema
        schema = herd_sdk.Schema(columns=None,
                                 partitions=None,
                                 null_value="null",
                                 delimiter="|",
                                 escape_character="/",
                                 partition_key_group=None)

        # Create the mock business object format
        business_object_format = \
            herd_sdk.BusinessObjectFormat(id=123,
                                          namespace='NAMESPACE',
                                          business_object_definition_name='BUS_OBJ_DEF_NAME',
                                          business_object_format_usage='USAGE',
                                          business_object_format_file_type='F_TYPE',
                                          business_object_format_version=2, latest_version=2,
                                          partition_key='partition-key', description=None,
                                          document_schema=None, attributes=None,
                                          attribute_definitions=None, schema=schema,
                                          business_object_format_parents=None,
                                          business_object_format_children=None,
                                          business_object_format_external_interfaces=None,
                                          record_flag=None, retention_period_in_days=None,
                                          retention_type=None,
                                          allow_non_backwards_compatible_changes=None)

        # Set the mock return value
        business_object_format_api_mock.return_value = business_object_format

        # Build a mock business object data key
        business_object_data_key = \
            herd_sdk.BusinessObjectDataKey(namespace='NAMESPACE',
                                           business_object_definition_name='BUS_OBJ_DEF_NAME',
                                           business_object_format_usage='USAGE',
                                           business_object_format_file_type='F_TYPE',
                                           business_object_format_version=2,
                                           partition_value='2019-01-30',
                                           sub_partition_values=None,
                                           business_object_data_version=3)

        # Create the mock business object data
        business_object_data = herd_sdk.BusinessObjectDataKeys([business_object_data_key])

        # Set the mock return value
        business_object_data_api_mock.return_value = business_object_data

        # Set the mock return value
        pandas_mock.return_value = pd.DataFrame(index=[0], columns=['ISSUE_SYM_ID']).fillna('I')

        # Call the method under test
        data_frame = self.entity.get_file('BUCKET', 'USAGE', 'F_TYPE', '2019-01-30', '000000_0.bz2')
        self.assertEqual('I', data_frame['ISSUE_SYM_ID'].values[0])
        assert business_object_data_api_mock \
            is herd_sdk.BusinessObjectDataApi. \
            business_object_data_get_all_business_object_data_by_business_object_definition
        assert business_object_data_api_mock.called_once
        business_object_data_api_mock.assert_called_once_with('NAMESPACE', 'BUS_OBJ_DEF_NAME')
        assert business_object_format_api_mock \
            is herd_sdk.BusinessObjectFormatApi.business_object_format_get_business_object_format
        assert business_object_format_api_mock.called_twice
        business_object_format_api_mock.assert_called_with('NAMESPACE',
                                                           'BUS_OBJ_DEF_NAME',
                                                           'USAGE',
                                                           'F_TYPE')
        assert column_api_mock \
               is herd_sdk.BusinessObjectDefinitionColumnApi. \
            business_object_definition_column_search_business_object_definition_columns
        assert column_api_mock.called_once
        column_search_key = \
            herd_sdk.BusinessObjectDefinitionColumnSearchKey(
                "NAMESPACE", "BUS_OBJ_DEF_NAME")
        column_search_filter = \
            herd_sdk.BusinessObjectDefinitionColumnSearchFilter(
                [column_search_key])
        column_search_request = herd_sdk.BusinessObjectDefinitionColumnSearchRequest(
            business_object_definition_column_search_filters=[column_search_filter])
        column_api_mock. \
            assert_called_once_with(column_search_request, fields='schemaColumnName')
        assert pandas_mock is pd.read_csv
        assert pandas_mock.called_once
        # pylint: disable=line-too-long
        pandas_mock.assert_called_once_with("s3://BUCKET/namespace/dpn/usage/f_type/bus_obj_def_name/schm-v2/data-v3/partition-key=2019-01-30/000000_0.bz2", delimiter="|", low_memory=False, names=['SCHEMA_COLUMN_NAME'])
