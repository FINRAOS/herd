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

import herd_sdk
import pandas as pd


class Entity(object):
    """
     The Entity (Business Object) class.
    """

    def __init__(self, configuration, namespace, name):
        """
        The init method for the entity class.

        :param configuration: the api client configuration used by the herd sdk
        :param namespace: the namespace for this entity
        :param name: the name of the entity
        """

        # Create an instance of the Herd API Client
        herd_client = herd_sdk.ApiClient(configuration)

        # Business Object Definition API
        self.business_object_definition_api = herd_sdk.BusinessObjectDefinitionApi(herd_client)

        # Business Object Data API
        self.business_object_data_api = herd_sdk.BusinessObjectDataApi(herd_client)

        # Business Object Format API
        self.business_object_format_api = herd_sdk.BusinessObjectFormatApi(herd_client)

        # Business Object Definition Column API
        self.column_api =\
            herd_sdk.BusinessObjectDefinitionColumnApi(herd_client)

        self.namespace = namespace
        self.name = name

    def get_columns(self):
        """
        Get the business object definition columns associated with this entity.

        :return: the business object definition columns
        """

        column_search_key =\
            herd_sdk.BusinessObjectDefinitionColumnSearchKey(
                self.namespace, self.name)
        column_search_filter =\
            herd_sdk.BusinessObjectDefinitionColumnSearchFilter(
                [column_search_key])
        column_search_request =\
            herd_sdk.BusinessObjectDefinitionColumnSearchRequest(
                [column_search_filter])
        column_search_response = self.\
            column_api.business_object_definition_column_search_business_object_definition_columns(
                column_search_request, fields='schemaColumnName')

        columns = [{'PhysicalName':
                    business_object_definition_column.schema_column_name,
                    'BusinessName':
                    business_object_definition_column.business_object_definition_column_key.
                        business_object_definition_column_name}
                   for business_object_definition_column in
                   column_search_response.
                   business_object_definition_columns]

        return pd.DataFrame(columns)

    def list_registered_data(self, *partition_values):
        """
        Get a list of registered data associated with this entity.

        :param partition_values: the partition values
        :return: the list of registered data
        """

        business_object_data_keys = self.business_object_data_api.\
            business_object_data_get_all_business_object_data_by_business_object_definition(
                self.namespace, self.name)
        data_keys = [{'dataVersion': business_object_data_key.business_object_data_version,
                      'fileType': business_object_data_key.business_object_format_file_type,
                      'formatUsage': business_object_data_key.business_object_format_usage,
                      'formatVersion': business_object_data_key.business_object_format_version,
                      'name': business_object_data_key.business_object_definition_name,
                      'partitionValue': business_object_data_key.partition_value,
                      'subPartitionValues': business_object_data_key.sub_partition_values}
                     for business_object_data_key in
                     business_object_data_keys.business_object_data_keys]

        bo_data = pd.DataFrame(data_keys)

        bo_data['ranking'] = bo_data.groupby('partitionValue')['dataVersion'].\
            rank(ascending=False).astype(int)
        bo_data = bo_data.loc[bo_data.ranking == 1, :]

        if partition_values:
            bo_data = bo_data[bo_data.partitionValue.isin(partition_values)]

        return bo_data[['dataVersion',
                        'fileType',
                        'formatUsage',
                        'formatVersion',
                        'partitionValue',
                        'subPartitionValues']].reset_index(drop=True)

    def get_data_version(self, *partition_values):
        """
        Get the data version.

        :param partition_values: the partition value
        :return: the data version
        """

        registered_data = self.list_registered_data(*partition_values).copy()\
            .set_index('partitionValue')

        return registered_data.dataVersion[0].item()

    def get_prefix(self, format_usage, file_type, partition):
        """
        Get the prefix used to find the business object data

        :param format_usage: the format usage
        :param file_type: the file type
        :param partition: the partition value
        :return: the prefix
        """

        business_object_data_version = self.get_data_version(partition)
        business_object_format = self.business_object_format_api.\
            business_object_format_get_business_object_format(self.namespace,
                                                              self.name,
                                                              format_usage,
                                                              file_type)
        business_object_format_version = business_object_format.business_object_format_version

        s3_key_prefix_information = self.business_object_data_api \
            .business_object_data_get_s3_key_prefix(
                self.namespace, self.name, format_usage, file_type,
                business_object_format_version,
                business_object_data_version=business_object_data_version,
                partition_value=partition)

        return s3_key_prefix_information.s3_key_prefix

    @staticmethod
    def s3_url(key):
        """
        Get the s3 url.

        :param key: the key value to prefix with the s3 url scheme
        :return: the s3 url
        """

        return 's3://{}'.format(key)

    # pylint: disable=too-many-arguments
    def get_file(self, bucket, format_usage, file_type, partition, filename):
        """
        Get a file associated with this entity.

        :param bucket: the bucket name
        :param format_usage: the format usage
        :param file_type: the file type
        :param partition: the partition value
        :param filename: the filename
        :return: the file in a pandas data frame
        """

        prefix = self.get_prefix(format_usage, file_type, partition)
        url = self.s3_url('{}/{}/{}'.format(bucket, prefix, filename))
        columns = self.get_columns()
        cols = columns['PhysicalName'].values.tolist()
        fmt = self.business_object_format_api.\
            business_object_format_get_business_object_format(self.namespace,
                                                              self.name,
                                                              format_usage,
                                                              file_type)

        return pd.read_csv(url, delimiter=fmt.schema.delimiter, names=cols, low_memory=False)

    def __repr__(self):
        """
        The string representation of this python class.

        :return: the entity namespace and name
        """

        return '{}/{}'.format(self.namespace, self.name)
