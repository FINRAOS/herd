/*
* Copyright 2015 herd contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.finra.herd.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

@Component
public class BusinessObjectDataAvailabilityTestHelper
{
    @Autowired
    private BusinessObjectDataDaoTestHelper businessObjectDataDaoTestHelper;

    @Autowired
    private BusinessObjectFormatDao businessObjectFormatDao;

    @Autowired
    private BusinessObjectFormatDaoTestHelper businessObjectFormatDaoTestHelper;

    @Autowired
    private SchemaColumnDaoTestHelper schemaColumnDaoTestHelper;

    @Autowired
    private StorageDao storageDao;

    @Autowired
    private StorageDaoTestHelper storageDaoTestHelper;

    @Autowired
    private StorageUnitDaoTestHelper storageUnitDaoTestHelper;

    @Autowired
    private StorageUnitStatusDao storageUnitStatusDao;

    /**
     * Creates relative database entities required for the business object data availability service unit tests.
     */
    public void createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(String partitionKeyGroupName, List<SchemaColumn> columns,
        List<SchemaColumn> partitionColumns, int partitionColumnPosition, List<String> subPartitionValues, boolean allowDuplicateBusinessObjectData)
    {
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(partitionKeyGroupName, columns, partitionColumns, partitionColumnPosition,
            subPartitionValues, allowDuplicateBusinessObjectData, Arrays.asList(AbstractDaoTest.STORAGE_NAME));
    }

    /**
     * Creates relative database entities required for the business object data availability service unit tests.
     *
     * @param partitionKeyGroupName the name of the partition key group
     */
    public void createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(String partitionKeyGroupName)
    {
        createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(partitionKeyGroupName, schemaColumnDaoTestHelper.getTestSchemaColumns(),
            schemaColumnDaoTestHelper.getTestPartitionColumns(), BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION,
            AbstractDaoTest.NO_SUBPARTITION_VALUES, AbstractDaoTest.ALLOW_DUPLICATE_BUSINESS_OBJECT_DATA);
    }

    /**
     * Creates relative database entities required for the business object data availability service unit tests.
     *
     * @param partitionKeyGroupName the partition key group name
     * @param columns the list of schema columns
     * @param partitionColumns the list of schema partition columns
     * @param partitionColumnPosition the position of the partition column (1-based numbering) that will be changing
     * @param subPartitionValues the list of sub-partition values to be used in test business object data generation
     * @param allowDuplicateBusinessObjectData specifies if business object data is allowed to be registered in multiple storages
     * @param expectedRequestStorageNames the list of storage names expected to be listed in the relative unit test availability requests when queering business
     * object data availability. This list will be used to produce the ordered list of expected available storage units.
     *
     * @return the ordered list of storage unit entities expected to be available across the specified list of storages
     */
    public List<StorageUnitEntity> createDatabaseEntitiesForBusinessObjectDataAvailabilityTesting(String partitionKeyGroupName, List<SchemaColumn> columns,
        List<SchemaColumn> partitionColumns, int partitionColumnPosition, List<String> subPartitionValues, boolean allowDuplicateBusinessObjectData,
        List<String> expectedRequestStorageNames)
    {
        List<StorageUnitEntity> availableStorageUnits = new ArrayList<>();

        // Create relative database entities.
        String partitionKey = partitionColumns.isEmpty() ? AbstractDaoTest.PARTITION_KEY : partitionColumns.get(0).getName();

        // Create a business object format entity if it does not exist.
        if (businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME, AbstractDaoTest.FORMAT_USAGE_CODE,
                AbstractDaoTest.FORMAT_FILE_TYPE_CODE, AbstractDaoTest.FORMAT_VERSION)) == null)
        {
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME, AbstractDaoTest.FORMAT_USAGE_CODE,
                    AbstractDaoTest.FORMAT_FILE_TYPE_CODE, AbstractDaoTest.FORMAT_VERSION, AbstractDaoTest.FORMAT_DESCRIPTION,
                    AbstractDaoTest.FORMAT_DOCUMENT_SCHEMA, AbstractDaoTest.LATEST_VERSION_FLAG_SET, partitionKey, partitionKeyGroupName,
                    AbstractDaoTest.NO_ATTRIBUTES, AbstractDaoTest.SCHEMA_DELIMITER_PIPE, AbstractDaoTest.SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
                    AbstractDaoTest.SCHEMA_NULL_VALUE_BACKSLASH_N, columns, partitionColumns);
        }

        // Create storage entities if they do not exist.
        StorageEntity storageEntity1 = storageDao.getStorageByName(AbstractDaoTest.STORAGE_NAME);
        if (storageEntity1 == null)
        {
            storageEntity1 = storageDaoTestHelper.createStorageEntity(AbstractDaoTest.STORAGE_NAME);
        }
        StorageEntity storageEntity2 = storageDao.getStorageByName(AbstractDaoTest.STORAGE_NAME_2);
        if (storageEntity2 == null)
        {
            storageEntity2 = storageDaoTestHelper.createStorageEntity(AbstractDaoTest.STORAGE_NAME_2);
        }

        // Get storage status unit status entity.
        StorageUnitStatusEntity storageUnitStatusEntity = storageUnitStatusDao.getStorageUnitStatusByCode(StorageUnitStatusEntity.ENABLED);

        // Create business object data instances and relative storage units.
        for (String partitionValue : AbstractDaoTest.SORTED_PARTITION_VALUES)
        {
            BusinessObjectDataEntity businessObjectDataEntity;

            // Create a business object data instance for the specified partition value.
            if (partitionColumnPosition == BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION)
            {
                businessObjectDataEntity = businessObjectDataDaoTestHelper
                    .createBusinessObjectDataEntity(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME, AbstractDaoTest.FORMAT_USAGE_CODE,
                        AbstractDaoTest.FORMAT_FILE_TYPE_CODE, AbstractDaoTest.FORMAT_VERSION, partitionValue, subPartitionValues, AbstractDaoTest.DATA_VERSION,
                        AbstractDaoTest.LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
            }
            else
            {
                List<String> testSubPartitionValues = new ArrayList<>(subPartitionValues);
                // Please note that the second partition column is located at index 0.
                testSubPartitionValues.set(partitionColumnPosition - 2, partitionValue);
                businessObjectDataEntity = businessObjectDataDaoTestHelper
                    .createBusinessObjectDataEntity(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME, AbstractDaoTest.FORMAT_USAGE_CODE,
                        AbstractDaoTest.FORMAT_FILE_TYPE_CODE, AbstractDaoTest.FORMAT_VERSION, AbstractDaoTest.PARTITION_VALUE, testSubPartitionValues,
                        AbstractDaoTest.DATA_VERSION, AbstractDaoTest.LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);
            }

            // Check if we need to create the relative storage units.
            if (AbstractDaoTest.STORAGE_1_AVAILABLE_PARTITION_VALUES.contains(partitionValue))
            {
                StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
                    .createStorageUnitEntity(storageEntity1, businessObjectDataEntity, storageUnitStatusEntity, AbstractDaoTest.NO_STORAGE_DIRECTORY_PATH);

                if (expectedRequestStorageNames.contains(AbstractDaoTest.STORAGE_NAME))
                {
                    availableStorageUnits.add(storageUnitEntity);
                }
            }

            if (AbstractDaoTest.STORAGE_2_AVAILABLE_PARTITION_VALUES.contains(partitionValue) &&
                (allowDuplicateBusinessObjectData || !AbstractDaoTest.STORAGE_1_AVAILABLE_PARTITION_VALUES.contains(partitionValue)))
            {
                StorageUnitEntity storageUnitEntity = storageUnitDaoTestHelper
                    .createStorageUnitEntity(storageEntity2, businessObjectDataEntity, storageUnitStatusEntity, AbstractDaoTest.NO_STORAGE_DIRECTORY_PATH);

                if (expectedRequestStorageNames.contains(AbstractDaoTest.STORAGE_NAME_2))
                {
                    availableStorageUnits.add(storageUnitEntity);
                }
            }
        }

        return availableStorageUnits;
    }
}
