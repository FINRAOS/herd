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
package org.finra.herd.service.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitCreateResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.StorageDirectory;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StorageFileHelper;

/**
 * This class tests functionality within the business object data storage unit service implementation.
 */
public class BusinessObjectDataStorageUnitServiceImplTest extends AbstractServiceTest
{
    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @InjectMocks
    private BusinessObjectDataStorageUnitServiceImpl businessObjectDataStorageUnitServiceImpl;

    @Mock
    private StorageDaoHelper storageDaoHelper;

    @Mock
    private StorageFileHelper storageFileHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateBusinessObjectDataStorageUnit()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data storage key.
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey =
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME);

        // Create a storage unit directory.
        StorageDirectory storageDirectory = new StorageDirectory(STORAGE_DIRECTORY_PATH);

        // Create a list of storage files.
        List<StorageFile> storageFiles =
            Arrays.asList(new StorageFile(FILE_NAME, FILE_SIZE, ROW_COUNT), new StorageFile(FILE_NAME_2, FILE_SIZE_2, ROW_COUNT_2));

        // Create a business object data storage unit request.
        BusinessObjectDataStorageUnitCreateRequest request =
            new BusinessObjectDataStorageUnitCreateRequest(businessObjectDataStorageUnitKey, storageDirectory, storageFiles, NO_DISCOVER_STORAGE_FILES);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setId(ID);

        // Create a storage entity.
        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);

        // Create a list of storage file entities.
        List<StorageFileEntity> storageFileEntities = Arrays.asList(new StorageFileEntity(), new StorageFileEntity());

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setBusinessObjectData(businessObjectDataEntity);
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setDirectoryPath(STORAGE_DIRECTORY_PATH);
        storageUnitEntity.setStorageFiles(storageFileEntities);

        // Create expected business object data storage unit response.
        BusinessObjectDataStorageUnitCreateResponse businessObjectDataStorageUnitCreateResponse =
            new BusinessObjectDataStorageUnitCreateResponse(businessObjectDataStorageUnitKey, storageDirectory, storageFiles);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("namespace", BDEF_NAMESPACE)).thenReturn(BDEF_NAMESPACE);
        when(alternateKeyHelper.validateStringParameter("business object definition name", BDEF_NAME)).thenReturn(BDEF_NAME);
        when(alternateKeyHelper.validateStringParameter("business object format usage", FORMAT_USAGE_CODE)).thenReturn(FORMAT_USAGE_CODE);
        when(alternateKeyHelper.validateStringParameter("business object format file type", FORMAT_FILE_TYPE_CODE)).thenReturn(FORMAT_FILE_TYPE_CODE);
        when(alternateKeyHelper.validateStringParameter("partition value", PARTITION_VALUE)).thenReturn(PARTITION_VALUE);
        when(alternateKeyHelper.validateStringParameter("storage name", STORAGE_NAME)).thenReturn(STORAGE_NAME);
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(storageDaoHelper.getStorageEntity(STORAGE_NAME)).thenReturn(storageEntity);
        when(businessObjectDataDaoHelper
            .createStorageUnitEntity(businessObjectDataEntity, storageEntity, storageDirectory, storageFiles, NO_DISCOVER_STORAGE_FILES))
            .thenReturn(storageUnitEntity);
        when(businessObjectDataHelper.createBusinessObjectDataKeyFromEntity(businessObjectDataEntity)).thenReturn(businessObjectDataKey);
        when(storageFileHelper.createStorageFilesFromEntities(storageFileEntities)).thenReturn(storageFiles);

        // Call the method under test.
        BusinessObjectDataStorageUnitCreateResponse result = businessObjectDataStorageUnitServiceImpl.createBusinessObjectDataStorageUnit(request);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("namespace", BDEF_NAMESPACE);
        verify(alternateKeyHelper).validateStringParameter("business object definition name", BDEF_NAME);
        verify(alternateKeyHelper).validateStringParameter("business object format usage", FORMAT_USAGE_CODE);
        verify(alternateKeyHelper).validateStringParameter("business object format file type", FORMAT_FILE_TYPE_CODE);
        verify(alternateKeyHelper).validateStringParameter("partition value", PARTITION_VALUE);
        verify(businessObjectDataHelper).validateSubPartitionValues(SUBPARTITION_VALUES);
        verify(alternateKeyHelper).validateStringParameter("storage name", STORAGE_NAME);
        verify(storageFileHelper).validateCreateRequestStorageFiles(storageFiles);
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(storageDaoHelper).getStorageEntity(STORAGE_NAME);
        verify(businessObjectDataDaoHelper)
            .createStorageUnitEntity(businessObjectDataEntity, storageEntity, storageDirectory, storageFiles, NO_DISCOVER_STORAGE_FILES);
        verify(businessObjectDataHelper).createBusinessObjectDataKeyFromEntity(businessObjectDataEntity);
        verify(storageFileHelper).createStorageFilesFromEntities(storageFileEntities);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectDataStorageUnitCreateResponse, result);
    }

    @Test
    public void testValidateBusinessObjectDataStorageUnitCreateRequestDirectoryOnlyRegistration()
    {
        // Call the method under test.
        businessObjectDataStorageUnitServiceImpl.validateBusinessObjectDataStorageUnitCreateRequest(new BusinessObjectDataStorageUnitCreateRequest(
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME), new StorageDirectory(STORAGE_DIRECTORY_PATH), NO_STORAGE_FILES, DISCOVER_STORAGE_FILES));

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("namespace", BDEF_NAMESPACE);
        verify(alternateKeyHelper).validateStringParameter("business object definition name", BDEF_NAME);
        verify(alternateKeyHelper).validateStringParameter("business object format usage", FORMAT_USAGE_CODE);
        verify(alternateKeyHelper).validateStringParameter("business object format file type", FORMAT_FILE_TYPE_CODE);
        verify(alternateKeyHelper).validateStringParameter("partition value", PARTITION_VALUE);
        verify(businessObjectDataHelper).validateSubPartitionValues(SUBPARTITION_VALUES);
        verify(alternateKeyHelper).validateStringParameter("storage name", STORAGE_NAME);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testValidateBusinessObjectDataStorageUnitCreateRequestInvalidParameters()
    {
        // Try to call the method under test when create request is not specified.
        try
        {
            businessObjectDataStorageUnitServiceImpl.validateBusinessObjectDataStorageUnitCreateRequest(null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data storage unit create request must be specified.", e.getMessage());
        }

        // Try to call the method under test when auto-discovery is enabled and storage directory is not specified.
        try
        {
            businessObjectDataStorageUnitServiceImpl.validateBusinessObjectDataStorageUnitCreateRequest(new BusinessObjectDataStorageUnitCreateRequest(
                new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME), NO_STORAGE_DIRECTORY, NO_STORAGE_FILES, DISCOVER_STORAGE_FILES));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage directory must be specified when discovery of storage files is enabled.", e.getMessage());
        }

        // Try to call the method under test when auto-discovery is enabled and storage files are specified.
        try
        {
            businessObjectDataStorageUnitServiceImpl.validateBusinessObjectDataStorageUnitCreateRequest(new BusinessObjectDataStorageUnitCreateRequest(
                new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME), new StorageDirectory(STORAGE_DIRECTORY_PATH),
                Collections.singletonList(new StorageFile(FILE_NAME, FILE_SIZE, ROW_COUNT)), DISCOVER_STORAGE_FILES));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Storage files cannot be specified when discovery of storage files is enabled.", e.getMessage());
        }

        // Try to call the method under test when auto-discovery is not enabled and no storage directory or files are specified.
        try
        {
            businessObjectDataStorageUnitServiceImpl.validateBusinessObjectDataStorageUnitCreateRequest(new BusinessObjectDataStorageUnitCreateRequest(
                new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME), NO_STORAGE_DIRECTORY, NO_STORAGE_FILES, NO_DISCOVER_STORAGE_FILES));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage directory or at least one storage file must be specified when discovery of storage files is not enabled.", e.getMessage());
        }

        // Try to call the method under test when storage directory is specified with a blank storage directory path.
        try
        {
            businessObjectDataStorageUnitServiceImpl.validateBusinessObjectDataStorageUnitCreateRequest(new BusinessObjectDataStorageUnitCreateRequest(
                new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME), new StorageDirectory(BLANK_TEXT), NO_STORAGE_FILES, NO_DISCOVER_STORAGE_FILES));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage directory path must be specified.", e.getMessage());
        }
    }

    @Test
    public void testValidateBusinessObjectDataStorageUnitKeyInvalidParameters()
    {
        // Try to call the method under test when business object data storage unit key is not specified.
        try
        {
            businessObjectDataStorageUnitServiceImpl.validateBusinessObjectDataStorageUnitKey(null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data storage unit key must be specified.", e.getMessage());
        }

        // Try to call the method under test when business object format version is not specified.
        try
        {
            businessObjectDataStorageUnitServiceImpl.validateBusinessObjectDataStorageUnitKey(
                new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to call the method under test when business object data version is not specified.
        try
        {
            businessObjectDataStorageUnitServiceImpl.validateBusinessObjectDataStorageUnitKey(
                new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, NO_DATA_VERSION, STORAGE_NAME));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data version must be specified.", e.getMessage());
        }
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDataDaoHelper, businessObjectDataHelper, storageDaoHelper, storageFileHelper);
    }
}
