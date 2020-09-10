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
package org.finra.herd.service.helper;

import static org.finra.herd.dao.AbstractDaoTest.BDEF_NAME;
import static org.finra.herd.dao.AbstractDaoTest.DATA_VERSION;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_FILE_TYPE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_USAGE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_VERSION;
import static org.finra.herd.dao.AbstractDaoTest.LOCAL_FILE;
import static org.finra.herd.dao.AbstractDaoTest.NAMESPACE;
import static org.finra.herd.dao.AbstractDaoTest.PARTITION_VALUE;
import static org.finra.herd.dao.AbstractDaoTest.STORAGE_NAME;
import static org.finra.herd.dao.AbstractDaoTest.SUBPARTITION_VALUES;
import static org.finra.herd.dao.AbstractDaoTest.TEST_S3_KEY_PREFIX;
import static org.finra.herd.service.AbstractServiceTest.DIRECTORY_PATH;
import static org.finra.herd.service.AbstractServiceTest.FILE_NAME;
import static org.finra.herd.service.AbstractServiceTest.FILE_NAME_2;
import static org.finra.herd.service.AbstractServiceTest.FILE_SIZE;
import static org.finra.herd.service.AbstractServiceTest.FILE_SIZE_2;
import static org.finra.herd.service.AbstractServiceTest.ROW_COUNT;
import static org.finra.herd.service.AbstractServiceTest.ROW_COUNT_2;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.StorageFileDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageFile;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StorageFileEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;

public class StorageFileDaoHelperTest
{
    @Captor
    private ArgumentCaptor<List<StorageFileEntity>> argumentCaptor;

    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Mock
    private StorageFileDao storageFileDao;

    @InjectMocks
    private StorageFileDaoHelper storageFileDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateStorageFileEntitiesFromStorageFiles()
    {
        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();

        // Create a list of storage files
        List<StorageFile> storageFiles =
            Lists.newArrayList(new StorageFile(FILE_NAME, FILE_SIZE, ROW_COUNT), new StorageFile(FILE_NAME_2, FILE_SIZE_2, ROW_COUNT_2));

        // Call the method under test.
        List<StorageFileEntity> result = storageFileDaoHelper.createStorageFileEntitiesFromStorageFiles(storageUnitEntity, storageFiles, DIRECTORY_PATH);

        // Validate the results.
        assertThat("Result size not equal to two.", result.size(), is(2));
        assertThat("File size not equal.", result.get(0).getFileSizeBytes(), is(FILE_SIZE));
        assertThat("Row count not equal.", result.get(0).getRowCount(), is(ROW_COUNT));
        assertThat("File size not equal.", result.get(1).getFileSizeBytes(), is(FILE_SIZE_2));
        assertThat("Row count not equal.", result.get(1).getRowCount(), is(ROW_COUNT_2));

        // Verify the external calls.
        verify(storageFileDao).saveStorageFiles(argumentCaptor.capture());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetStorageFileEntity()
    {
        // Create a storage file entity
        StorageFileEntity storageFileEntity = new StorageFileEntity();
        storageFileEntity.setPath(TEST_S3_KEY_PREFIX);
        storageFileEntity.setRowCount(ROW_COUNT);
        storageFileEntity.setFileSizeBytes(FILE_SIZE);

        // Create a file path.
        String filePath = TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE;

        // Mock the external calls.
        when(storageFileDao.getStorageFileByStorageNameAndFilePath(STORAGE_NAME, filePath)).thenReturn(storageFileEntity);

        // Call the method under test.
        StorageFileEntity result = storageFileDaoHelper.getStorageFileEntity(STORAGE_NAME, filePath);

        // Validate the results.
        assertThat("Result not equal to storage file entity.", result, is(storageFileEntity));

        // Verify the external calls.
        verify(storageFileDao).getStorageFileByStorageNameAndFilePath(STORAGE_NAME, filePath);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetStorageFileEntityWithNullStorageFileEntity()
    {
        // Create a file path.
        String filePath = TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE;

        // Mock the external calls.
        when(storageFileDao.getStorageFileByStorageNameAndFilePath(STORAGE_NAME, filePath)).thenReturn(null);

        try
        {
            // Call the method under test.
            storageFileDaoHelper.getStorageFileEntity(STORAGE_NAME, filePath);
            fail();
        }
        catch (ObjectNotFoundException objectNotFoundException)
        {
            // Validate the exception message.
            assertThat("Exception message not equal to expected exception message.", objectNotFoundException.getMessage(),
                is(String.format("Storage file \"%s\" doesn't exist in \"%s\" storage.", filePath, STORAGE_NAME)));
        }

        // Verify the external calls.
        verify(storageFileDao).getStorageFileByStorageNameAndFilePath(STORAGE_NAME, filePath);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetStorageFileEntityByStorageUnitEntityAndFilePath()
    {
        // Create a new business object data key
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();

        // Create a file path.
        String filePath = TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE;

        // Create a storage file entity
        StorageFileEntity storageFileEntity = new StorageFileEntity();
        storageFileEntity.setPath(TEST_S3_KEY_PREFIX);
        storageFileEntity.setRowCount(ROW_COUNT);
        storageFileEntity.setFileSizeBytes(FILE_SIZE);

        // Mock the external calls.
        when(storageFileDao.getStorageFileByStorageUnitEntityAndFilePath(storageUnitEntity, filePath)).thenReturn(storageFileEntity);

        // Call the method under test.
        StorageFileEntity result = storageFileDaoHelper.getStorageFileEntity(storageUnitEntity, filePath, businessObjectDataKey);

        // Validate the results.
        assertThat("Result not equal to storage file entity.", result, is(storageFileEntity));

        // Verify the external calls.
        verify(storageFileDao).getStorageFileByStorageUnitEntityAndFilePath(storageUnitEntity, filePath);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetStorageFileEntityByStorageUnitEntityAndFilePathWithFileOnlyPrefix()
    {
        // Create a new business object data key
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage unit entity.
        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setDirectoryPath(TEST_S3_KEY_PREFIX);

        // Create a file path.
        String filePath = TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE;

        // Create a storage file entity
        StorageFileEntity storageFileEntity = new StorageFileEntity();
        storageFileEntity.setPath(TEST_S3_KEY_PREFIX);
        storageFileEntity.setRowCount(ROW_COUNT);
        storageFileEntity.setFileSizeBytes(FILE_SIZE);

        // Mock the external calls.
        when(storageFileDao.getStorageFileByStorageUnitEntityAndFilePath(storageUnitEntity, filePath)).thenReturn(null);
        when(storageFileDao.getStorageFileByStorageUnitEntityAndFilePath(storageUnitEntity, LOCAL_FILE)).thenReturn(storageFileEntity);

        // Call the method under test.
        StorageFileEntity result = storageFileDaoHelper.getStorageFileEntity(storageUnitEntity, filePath, businessObjectDataKey);

        // Validate the results.
        assertThat("Result not equal to storage file entity.", result, is(storageFileEntity));

        // Verify the external calls.
        verify(storageFileDao).getStorageFileByStorageUnitEntityAndFilePath(storageUnitEntity, filePath);
        verify(storageFileDao).getStorageFileByStorageUnitEntityAndFilePath(storageUnitEntity, LOCAL_FILE);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetStorageFileEntityByStorageUnitEntityAndFilePathWithNullStorageFileEntity()
    {
        // Create a new business object data key
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage unit entity.
        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStorage(storageEntity);
        storageUnitEntity.setDirectoryPath(DIRECTORY_PATH);

        // Create a file path.
        String filePath = TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE;

        // Mock the external calls.
        when(storageFileDao.getStorageFileByStorageUnitEntityAndFilePath(storageUnitEntity, filePath)).thenReturn(null);

        try
        {
            // Call the method under test.
            storageFileDaoHelper.getStorageFileEntity(storageUnitEntity, filePath, businessObjectDataKey);
            fail();
        }
        catch (ObjectNotFoundException objectNotFoundException)
        {
            // Validate the exception message.
            assertThat("Exception message not equal to expected exception message.", objectNotFoundException.getMessage(), is(String
                .format("Storage file \"%s\" doesn't exist in \"%s\" storage. Business object data: {%s}", filePath, storageUnitEntity.getStorage().getName(),
                    businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey))));
        }

        // Verify the external calls.
        verify(storageFileDao).getStorageFileByStorageUnitEntityAndFilePath(storageUnitEntity, filePath);
        verify(businessObjectDataHelper, times(2)).businessObjectDataKeyToString(businessObjectDataKey);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetStorageFileEntityByStorageUnitEntityAndFilePathWithNullStorageFileEntityWithBlankDirectoryPath()
    {
        // Create a new business object data key
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage unit entity.
        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setName(STORAGE_NAME);
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();
        storageUnitEntity.setStorage(storageEntity);

        // Create a file path.
        String filePath = TEST_S3_KEY_PREFIX + "/" + LOCAL_FILE;

        // Mock the external calls.
        when(storageFileDao.getStorageFileByStorageUnitEntityAndFilePath(storageUnitEntity, filePath)).thenReturn(null);

        try
        {
            // Call the method under test.
            storageFileDaoHelper.getStorageFileEntity(storageUnitEntity, filePath, businessObjectDataKey);
            fail();
        }
        catch (ObjectNotFoundException objectNotFoundException)
        {
            // Validate the exception message.
            assertThat("Exception message not equal to expected exception message.", objectNotFoundException.getMessage(), is(String
                .format("Storage file \"%s\" doesn't exist in \"%s\" storage. Business object data: {%s}", filePath, storageUnitEntity.getStorage().getName(),
                    businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey))));
        }

        // Verify the external calls.
        verify(storageFileDao).getStorageFileByStorageUnitEntityAndFilePath(storageUnitEntity, filePath);
        verify(businessObjectDataHelper, times(2)).businessObjectDataKeyToString(businessObjectDataKey);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectDataHelper, storageFileDao);
    }
}
