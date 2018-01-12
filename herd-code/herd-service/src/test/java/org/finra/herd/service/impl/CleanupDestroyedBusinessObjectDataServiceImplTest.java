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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.StorageUnitHelper;

/**
 * This class tests functionality within the cleanup destroyed business object data service implementation.
 */
public class CleanupDestroyedBusinessObjectDataServiceImplTest extends AbstractServiceTest
{
    @Mock
    private BusinessObjectDataDao mockBusinessObjectDataDao;

    @Mock
    private BusinessObjectDataDaoHelper mockBusinessObjectDataDaoHelper;

    @Mock
    private BusinessObjectDataHelper mockBusinessObjectDataHelper;

    @InjectMocks
    private CleanupDestroyedBusinessObjectDataServiceImpl cleanupDestroyedBusinessObjectDataService;

    @Mock
    private NotificationEventService mockNotificationEventService;

    @Mock
    private StorageUnitDao mockStorageUnitDao;

    @Mock
    private StorageUnitHelper mockStorageUnitHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCleanupS3StorageUnit()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage unit key.
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey =
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME);

        // Create mocks
        BusinessObjectDataEntity businessObjectDataEntity = mock(BusinessObjectDataEntity.class);

        // Mock the external calls.
        when(mockBusinessObjectDataHelper.createBusinessObjectDataKeyFromStorageUnitKey(businessObjectDataStorageUnitKey)).thenReturn(businessObjectDataKey);
        when(mockBusinessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(mockBusinessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity)).thenReturn(businessObjectDataKey);
        when(businessObjectDataEntity.getLatestVersion()).thenReturn(true);
        when(mockBusinessObjectDataDao.getBusinessObjectDataMaxVersion(businessObjectDataKey)).thenReturn(BUSINESS_OBJECT_DATA_MAX_VERSION);
        when(mockBusinessObjectDataDao.getBusinessObjectDataByAltKey(
            new BusinessObjectDataKey(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(),
                businessObjectDataKey.getSubPartitionValues(), BUSINESS_OBJECT_DATA_MAX_VERSION))).thenReturn(businessObjectDataEntity);

        // Call the method under test.
        cleanupDestroyedBusinessObjectDataService.cleanupS3StorageUnit(businessObjectDataStorageUnitKey);

        // Verify the external calls.
        verify(mockBusinessObjectDataHelper).createBusinessObjectDataKeyFromStorageUnitKey(businessObjectDataStorageUnitKey);
        verify(mockBusinessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);

        verify(businessObjectDataEntity).getStorageUnits();

        verify(mockBusinessObjectDataDao).delete(businessObjectDataEntity);

        verify(mockNotificationEventService)
            .processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG, businessObjectDataKey,
                STORAGE_NAME, null, StorageUnitStatusEntity.DISABLED);
        verify(mockNotificationEventService)
            .processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG, businessObjectDataKey, null,
                BusinessObjectDataStatusEntity.DELETED);

        verify(businessObjectDataEntity).getBusinessObjectDataChildren();
        verify(businessObjectDataEntity).getBusinessObjectDataParents();

        verify(businessObjectDataEntity).getLatestVersion();
        verify(mockBusinessObjectDataDao).getBusinessObjectDataMaxVersion(businessObjectDataKey);
        verify(mockBusinessObjectDataDao).getBusinessObjectDataByAltKey(
            new BusinessObjectDataKey(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(),
                businessObjectDataKey.getSubPartitionValues(), BUSINESS_OBJECT_DATA_MAX_VERSION));

        verify(businessObjectDataEntity).setLatestVersion(true);
        verify(mockBusinessObjectDataDao).saveAndRefresh(businessObjectDataEntity);

        verifyNoMoreInteractions(businessObjectDataEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCleanupS3StorageUnitWithIllegalArgumentException()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a storage unit key.
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey =
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME);

        // Create mocks
        BusinessObjectDataEntity businessObjectDataEntity = mock(BusinessObjectDataEntity.class);

        // Create storage units
        StoragePlatformEntity storagePlatformEntity = new StoragePlatformEntity();
        storagePlatformEntity.setName(StoragePlatformEntity.S3);
        StorageEntity storageEntity = new StorageEntity();
        storageEntity.setStoragePlatform(storagePlatformEntity);
        StorageUnitEntity storageUnitEntity1 = new StorageUnitEntity();
        storageUnitEntity1.setStorage(storageEntity);
        StorageUnitEntity storageUnitEntity2 = new StorageUnitEntity();
        storageUnitEntity2.setStorage(storageEntity);

        Collection<StorageUnitEntity> storageUnitEntities = Lists.newArrayList();
        storageUnitEntities.add(storageUnitEntity1);
        storageUnitEntities.add(storageUnitEntity2);

        // Mock the external calls.
        when(mockBusinessObjectDataHelper.createBusinessObjectDataKeyFromStorageUnitKey(businessObjectDataStorageUnitKey)).thenReturn(businessObjectDataKey);
        when(mockBusinessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataEntity.getStorageUnits()).thenReturn(storageUnitEntities);
        when(mockBusinessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey))
            .thenReturn(businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey));

        // Call the method under test.
        try
        {
            cleanupDestroyedBusinessObjectDataService.cleanupS3StorageUnit(businessObjectDataStorageUnitKey);
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            assertThat(illegalArgumentException.getMessage(), is(equalTo("Business object data has multiple (2) S3 storage units. Business object data: {" +
                businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey) + "}")));
        }

        // Verify the external calls.
        verify(mockBusinessObjectDataHelper).createBusinessObjectDataKeyFromStorageUnitKey(businessObjectDataStorageUnitKey);
        verify(mockBusinessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(businessObjectDataEntity).getStorageUnits();
        verify(mockBusinessObjectDataHelper).businessObjectDataKeyToString(businessObjectDataKey);

        verifyNoMoreInteractions(businessObjectDataEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetS3StorageUnitsToCleanup()
    {
        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity = new StorageUnitEntity();

        // Create a list of storage unit entities.
        List<StorageUnitEntity> storageUnitEntities = Collections.singletonList(storageUnitEntity);

        // Create a storage unit key.
        BusinessObjectDataStorageUnitKey storageUnitKey =
            new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME);

        // Mock the external calls.
        when(mockStorageUnitDao.getS3StorageUnitsToCleanup(MAX_RESULT)).thenReturn(storageUnitEntities);
        when(mockStorageUnitHelper.createStorageUnitKeyFromEntity(storageUnitEntity)).thenReturn(storageUnitKey);

        // Call the method under test.
        List<BusinessObjectDataStorageUnitKey> result = cleanupDestroyedBusinessObjectDataService.getS3StorageUnitsToCleanup(MAX_RESULT);

        // Verify the external calls.
        verify(mockStorageUnitDao).getS3StorageUnitsToCleanup(MAX_RESULT);
        verify(mockStorageUnitHelper).createStorageUnitKeyFromEntity(storageUnitEntity);
        verifyNoMoreInteractionsHelper();

        // Validate the result.
        assertEquals(Collections.singletonList(storageUnitKey), result);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(mockBusinessObjectDataHelper, mockBusinessObjectDataDaoHelper, mockBusinessObjectDataDao, mockNotificationEventService,
            mockStorageUnitDao, mockStorageUnitHelper);
    }
}
