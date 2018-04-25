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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributesUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataRetentionInformationUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.dto.BusinessObjectDataDestroyDto;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.RetentionTypeEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.BusinessObjectDataInitiateDestroyHelperService;
import org.finra.herd.service.BusinessObjectDataInitiateRestoreHelperService;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.S3Service;
import org.finra.herd.service.helper.AttributeDaoHelper;
import org.finra.herd.service.helper.AttributeHelper;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.BusinessObjectDataInvalidateUnregisteredHelper;
import org.finra.herd.service.helper.BusinessObjectDataRetryStoragePolicyTransitionHelper;
import org.finra.herd.service.helper.BusinessObjectDataSearchHelper;
import org.finra.herd.service.helper.BusinessObjectDataStatusDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.BusinessObjectFormatDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatHelper;
import org.finra.herd.service.helper.CustomDdlDaoHelper;
import org.finra.herd.service.helper.DdlGeneratorFactory;
import org.finra.herd.service.helper.S3KeyPrefixHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitHelper;

/**
 * This class tests functionality within the business object data service implementation.
 */
public class BusinessObjectDataServiceImplTest extends AbstractServiceTest
{
    @Mock
    private AttributeDaoHelper attributeDaoHelper;

    @Mock
    private AttributeHelper attributeHelper;

    @Mock
    private BusinessObjectDataDao businessObjectDataDao;

    @Mock
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Mock
    private BusinessObjectDataInitiateDestroyHelperService businessObjectDataInitiateDestroyHelperService;

    @Mock
    private BusinessObjectDataInitiateRestoreHelperService businessObjectDataInitiateRestoreHelperService;

    @Mock
    private BusinessObjectDataInvalidateUnregisteredHelper businessObjectDataInvalidateUnregisteredHelper;

    @Mock
    private BusinessObjectDataRetryStoragePolicyTransitionHelper businessObjectDataRetryStoragePolicyTransitionHelper;

    @Mock
    private BusinessObjectDataSearchHelper businessObjectDataSearchHelper;

    @InjectMocks
    private BusinessObjectDataServiceImpl businessObjectDataServiceImpl;

    @Mock
    private BusinessObjectDataStatusDaoHelper businessObjectDataStatusDaoHelper;

    @Mock
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Mock
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Mock
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Mock
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private CustomDdlDaoHelper customDdlDaoHelper;

    @Mock
    private DdlGeneratorFactory ddlGeneratorFactory;

    @Mock
    private JsonHelper jsonHelper;

    @Mock
    private NotificationEventService notificationEventService;

    @Mock
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Mock
    private S3Service s3Service;

    @Mock
    private StorageDaoHelper storageDaoHelper;

    @Mock
    private StorageHelper storageHelper;

    @Mock
    private StorageUnitDao storageUnitDao;

    @Mock
    private StorageUnitHelper storageUnitHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testDestroyBusinessObjectData()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create multiple states of business object data destroy parameters DTO.
        List<BusinessObjectDataDestroyDto> businessObjectDataDestroyDtoStates = Arrays.asList(
            new BusinessObjectDataDestroyDto(businessObjectDataKey, STORAGE_NAME, BusinessObjectDataStatusEntity.DELETED, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.DISABLING, StorageUnitStatusEntity.ENABLED, S3_ENDPOINT, S3_BUCKET_NAME, S3_KEY_PREFIX, S3_OBJECT_TAG_KEY,
                S3_OBJECT_TAG_VALUE, S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME, BDATA_FINAL_DESTROY_DELAY_IN_DAYS),
            new BusinessObjectDataDestroyDto(businessObjectDataKey, STORAGE_NAME, BusinessObjectDataStatusEntity.DELETED, BusinessObjectDataStatusEntity.VALID,
                StorageUnitStatusEntity.DISABLED, StorageUnitStatusEntity.DISABLING, S3_ENDPOINT, S3_BUCKET_NAME, S3_KEY_PREFIX, S3_OBJECT_TAG_KEY,
                S3_OBJECT_TAG_VALUE, S3_OBJECT_TAGGER_ROLE_ARN, S3_OBJECT_TAGGER_ROLE_SESSION_NAME, BDATA_FINAL_DESTROY_DELAY_IN_DAYS));

        // Create a business object data.
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setId(ID);

        // Mock the external calls.
        doAnswer(invocation -> {
            BusinessObjectDataDestroyDto businessObjectDataDestroyDto = (BusinessObjectDataDestroyDto) invocation.getArguments()[0];
            businessObjectDataDestroyDtoStates.get(0).copyTo(businessObjectDataDestroyDto);
            return null;
        }).when(businessObjectDataInitiateDestroyHelperService).prepareToInitiateDestroy(new BusinessObjectDataDestroyDto(), businessObjectDataKey);
        doAnswer(invocation -> {
            BusinessObjectDataDestroyDto businessObjectDataDestroyDto = (BusinessObjectDataDestroyDto) invocation.getArguments()[0];
            businessObjectDataDestroyDtoStates.get(1).copyTo(businessObjectDataDestroyDto);
            return null;
        }).when(businessObjectDataInitiateDestroyHelperService).executeInitiateDestroyAfterStep(any(BusinessObjectDataDestroyDto.class));
        when(businessObjectDataInitiateDestroyHelperService.executeInitiateDestroyAfterStep(any(BusinessObjectDataDestroyDto.class)))
            .thenReturn(businessObjectData);

        // Call the method under test.
        BusinessObjectData result = businessObjectDataServiceImpl.destroyBusinessObjectData(businessObjectDataKey);

        // Verify the external calls.
        verify(businessObjectDataInitiateDestroyHelperService)
            .prepareToInitiateDestroy(any(BusinessObjectDataDestroyDto.class), any(BusinessObjectDataKey.class));
        verify(businessObjectDataInitiateDestroyHelperService).executeS3SpecificSteps(any(BusinessObjectDataDestroyDto.class));
        verify(businessObjectDataInitiateDestroyHelperService).executeInitiateDestroyAfterStep(any(BusinessObjectDataDestroyDto.class));
        verify(notificationEventService, times(2))
            .processStorageUnitNotificationEventAsync(any(NotificationEventTypeEntity.EventTypesStorageUnit.class), any(BusinessObjectDataKey.class),
                any(String.class), any(String.class), any(String.class));
        verify(notificationEventService)
            .processBusinessObjectDataNotificationEventAsync(any(NotificationEventTypeEntity.EventTypesBdata.class), any(BusinessObjectDataKey.class),
                any(String.class), any(String.class));
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectData, result);
    }

    @Test
    public void testUpdateBusinessObjectDataAttributes()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a list of attributes.
        List<Attribute> attributes = Arrays.asList(new Attribute(ATTRIBUTE_NAME, ATTRIBUTE_VALUE));

        // Create a business object data attributes update request.
        BusinessObjectDataAttributesUpdateRequest businessObjectDataAttributesUpdateRequest = new BusinessObjectDataAttributesUpdateRequest(attributes);

        // Create a list of attribute definitions.
        List<BusinessObjectDataAttributeDefinitionEntity> attributeDefinitionEntities = Arrays.asList(new BusinessObjectDataAttributeDefinitionEntity());

        // Create a business object format definition.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setAttributeDefinitions(attributeDefinitionEntities);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);

        // Create a business object data.
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setId(ID);

        // Mock the external calls.
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataDao.saveAndRefresh(businessObjectDataEntity)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity)).thenReturn(businessObjectData);

        // Call the method under test.
        BusinessObjectData result =
            businessObjectDataServiceImpl.updateBusinessObjectDataAttributes(businessObjectDataKey, businessObjectDataAttributesUpdateRequest);

        // Verify the external calls.
        verify(businessObjectDataHelper).validateBusinessObjectDataKey(businessObjectDataKey, true, true);
        verify(attributeHelper).validateAttributes(attributes);
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(attributeDaoHelper).validateAttributesAgainstBusinessObjectDataAttributeDefinitions(attributes, attributeDefinitionEntities);
        verify(attributeDaoHelper).updateBusinessObjectDataAttributes(businessObjectDataEntity, attributes);
        verify(businessObjectDataDao).saveAndRefresh(businessObjectDataEntity);
        verify(businessObjectDataHelper).createBusinessObjectDataFromEntity(businessObjectDataEntity);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectData, result);
    }

    @Test
    public void testUpdateBusinessObjectDataAttributesMissingRequiredParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Try to update business object data attributes when the update request is not specified.
        try
        {
            businessObjectDataServiceImpl.updateBusinessObjectDataAttributes(businessObjectDataKey, null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("A business object data attributes update request must be specified."), e.getMessage());
        }

        // Try to update business object data attributes when the list of attributes is not specified.
        try
        {
            businessObjectDataServiceImpl.updateBusinessObjectDataAttributes(businessObjectDataKey, new BusinessObjectDataAttributesUpdateRequest());
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("A list of business object data attributes must be specified."), e.getMessage());
        }

        // Verify the external calls.
        verify(businessObjectDataHelper, times(2)).validateBusinessObjectDataKey(businessObjectDataKey, true, true);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateBusinessObjectDataRetentionInformation()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data retention information update request.
        BusinessObjectDataRetentionInformationUpdateRequest businessObjectDataRetentionInformationUpdateRequest =
            new BusinessObjectDataRetentionInformationUpdateRequest(RETENTION_EXPIRATION_DATE);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();

        // Create a business object format key without version.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Create a retention type entity for BDATA_RETENTION_DATE.
        RetentionTypeEntity retentionTypeEntity = new RetentionTypeEntity();
        retentionTypeEntity.setCode(RetentionTypeEntity.BDATA_RETENTION_DATE);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setRetentionType(retentionTypeEntity);

        // Create a business object data.
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setId(ID);

        // Mock the external calls.
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(businessObjectDataDao.saveAndRefresh(businessObjectDataEntity)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity)).thenReturn(businessObjectData);

        // Call the method under test.
        BusinessObjectData result = businessObjectDataServiceImpl
            .updateBusinessObjectDataRetentionInformation(businessObjectDataKey, businessObjectDataRetentionInformationUpdateRequest);

        // Verify the external calls.
        verify(businessObjectDataHelper).validateBusinessObjectDataKey(businessObjectDataKey, true, true);
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(businessObjectFormatDaoHelper).getBusinessObjectFormatEntity(businessObjectFormatKey);
        verify(businessObjectDataDao).saveAndRefresh(businessObjectDataEntity);
        verify(businessObjectDataHelper).createBusinessObjectDataFromEntity(businessObjectDataEntity);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectData, result);
        assertEquals(businessObjectDataEntity.getRetentionExpiration(), new Timestamp(RETENTION_EXPIRATION_DATE.toGregorianCalendar().getTimeInMillis()));
    }

    @Test
    public void testUpdateBusinessObjectDataRetentionInformationBusinessObjectFormatHasInvalidRetentionType()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data retention information update request.
        BusinessObjectDataRetentionInformationUpdateRequest businessObjectDataRetentionInformationUpdateRequest =
            new BusinessObjectDataRetentionInformationUpdateRequest(RETENTION_EXPIRATION_DATE);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();

        // Create a business object format key without version.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Create a retention type entity for an invalid retention type.
        RetentionTypeEntity retentionTypeEntity = new RetentionTypeEntity();
        retentionTypeEntity.setCode(INVALID_VALUE);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setRetentionType(retentionTypeEntity);

        // Mock the external calls.
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(businessObjectFormatHelper.businessObjectFormatKeyToString(businessObjectFormatKey)).thenReturn(BUSINESS_OBJECT_FORMAT_KEY_AS_STRING);

        // Try to call the method under test when business object format has an invalid retention type.
        try
        {
            businessObjectDataServiceImpl
                .updateBusinessObjectDataRetentionInformation(businessObjectDataKey, businessObjectDataRetentionInformationUpdateRequest);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Retention information with %s retention type must be configured for business object format. Business object format: {%s}",
                    RetentionTypeEntity.BDATA_RETENTION_DATE, BUSINESS_OBJECT_FORMAT_KEY_AS_STRING), e.getMessage());
        }

        // Verify the external calls.
        verify(businessObjectDataHelper).validateBusinessObjectDataKey(businessObjectDataKey, true, true);
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(businessObjectFormatDaoHelper).getBusinessObjectFormatEntity(businessObjectFormatKey);
        verify(businessObjectFormatHelper).businessObjectFormatKeyToString(businessObjectFormatKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertNull(businessObjectDataEntity.getRetentionExpiration());
    }

    @Test
    public void testUpdateBusinessObjectDataRetentionInformationBusinessObjectFormatHasNoRetentionType()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data retention information update request.
        BusinessObjectDataRetentionInformationUpdateRequest businessObjectDataRetentionInformationUpdateRequest =
            new BusinessObjectDataRetentionInformationUpdateRequest(RETENTION_EXPIRATION_DATE);

        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();

        // Create a business object format key without version.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Create a business object format entity without a retention type.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Mock the external calls.
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(businessObjectFormatHelper.businessObjectFormatKeyToString(businessObjectFormatKey)).thenReturn(BUSINESS_OBJECT_FORMAT_KEY_AS_STRING);

        // Try to call the method under test when business object format has no retention type configured.
        try
        {
            businessObjectDataServiceImpl
                .updateBusinessObjectDataRetentionInformation(businessObjectDataKey, businessObjectDataRetentionInformationUpdateRequest);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Retention information with %s retention type must be configured for business object format. Business object format: {%s}",
                    RetentionTypeEntity.BDATA_RETENTION_DATE, BUSINESS_OBJECT_FORMAT_KEY_AS_STRING), e.getMessage());
        }

        // Verify the external calls.
        verify(businessObjectDataHelper).validateBusinessObjectDataKey(businessObjectDataKey, true, true);
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(businessObjectFormatDaoHelper).getBusinessObjectFormatEntity(businessObjectFormatKey);
        verify(businessObjectFormatHelper).businessObjectFormatKeyToString(businessObjectFormatKey);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertNull(businessObjectDataEntity.getRetentionExpiration());
    }

    @Test
    public void testUpdateBusinessObjectDataRetentionInformationMissingOptionalParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Create a business object data retention information update request with retention expiration date set to null.
        BusinessObjectDataRetentionInformationUpdateRequest businessObjectDataRetentionInformationUpdateRequest =
            new BusinessObjectDataRetentionInformationUpdateRequest(NO_RETENTION_EXPIRATION_DATE);

        // Create a business object data entity with a retention expiration date.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setRetentionExpiration(new Timestamp(RETENTION_EXPIRATION_DATE.toGregorianCalendar().getTimeInMillis()));

        // Create a business object format key without version.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Create a retention type entity for BDATA_RETENTION_DATE.
        RetentionTypeEntity retentionTypeEntity = new RetentionTypeEntity();
        retentionTypeEntity.setCode(RetentionTypeEntity.BDATA_RETENTION_DATE);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setRetentionType(retentionTypeEntity);

        // Create a business object data.
        BusinessObjectData businessObjectData = new BusinessObjectData();
        businessObjectData.setId(ID);

        // Mock the external calls.
        when(businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey)).thenReturn(businessObjectDataEntity);
        when(businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(businessObjectDataDao.saveAndRefresh(businessObjectDataEntity)).thenReturn(businessObjectDataEntity);
        when(businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity)).thenReturn(businessObjectData);

        // Call the method under test.
        BusinessObjectData result = businessObjectDataServiceImpl
            .updateBusinessObjectDataRetentionInformation(businessObjectDataKey, businessObjectDataRetentionInformationUpdateRequest);

        // Verify the external calls.
        verify(businessObjectDataHelper).validateBusinessObjectDataKey(businessObjectDataKey, true, true);
        verify(businessObjectDataDaoHelper).getBusinessObjectDataEntity(businessObjectDataKey);
        verify(businessObjectFormatDaoHelper).getBusinessObjectFormatEntity(businessObjectFormatKey);
        verify(businessObjectDataDao).saveAndRefresh(businessObjectDataEntity);
        verify(businessObjectDataHelper).createBusinessObjectDataFromEntity(businessObjectDataEntity);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectData, result);
        assertNull(businessObjectDataEntity.getRetentionExpiration());
    }

    @Test
    public void testUpdateBusinessObjectDataRetentionInformationMissingRequiredParameters()
    {
        // Create a business object data key.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);

        // Try to call the method under test without specifying a business object data retention information update request.
        try
        {
            businessObjectDataServiceImpl.updateBusinessObjectDataRetentionInformation(businessObjectDataKey, null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data retention information update request must be specified.", e.getMessage());
        }
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(attributeDaoHelper, attributeHelper, businessObjectDataDao, businessObjectDataDaoHelper, businessObjectDataHelper,
            businessObjectDataInitiateDestroyHelperService, businessObjectDataInitiateRestoreHelperService, businessObjectDataInvalidateUnregisteredHelper,
            businessObjectDataRetryStoragePolicyTransitionHelper, businessObjectDataSearchHelper, businessObjectDataStatusDaoHelper,
            businessObjectDefinitionDaoHelper, businessObjectDefinitionHelper, businessObjectFormatDaoHelper, businessObjectFormatHelper, configurationHelper,
            customDdlDaoHelper, ddlGeneratorFactory, jsonHelper, notificationEventService, s3KeyPrefixHelper, s3Service, storageDaoHelper, storageHelper,
            storageUnitDao, storageUnitHelper);
    }
}
