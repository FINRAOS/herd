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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.BusinessObjectFormatDao;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatRetentionInformationUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatSchemaBackwardsCompatibilityUpdateRequest;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.RetentionTypeEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.MessageNotificationEventService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.AttributeHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.BusinessObjectFormatDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatHelper;
import org.finra.herd.service.helper.CustomDdlDaoHelper;
import org.finra.herd.service.helper.DdlGeneratorFactory;
import org.finra.herd.service.helper.FileTypeDaoHelper;
import org.finra.herd.service.helper.PartitionKeyGroupDaoHelper;
import org.finra.herd.service.helper.SearchIndexUpdateHelper;

public class BusinessObjectFormatServiceImplTest extends AbstractServiceTest
{
    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private AttributeHelper attributeHelper;

    @Mock
    private BusinessObjectDataDao businessObjectDataDao;

    @Mock
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Mock
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Mock
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Mock
    private BusinessObjectFormatDao businessObjectFormatDao;

    @Mock
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Mock
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @InjectMocks
    private BusinessObjectFormatServiceImpl businessObjectFormatServiceImpl;

    @Mock
    private CustomDdlDaoHelper customDdlDaoHelper;

    @Mock
    private DdlGeneratorFactory ddlGeneratorFactory;

    @Mock
    private FileTypeDaoHelper fileTypeDaoHelper;

    @Mock
    private MessageNotificationEventService messageNotificationEventService;

    @Mock
    private PartitionKeyGroupDaoHelper partitionKeyGroupDaoHelper;

    @Mock
    private SearchIndexUpdateHelper searchIndexUpdateHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testUpdateBusinessObjectFormatRetentionInformationMissingRequiredParameters()
    {
        // Create a business object format key without version.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Try to call the method under test without specifying a business object format retention information update request.
        try
        {
            businessObjectFormatServiceImpl.updateBusinessObjectFormatRetentionInformation(businessObjectFormatKey, null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format retention information update request must be specified.", e.getMessage());
        }

        // Try to call the method under test without specifying a record flag.
        try
        {
            businessObjectFormatServiceImpl.updateBusinessObjectFormatRetentionInformation(businessObjectFormatKey,
                new BusinessObjectFormatRetentionInformationUpdateRequest(null, RETENTION_PERIOD_DAYS, RetentionTypeEntity.PARTITION_VALUE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A record flag in business object format retention information update request must be specified.", e.getMessage());
        }

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateBusinessObjectFormatRetentionInformationNoRetentionType()
    {
        // Create a business object format key without version.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Create a business object format retention information update request without retention type and retention period in days.
        BusinessObjectFormatRetentionInformationUpdateRequest businessObjectFormatRetentionInformationUpdateRequest =
            new BusinessObjectFormatRetentionInformationUpdateRequest(RECORD_FLAG_SET, NO_RETENTION_PERIOD_IN_DAYS, NO_RETENTION_TYPE);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create a business object format.
        BusinessObjectFormat businessObjectFormat = new BusinessObjectFormat();
        businessObjectFormat.setId(ID);

        // Mock the external calls.
        when(businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity)).thenReturn(businessObjectFormatEntity);
        when(businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity)).thenReturn(businessObjectFormat);

        // Call the method under test.
        BusinessObjectFormat result = businessObjectFormatServiceImpl
            .updateBusinessObjectFormatRetentionInformation(businessObjectFormatKey, businessObjectFormatRetentionInformationUpdateRequest);

        // Verify the external calls.
        verify(businessObjectFormatHelper).validateBusinessObjectFormatKey(businessObjectFormatKey, false);
        verify(businessObjectFormatDaoHelper).getBusinessObjectFormatEntity(businessObjectFormatKey);
        verify(businessObjectFormatDao).saveAndRefresh(businessObjectFormatEntity);
        verify(businessObjectFormatHelper).createBusinessObjectFormatFromEntity(businessObjectFormatEntity);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectFormat, result);
        assertEquals(businessObjectFormatEntity.isRecordFlag(), RECORD_FLAG_SET);
        assertEquals(businessObjectFormatEntity.getRetentionPeriodInDays(), NO_RETENTION_PERIOD_IN_DAYS);
        assertNull(businessObjectFormatEntity.getRetentionType());
    }

    @Test
    public void testUpdateBusinessObjectFormatRetentionInformationNoRetentionTypeWithRetentionPeriodInDays()
    {
        // Create a business object format key without version.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Try to call the method under test with retention period in days specified without a retention type.
        try
        {
            businessObjectFormatServiceImpl.updateBusinessObjectFormatRetentionInformation(businessObjectFormatKey,
                new BusinessObjectFormatRetentionInformationUpdateRequest(RECORD_FLAG_SET, RETENTION_PERIOD_DAYS, NO_RETENTION_TYPE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A retention period in days cannot be specified without retention type.", e.getMessage());
        }

        // Verify the external calls.
        verify(businessObjectFormatHelper).validateBusinessObjectFormatKey(businessObjectFormatKey, false);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateBusinessObjectFormatRetentionInformationPartitionValueRetentionType()
    {
        // Create a business object format key without version.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Create a business object format retention information update request.
        BusinessObjectFormatRetentionInformationUpdateRequest businessObjectFormatRetentionInformationUpdateRequest =
            new BusinessObjectFormatRetentionInformationUpdateRequest(RECORD_FLAG_SET, RETENTION_PERIOD_DAYS, RetentionTypeEntity.PARTITION_VALUE);

        // Create a retention type entity for PARTITION_VALUE.
        RetentionTypeEntity retentionTypeEntity = new RetentionTypeEntity();
        retentionTypeEntity.setCode(RetentionTypeEntity.PARTITION_VALUE);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create a business object format.
        BusinessObjectFormat businessObjectFormat = new BusinessObjectFormat();
        businessObjectFormat.setId(ID);

        // Mock the external calls.
        when(businessObjectFormatDaoHelper.getRecordRetentionTypeEntity(RetentionTypeEntity.PARTITION_VALUE)).thenReturn(retentionTypeEntity);
        when(businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity)).thenReturn(businessObjectFormatEntity);
        when(businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity)).thenReturn(businessObjectFormat);

        // Call the method under test.
        BusinessObjectFormat result = businessObjectFormatServiceImpl
            .updateBusinessObjectFormatRetentionInformation(businessObjectFormatKey, businessObjectFormatRetentionInformationUpdateRequest);

        // Verify the external calls.
        verify(businessObjectFormatHelper).validateBusinessObjectFormatKey(businessObjectFormatKey, false);
        verify(businessObjectFormatDaoHelper).getRecordRetentionTypeEntity(RetentionTypeEntity.PARTITION_VALUE);
        verify(businessObjectFormatDaoHelper).getBusinessObjectFormatEntity(businessObjectFormatKey);
        verify(businessObjectFormatDao).saveAndRefresh(businessObjectFormatEntity);
        verify(businessObjectFormatHelper).createBusinessObjectFormatFromEntity(businessObjectFormatEntity);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectFormat, result);
        assertEquals(businessObjectFormatEntity.isRecordFlag(), RECORD_FLAG_SET);
        assertEquals(businessObjectFormatEntity.getRetentionPeriodInDays(), RETENTION_PERIOD_DAYS);
        assertEquals(businessObjectFormatEntity.getRetentionType(), retentionTypeEntity);
    }

    @Test
    public void testUpdateBusinessObjectFormatRetentionInformationPartitionValueRetentionTypeMissingRetentionPeriodInDays()
    {
        // Create a business object format key without version.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Create a retention type entity for PARTITION_VALUE.
        RetentionTypeEntity retentionTypeEntity = new RetentionTypeEntity();
        retentionTypeEntity.setCode(RetentionTypeEntity.PARTITION_VALUE);

        // Mock the external calls.
        when(businessObjectFormatDaoHelper.getRecordRetentionTypeEntity(RetentionTypeEntity.PARTITION_VALUE)).thenReturn(retentionTypeEntity);

        // Try to call the method under test with PARTITION_VALUE retention type and without specifying a retention period in days.
        try
        {
            businessObjectFormatServiceImpl.updateBusinessObjectFormatRetentionInformation(businessObjectFormatKey,
                new BusinessObjectFormatRetentionInformationUpdateRequest(RECORD_FLAG_SET, NO_RETENTION_PERIOD_IN_DAYS, RetentionTypeEntity.PARTITION_VALUE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("A retention period in days must be specified for %s retention type.", RetentionTypeEntity.PARTITION_VALUE),
                e.getMessage());
        }

        // Verify the external calls.
        verify(businessObjectFormatHelper).validateBusinessObjectFormatKey(businessObjectFormatKey, false);
        verify(businessObjectFormatDaoHelper).getRecordRetentionTypeEntity(RetentionTypeEntity.PARTITION_VALUE);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateBusinessObjectFormatRetentionInformationPartitionValueRetentionTypeNonPositiveRetentionPeriodInDays()
    {
        // Create a business object format key without version.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Create a retention type entity for PARTITION_VALUE.
        RetentionTypeEntity retentionTypeEntity = new RetentionTypeEntity();
        retentionTypeEntity.setCode(RetentionTypeEntity.PARTITION_VALUE);

        // Mock the external calls.
        when(businessObjectFormatDaoHelper.getRecordRetentionTypeEntity(RetentionTypeEntity.PARTITION_VALUE)).thenReturn(retentionTypeEntity);

        // Try to call the method under test with PARTITION_VALUE retention type and with a 0 retention period in days.
        try
        {
            businessObjectFormatServiceImpl.updateBusinessObjectFormatRetentionInformation(businessObjectFormatKey,
                new BusinessObjectFormatRetentionInformationUpdateRequest(RECORD_FLAG_SET, 0, RetentionTypeEntity.PARTITION_VALUE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("A positive retention period in days must be specified for %s retention type.", RetentionTypeEntity.PARTITION_VALUE),
                e.getMessage());
        }

        // Verify the external calls.
        verify(businessObjectFormatHelper).validateBusinessObjectFormatKey(businessObjectFormatKey, false);
        verify(businessObjectFormatDaoHelper).getRecordRetentionTypeEntity(RetentionTypeEntity.PARTITION_VALUE);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateBusinessObjectFormatRetentionInformationRetentionDateRetentionType()
    {
        // Create a business object format key without version.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Create a business object format retention information update request.
        BusinessObjectFormatRetentionInformationUpdateRequest businessObjectFormatRetentionInformationUpdateRequest =
            new BusinessObjectFormatRetentionInformationUpdateRequest(RECORD_FLAG_SET, NO_RETENTION_PERIOD_IN_DAYS, RetentionTypeEntity.BDATA_RETENTION_DATE);

        // Create a retention type entity for BDATA_RETENTION_DATE.
        RetentionTypeEntity retentionTypeEntity = new RetentionTypeEntity();
        retentionTypeEntity.setCode(RetentionTypeEntity.BDATA_RETENTION_DATE);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create a business object format.
        BusinessObjectFormat businessObjectFormat = new BusinessObjectFormat();
        businessObjectFormat.setId(ID);

        // Mock the external calls.
        when(businessObjectFormatDaoHelper.getRecordRetentionTypeEntity(RetentionTypeEntity.BDATA_RETENTION_DATE)).thenReturn(retentionTypeEntity);
        when(businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity)).thenReturn(businessObjectFormatEntity);
        when(businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity)).thenReturn(businessObjectFormat);

        // Call the method under test.
        BusinessObjectFormat result = businessObjectFormatServiceImpl
            .updateBusinessObjectFormatRetentionInformation(businessObjectFormatKey, businessObjectFormatRetentionInformationUpdateRequest);

        // Verify the external calls.
        verify(businessObjectFormatHelper).validateBusinessObjectFormatKey(businessObjectFormatKey, false);
        verify(businessObjectFormatDaoHelper).getRecordRetentionTypeEntity(RetentionTypeEntity.BDATA_RETENTION_DATE);
        verify(businessObjectFormatDaoHelper).getBusinessObjectFormatEntity(businessObjectFormatKey);
        verify(businessObjectFormatDao).saveAndRefresh(businessObjectFormatEntity);
        verify(businessObjectFormatHelper).createBusinessObjectFormatFromEntity(businessObjectFormatEntity);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectFormat, result);
        assertEquals(businessObjectFormatEntity.isRecordFlag(), RECORD_FLAG_SET);
        assertEquals(businessObjectFormatEntity.getRetentionPeriodInDays(), NO_RETENTION_PERIOD_IN_DAYS);
        assertEquals(businessObjectFormatEntity.getRetentionType(), retentionTypeEntity);
    }

    @Test
    public void testUpdateBusinessObjectFormatRetentionInformationRetentionDateRetentionTypeWithRetentionPeriodInDays()
    {
        // Create a business object format key without version.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Create a retention type entity for BDATA_RETENTION_DATE.
        RetentionTypeEntity retentionTypeEntity = new RetentionTypeEntity();
        retentionTypeEntity.setCode(RetentionTypeEntity.BDATA_RETENTION_DATE);

        // Mock the external calls.
        when(businessObjectFormatDaoHelper.getRecordRetentionTypeEntity(RetentionTypeEntity.BDATA_RETENTION_DATE)).thenReturn(retentionTypeEntity);

        // Try to call the method under test with retention period in days specified for BDATA_RETENTION_DATE retention type.
        try
        {
            businessObjectFormatServiceImpl.updateBusinessObjectFormatRetentionInformation(businessObjectFormatKey,
                new BusinessObjectFormatRetentionInformationUpdateRequest(RECORD_FLAG_SET, RETENTION_PERIOD_DAYS, RetentionTypeEntity.BDATA_RETENTION_DATE));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("A retention period in days cannot be specified for %s retention type.", RetentionTypeEntity.BDATA_RETENTION_DATE),
                e.getMessage());
        }

        // Verify the external calls.
        verify(businessObjectFormatHelper).validateBusinessObjectFormatKey(businessObjectFormatKey, false);
        verify(businessObjectFormatDaoHelper).getRecordRetentionTypeEntity(RetentionTypeEntity.BDATA_RETENTION_DATE);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateBusinessObjectFormatSchemaBackwardsCompatibilityChanges()
    {
        // Create a business object format key without version.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Create a business object format schema backwards compatibility changes update request.
        BusinessObjectFormatSchemaBackwardsCompatibilityUpdateRequest businessObjectFormatSchemaBackwardsCompatibilityUpdateRequest =
            new BusinessObjectFormatSchemaBackwardsCompatibilityUpdateRequest(ALLOW_NON_BACKWARDS_COMPATIBLE_CHANGES_SET);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create a business object format.
        BusinessObjectFormat businessObjectFormat = new BusinessObjectFormat();
        businessObjectFormat.setId(ID);

        // Mock the external calls.
        when(businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity)).thenReturn(businessObjectFormatEntity);
        when(businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity)).thenReturn(businessObjectFormat);

        // Call the method under test.
        BusinessObjectFormat result = businessObjectFormatServiceImpl.updateBusinessObjectFormatSchemaBackwardsCompatibilityChanges(businessObjectFormatKey,
            businessObjectFormatSchemaBackwardsCompatibilityUpdateRequest);

        // Verify the external calls.
        verify(businessObjectFormatHelper).validateBusinessObjectFormatKey(businessObjectFormatKey, false);
        verify(businessObjectFormatDaoHelper).getBusinessObjectFormatEntity(businessObjectFormatKey);
        verify(businessObjectFormatDao).saveAndRefresh(businessObjectFormatEntity);
        verify(businessObjectFormatHelper).createBusinessObjectFormatFromEntity(businessObjectFormatEntity);
        verifyNoMoreInteractionsHelper();

        // Validate the results.
        assertEquals(businessObjectFormat, result);
        assertEquals(businessObjectFormatEntity.isAllowNonBackwardsCompatibleChanges(), ALLOW_NON_BACKWARDS_COMPATIBLE_CHANGES_SET);
    }

    @Test
    public void testUpdateBusinessObjectFormatSchemaBackwardsCompatibilityChangesMissingRequiredParameters()
    {
        // Create a business object format key without version.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION);

        // Try to call the method under test without specifying a business object format schema backwards compatibility changes update request.
        try
        {
            businessObjectFormatServiceImpl.updateBusinessObjectFormatSchemaBackwardsCompatibilityChanges(businessObjectFormatKey, null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format schema backwards compatibility changes update request must be specified.", e.getMessage());
        }

        // Try to call the method under test without specifying a record flag.
        try
        {
            businessObjectFormatServiceImpl.updateBusinessObjectFormatSchemaBackwardsCompatibilityChanges(businessObjectFormatKey,
                new BusinessObjectFormatSchemaBackwardsCompatibilityUpdateRequest(null));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(
                "allowNonBackwardsCompatibleChanges flag in business object format schema backwards compatibility changes update request must be specified.",
                e.getMessage());
        }

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(alternateKeyHelper, attributeHelper, businessObjectDataDao, businessObjectDefinitionDao, businessObjectDefinitionDaoHelper,
            businessObjectDefinitionHelper, businessObjectFormatDao, businessObjectFormatDaoHelper, businessObjectFormatHelper, customDdlDaoHelper,
            ddlGeneratorFactory, fileTypeDaoHelper, messageNotificationEventService, partitionKeyGroupDaoHelper, searchIndexUpdateHelper);
    }
}
