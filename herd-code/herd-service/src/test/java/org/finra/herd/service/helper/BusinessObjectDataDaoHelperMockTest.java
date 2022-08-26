package org.finra.herd.service.helper;

import static org.finra.herd.core.AbstractCoreTest.BLANK_TEXT;
import static org.finra.herd.dao.AbstractDaoTest.ATTRIBUTE_NAME;
import static org.finra.herd.dao.AbstractDaoTest.ATTRIBUTE_NAME_2;
import static org.finra.herd.dao.AbstractDaoTest.ATTRIBUTE_NAME_3;
import static org.finra.herd.dao.AbstractDaoTest.ATTRIBUTE_VALUE;
import static org.finra.herd.dao.AbstractDaoTest.ATTRIBUTE_VALUE_2;
import static org.finra.herd.dao.AbstractDaoTest.ATTRIBUTE_VALUE_3;
import static org.finra.herd.dao.AbstractDaoTest.BDATA_STATUS;
import static org.finra.herd.dao.AbstractDaoTest.ERROR_MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.ExpectedPartitionValueDao;
import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.dto.AttributeDto;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.service.MessageNotificationEventService;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.S3Service;

public class BusinessObjectDataDaoHelperMockTest
{
    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private AttributeDaoHelper attributeDaoHelper;

    @Mock
    private AttributeHelper attributeHelper;

    @Mock
    private BusinessObjectDataDao businessObjectDataDao;

    @InjectMocks
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelperImpl;

    @Mock
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Mock
    private BusinessObjectDataStatusDaoHelper businessObjectDataStatusDaoHelper;

    @Mock
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Mock
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private ExpectedPartitionValueDao expectedPartitionValueDao;

    @Mock
    private JsonHelper jsonHelper;

    @Mock
    private MessageNotificationEventService messageNotificationEventService;

    @Mock
    private NotificationEventService notificationEventService;

    @Mock
    private S3KeyPrefixHelper s3KeyPrefixHelper;

    @Mock
    private S3Service s3Service;

    @Mock
    private StorageDaoHelper storageDaoHelper;

    @Mock
    private StorageFileHelper storageFileHelper;

    @Mock
    private StorageHelper storageHelper;

    @Mock
    private StorageUnitDao storageUnitDao;

    @Mock
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @Mock
    private StorageUnitStatusDaoHelper storageUnitStatusDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetBusinessObjectStatusEntityForAvailableDataWithBlankStatusValue()
    {
        // Create business object data status entity for VALID status.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BusinessObjectDataStatusEntity.VALID);
        businessObjectDataStatusEntity.setPreRegistrationStatus(false);

        // Mock the external calls.
        when(businessObjectDataStatusDaoHelper.getBusinessObjectDataStatusEntity(BusinessObjectDataStatusEntity.VALID))
            .thenReturn(businessObjectDataStatusEntity);

        // Call the method under test.
        BusinessObjectDataStatusEntity result = businessObjectDataDaoHelperImpl.getBusinessObjectStatusEntityForAvailableData(BLANK_TEXT);

        // Validate the results.
        assertEquals(businessObjectDataStatusEntity, result);

        // Verify the external calls.
        verify(businessObjectDataStatusDaoHelper).getBusinessObjectDataStatusEntity(BusinessObjectDataStatusEntity.VALID);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetBusinessObjectStatusEntityForAvailableDataWithNonExistingStatusValue()
    {
        // Mock the external calls.
        when(businessObjectDataStatusDaoHelper.getBusinessObjectDataStatusEntity(BDATA_STATUS)).thenThrow(new ObjectNotFoundException(ERROR_MESSAGE));

        // Try to call the method under test.
        try
        {
            businessObjectDataDaoHelperImpl.getBusinessObjectStatusEntityForAvailableData(BDATA_STATUS);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(ERROR_MESSAGE, e.getMessage());
        }

        // Verify the external calls.
        verify(businessObjectDataStatusDaoHelper).getBusinessObjectDataStatusEntity(BDATA_STATUS);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetBusinessObjectStatusEntityForAvailableDataWithNonPreRegisteredStatusValue()
    {
        // Create business object data status entity that is neither VALID nor pre-registered status.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BDATA_STATUS);
        businessObjectDataStatusEntity.setPreRegistrationStatus(false);

        // Mock the external calls.
        when(businessObjectDataStatusDaoHelper.getBusinessObjectDataStatusEntity(BDATA_STATUS)).thenReturn(businessObjectDataStatusEntity);

        // Try to call the method under test.
        try
        {
            businessObjectDataDaoHelperImpl.getBusinessObjectStatusEntityForAvailableData(BDATA_STATUS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Business object data status specified in the request must be \"%s\" or one of the pre-registration statuses.",
                BusinessObjectDataStatusEntity.VALID), e.getMessage());
        }

        // Verify the external calls.
        verify(businessObjectDataStatusDaoHelper).getBusinessObjectDataStatusEntity(BDATA_STATUS);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetBusinessObjectStatusEntityForAvailableDataWithPreRegisteredStatusValue()
    {
        // Create business object data status entity for a pre-registered status.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BDATA_STATUS);
        businessObjectDataStatusEntity.setPreRegistrationStatus(true);

        // Mock the external calls.
        when(businessObjectDataStatusDaoHelper.getBusinessObjectDataStatusEntity(BDATA_STATUS)).thenReturn(businessObjectDataStatusEntity);

        // Call the method under test.
        BusinessObjectDataStatusEntity result = businessObjectDataDaoHelperImpl.getBusinessObjectStatusEntityForAvailableData(BDATA_STATUS);

        // Validate the results.
        assertEquals(businessObjectDataStatusEntity, result);

        // Verify the external calls.
        verify(businessObjectDataStatusDaoHelper).getBusinessObjectDataStatusEntity(BDATA_STATUS);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetBusinessObjectStatusEntityForAvailableDataWithValidStatusValue()
    {
        // Create business object data status entity for VALID status.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = new BusinessObjectDataStatusEntity();
        businessObjectDataStatusEntity.setCode(BusinessObjectDataStatusEntity.VALID);
        businessObjectDataStatusEntity.setPreRegistrationStatus(false);

        // Mock the external calls.
        when(businessObjectDataStatusDaoHelper.getBusinessObjectDataStatusEntity(BDATA_STATUS)).thenReturn(businessObjectDataStatusEntity);

        // Call the method under test.
        BusinessObjectDataStatusEntity result = businessObjectDataDaoHelperImpl.getBusinessObjectStatusEntityForAvailableData(BDATA_STATUS);

        // Validate the results.
        assertEquals(businessObjectDataStatusEntity, result);

        // Verify the external calls.
        verify(businessObjectDataStatusDaoHelper).getBusinessObjectDataStatusEntity(BDATA_STATUS);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetPublishedBusinessObjectDataAttributes()
    {
        // Create business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);

        // Create business object data attribute entity which is publishable.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity1 = new BusinessObjectDataAttributeEntity();
        businessObjectDataAttributeEntity1.setName(ATTRIBUTE_NAME);
        businessObjectDataAttributeEntity1.setValue(ATTRIBUTE_VALUE);

        // Create business object data attribute entity which has an attribute definition but is not publishable.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity2 = new BusinessObjectDataAttributeEntity();
        businessObjectDataAttributeEntity2.setName(ATTRIBUTE_NAME_2);
        businessObjectDataAttributeEntity2.setValue(ATTRIBUTE_VALUE_2);

        // Create business object data attribute entity which has no attribute definition.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity3 = new BusinessObjectDataAttributeEntity();
        businessObjectDataAttributeEntity3.setName(ATTRIBUTE_NAME_3);
        businessObjectDataAttributeEntity3.setValue(ATTRIBUTE_VALUE_3);

        // Add all business object data attribute entities to the business object data entity.
        businessObjectDataEntity
            .setAttributes(Arrays.asList(businessObjectDataAttributeEntity1, businessObjectDataAttributeEntity2, businessObjectDataAttributeEntity3));

        // Create a map of attribute definitions with the attribute having publish flag set to true.
        Map<String, BusinessObjectDataAttributeDefinitionEntity> attributeDefinitionEntityMap = new HashMap<>();
        BusinessObjectDataAttributeDefinitionEntity businessObjectDataAttributeDefinitionEntity = new BusinessObjectDataAttributeDefinitionEntity();
        businessObjectDataAttributeDefinitionEntity.setName(ATTRIBUTE_NAME);
        businessObjectDataAttributeDefinitionEntity.setPublish(true);
        attributeDefinitionEntityMap.put(ATTRIBUTE_NAME.toUpperCase(), businessObjectDataAttributeDefinitionEntity);

        // Mock the external calls.
        when(businessObjectFormatHelper.getAttributeDefinitionEntities(businessObjectFormatEntity)).thenReturn(attributeDefinitionEntityMap);

        // Call the method under test.
        List<AttributeDto> result = businessObjectDataDaoHelperImpl.getPublishedBusinessObjectDataAttributes(businessObjectDataEntity);

        // Validate the results.
        List<AttributeDto> expectedPublishedBusinessObjectDataAttributes = new ArrayList<>();
        expectedPublishedBusinessObjectDataAttributes.add(new AttributeDto(ATTRIBUTE_NAME, ATTRIBUTE_VALUE));
        assertEquals(expectedPublishedBusinessObjectDataAttributes, result);

        // Verify the external calls.
        verify(businessObjectFormatHelper).getAttributeDefinitionEntities(businessObjectFormatEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetPublishedBusinessObjectDataAttributesNoAttributeDefinitions()
    {
        // Create business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);

        // Create business object data attribute entity which has no attribute definition.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity = new BusinessObjectDataAttributeEntity();
        businessObjectDataAttributeEntity.setName(ATTRIBUTE_NAME);
        businessObjectDataAttributeEntity.setValue(ATTRIBUTE_VALUE);

        // Add business object data attribute entity to the business object data entity.
        businessObjectDataEntity.setAttributes(Collections.singletonList(businessObjectDataAttributeEntity));

        // Create an empty map of attribute definitions.
        Map<String, BusinessObjectDataAttributeDefinitionEntity> attributeDefinitionEntityMap = new HashMap<>();

        // Mock the external calls.
        when(businessObjectFormatHelper.getAttributeDefinitionEntities(businessObjectFormatEntity)).thenReturn(attributeDefinitionEntityMap);

        // Call the method under test.
        List<AttributeDto> result = businessObjectDataDaoHelperImpl.getPublishedBusinessObjectDataAttributes(businessObjectDataEntity);

        // Validate the results.
        assertTrue(result.isEmpty());

        // Verify the external calls.
        verify(businessObjectFormatHelper).getAttributeDefinitionEntities(businessObjectFormatEntity);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(alternateKeyHelper, attributeDaoHelper, attributeHelper, businessObjectDataDao, businessObjectDataHelper,
            businessObjectDataStatusDaoHelper, businessObjectFormatDaoHelper, businessObjectFormatHelper, configurationHelper, expectedPartitionValueDao,
            jsonHelper, messageNotificationEventService, notificationEventService, s3KeyPrefixHelper, s3Service, storageDaoHelper, storageFileHelper,
            storageHelper, storageUnitDao, storageUnitDaoHelper, storageUnitStatusDaoHelper);
    }
}
