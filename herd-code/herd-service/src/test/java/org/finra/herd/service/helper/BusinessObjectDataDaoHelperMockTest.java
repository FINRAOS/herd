package org.finra.herd.service.helper;

import static org.finra.herd.core.AbstractCoreTest.BLANK_TEXT;
import static org.finra.herd.dao.AbstractDaoTest.BDATA_STATUS;
import static org.finra.herd.dao.AbstractDaoTest.ERROR_MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
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
