package org.finra.herd.service.impl;

import static org.finra.herd.dao.AbstractDaoTest.BDEF_NAME;
import static org.finra.herd.dao.AbstractDaoTest.EXTERNAL_INTERFACE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_FILE_TYPE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_USAGE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.NAMESPACE;
import static org.finra.herd.service.AbstractServiceTest.ID;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.BusinessObjectFormatExternalInterfaceDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterface;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatExternalInterfaceEntity;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;
import org.finra.herd.service.helper.BusinessObjectFormatDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatExternalInterfaceDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatExternalInterfaceHelper;
import org.finra.herd.service.helper.ExternalInterfaceDaoHelper;

public class BusinessObjectFormatExternalInterfaceServiceImplTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Mock
    private BusinessObjectFormatExternalInterfaceDao businessObjectFormatExternalInterfaceDao;

    @Mock
    private BusinessObjectFormatExternalInterfaceDaoHelper businessObjectFormatExternalInterfaceDaoHelper;

    @Mock
    private BusinessObjectFormatExternalInterfaceHelper businessObjectFormatExternalInterfaceHelper;

    @InjectMocks
    private BusinessObjectFormatExternalInterfaceServiceImpl businessObjectFormatExternalInterfaceService;

    @Mock
    private ExternalInterfaceDaoHelper externalInterfaceDaoHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateBusinessObjectFormatExternalInterface()
    {
        // Create a business object format to external interface mapping key.
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey =
            new BusinessObjectFormatExternalInterfaceKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE);

        // Create a business object format to external interface mapping create request.
        BusinessObjectFormatExternalInterfaceCreateRequest businessObjectFormatExternalInterfaceCreateRequest =
            new BusinessObjectFormatExternalInterfaceCreateRequest(businessObjectFormatExternalInterfaceKey);

        // Create a business object format to external interface mapping.
        BusinessObjectFormatExternalInterface businessObjectFormatExternalInterface =
            new BusinessObjectFormatExternalInterface(ID, businessObjectFormatExternalInterfaceKey);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create a external interface entity.
        ExternalInterfaceEntity externalInterfaceEntity = new ExternalInterfaceEntity();

        // Mock the external calls.
        when(businessObjectFormatDaoHelper
            .getBusinessObjectFormatEntity(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null)))
            .thenReturn(businessObjectFormatEntity);
        when(externalInterfaceDaoHelper.getExternalInterfaceEntity(EXTERNAL_INTERFACE)).thenReturn(externalInterfaceEntity);
        when(businessObjectFormatExternalInterfaceDao
            .getBusinessObjectFormatExternalInterfaceByBusinessObjectFormatAndExternalInterface(businessObjectFormatEntity, externalInterfaceEntity))
            .thenReturn(null);
        when(businessObjectFormatExternalInterfaceHelper
            .createBusinessObjectFormatExternalInterfaceFromEntity(any(BusinessObjectFormatExternalInterfaceEntity.class)))
            .thenReturn(businessObjectFormatExternalInterface);

        // Call the method under test.
        BusinessObjectFormatExternalInterface result =
            businessObjectFormatExternalInterfaceService.createBusinessObjectFormatExternalInterface(businessObjectFormatExternalInterfaceCreateRequest);

        // Validate the results.
        assertEquals(businessObjectFormatExternalInterface, result);

        // Verify the external calls.
        verify(businessObjectFormatExternalInterfaceHelper)
            .validateAndTrimBusinessObjectFormatExternalInterfaceCreateRequest(businessObjectFormatExternalInterfaceCreateRequest);
        verify(businessObjectFormatDaoHelper)
            .getBusinessObjectFormatEntity(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));
        verify(externalInterfaceDaoHelper).getExternalInterfaceEntity(EXTERNAL_INTERFACE);
        verify(businessObjectFormatExternalInterfaceDao)
            .getBusinessObjectFormatExternalInterfaceByBusinessObjectFormatAndExternalInterface(businessObjectFormatEntity, externalInterfaceEntity);
        verify(businessObjectFormatExternalInterfaceDao).saveAndRefresh(any(BusinessObjectFormatExternalInterfaceEntity.class));
        verify(businessObjectFormatExternalInterfaceHelper)
            .createBusinessObjectFormatExternalInterfaceFromEntity(any(BusinessObjectFormatExternalInterfaceEntity.class));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateBusinessObjectFormatExternalInterfaceAlreadyExists()
    {
        // Create a business object format to external interface mapping key.
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey =
            new BusinessObjectFormatExternalInterfaceKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE);

        // Create a business object format to external interface mapping create request.
        BusinessObjectFormatExternalInterfaceCreateRequest businessObjectFormatExternalInterfaceCreateRequest =
            new BusinessObjectFormatExternalInterfaceCreateRequest(businessObjectFormatExternalInterfaceKey);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create a external interface entity.
        ExternalInterfaceEntity externalInterfaceEntity = new ExternalInterfaceEntity();

        // Mock the external calls.
        when(businessObjectFormatDaoHelper
            .getBusinessObjectFormatEntity(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null)))
            .thenReturn(businessObjectFormatEntity);
        when(externalInterfaceDaoHelper.getExternalInterfaceEntity(EXTERNAL_INTERFACE)).thenReturn(externalInterfaceEntity);
        when(businessObjectFormatExternalInterfaceDao
            .getBusinessObjectFormatExternalInterfaceByBusinessObjectFormatAndExternalInterface(businessObjectFormatEntity, externalInterfaceEntity))
            .thenReturn(new BusinessObjectFormatExternalInterfaceEntity());

        // Specify the expected exception.
        expectedException.expect(AlreadyExistsException.class);
        expectedException.expectMessage(String.format("Unable to create business object format to external interface mapping for \"%s\" namespace, " +
            "\"%s\" business object definition name, \"%s\" business object format usage, \"%s\" business object format file type, and " +
            "\"%s\" external interface name because it already exists.", NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE));

        // Call the method under test.
        businessObjectFormatExternalInterfaceService.createBusinessObjectFormatExternalInterface(businessObjectFormatExternalInterfaceCreateRequest);

        // Verify the external calls.
        verify(businessObjectFormatExternalInterfaceHelper)
            .validateAndTrimBusinessObjectFormatExternalInterfaceCreateRequest(businessObjectFormatExternalInterfaceCreateRequest);
        verify(businessObjectFormatDaoHelper)
            .getBusinessObjectFormatEntity(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null));
        verify(externalInterfaceDaoHelper).getExternalInterfaceEntity(EXTERNAL_INTERFACE);
        verify(businessObjectFormatExternalInterfaceDao)
            .getBusinessObjectFormatExternalInterfaceByBusinessObjectFormatAndExternalInterface(businessObjectFormatEntity, externalInterfaceEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testDeleteBusinessObjectFormatExternalInterface()
    {
        // Create a business object format to external interface mapping key.
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey =
            new BusinessObjectFormatExternalInterfaceKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE);

        // Create a business object format to external interface mapping.
        BusinessObjectFormatExternalInterface businessObjectFormatExternalInterface =
            new BusinessObjectFormatExternalInterface(ID, businessObjectFormatExternalInterfaceKey);

        // Create a business object format to external interface mapping entity.
        BusinessObjectFormatExternalInterfaceEntity businessObjectFormatExternalInterfaceEntity = new BusinessObjectFormatExternalInterfaceEntity();

        // Mock the external calls.
        when(businessObjectFormatExternalInterfaceDaoHelper.getBusinessObjectFormatExternalInterfaceEntity(businessObjectFormatExternalInterfaceKey))
            .thenReturn(businessObjectFormatExternalInterfaceEntity);
        when(businessObjectFormatExternalInterfaceHelper.createBusinessObjectFormatExternalInterfaceFromEntity(businessObjectFormatExternalInterfaceEntity))
            .thenReturn(businessObjectFormatExternalInterface);

        // Call the method under test.
        BusinessObjectFormatExternalInterface result =
            businessObjectFormatExternalInterfaceService.deleteBusinessObjectFormatExternalInterface(businessObjectFormatExternalInterfaceKey);

        // Validate the results.
        assertEquals(businessObjectFormatExternalInterface, result);

        // Verify the external calls.
        verify(businessObjectFormatExternalInterfaceHelper).validateAndTrimBusinessObjectFormatExternalInterfaceKey(businessObjectFormatExternalInterfaceKey);
        verify(businessObjectFormatExternalInterfaceDaoHelper).getBusinessObjectFormatExternalInterfaceEntity(businessObjectFormatExternalInterfaceKey);
        verify(businessObjectFormatExternalInterfaceDao).delete(businessObjectFormatExternalInterfaceEntity);
        verify(businessObjectFormatExternalInterfaceHelper).createBusinessObjectFormatExternalInterfaceFromEntity(businessObjectFormatExternalInterfaceEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetBusinessObjectFormatExternalInterface()
    {
        // Create a business object format to external interface mapping key.
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey =
            new BusinessObjectFormatExternalInterfaceKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE);

        // Create a business object format to external interface mapping.
        BusinessObjectFormatExternalInterface businessObjectFormatExternalInterface =
            new BusinessObjectFormatExternalInterface(ID, businessObjectFormatExternalInterfaceKey);

        // Create a business object format to external interface mapping entity.
        BusinessObjectFormatExternalInterfaceEntity businessObjectFormatExternalInterfaceEntity = new BusinessObjectFormatExternalInterfaceEntity();

        // Mock the external calls.
        when(businessObjectFormatExternalInterfaceDaoHelper.getBusinessObjectFormatExternalInterfaceEntity(businessObjectFormatExternalInterfaceKey))
            .thenReturn(businessObjectFormatExternalInterfaceEntity);
        when(businessObjectFormatExternalInterfaceHelper.createBusinessObjectFormatExternalInterfaceFromEntity(businessObjectFormatExternalInterfaceEntity))
            .thenReturn(businessObjectFormatExternalInterface);

        // Call the method under test.
        BusinessObjectFormatExternalInterface result =
            businessObjectFormatExternalInterfaceService.getBusinessObjectFormatExternalInterface(businessObjectFormatExternalInterfaceKey);

        // Validate the results.
        assertEquals(businessObjectFormatExternalInterface, result);

        // Verify the external calls.
        verify(businessObjectFormatExternalInterfaceHelper).validateAndTrimBusinessObjectFormatExternalInterfaceKey(businessObjectFormatExternalInterfaceKey);
        verify(businessObjectFormatExternalInterfaceDaoHelper).getBusinessObjectFormatExternalInterfaceEntity(businessObjectFormatExternalInterfaceKey);
        verify(businessObjectFormatExternalInterfaceHelper).createBusinessObjectFormatExternalInterfaceFromEntity(businessObjectFormatExternalInterfaceEntity);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectFormatDaoHelper, businessObjectFormatExternalInterfaceDao, businessObjectFormatExternalInterfaceDaoHelper,
            businessObjectFormatExternalInterfaceHelper, externalInterfaceDaoHelper);
    }
}
