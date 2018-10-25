package org.finra.herd.service.helper;

import static org.finra.herd.dao.AbstractDaoTest.BDEF_NAME;
import static org.finra.herd.dao.AbstractDaoTest.EXTERNAL_INTERFACE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_FILE_TYPE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_USAGE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.NAMESPACE;
import static org.junit.Assert.assertEquals;
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

import org.finra.herd.dao.BusinessObjectFormatDao;
import org.finra.herd.dao.BusinessObjectFormatExternalInterfaceDao;
import org.finra.herd.dao.ExternalInterfaceDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatExternalInterfaceEntity;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;

public class BusinessObjectFormatExternalInterfaceDaoHelperTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private BusinessObjectFormatDao businessObjectFormatDao;

    @Mock
    private BusinessObjectFormatExternalInterfaceDao businessObjectFormatExternalInterfaceDao;

    @InjectMocks
    private BusinessObjectFormatExternalInterfaceDaoHelper businessObjectFormatExternalInterfaceDaoHelper;

    @Mock
    private ExternalInterfaceDao externalInterfaceDao;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetBusinessObjectFormatExternalInterfaceEntity()
    {
        // Create a business object format to external interface mapping key.
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey =
            new BusinessObjectFormatExternalInterfaceKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE);

        // Create a version-less business object format key.
        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create a external interface entity.
        ExternalInterfaceEntity externalInterfaceEntity = new ExternalInterfaceEntity();

        // Create a business object format to external interface mapping entity.
        BusinessObjectFormatExternalInterfaceEntity businessObjectFormatExternalInterfaceEntity = new BusinessObjectFormatExternalInterfaceEntity();

        // Mock the external calls.
        when(businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(externalInterfaceDao.getExternalInterfaceByName(EXTERNAL_INTERFACE)).thenReturn(externalInterfaceEntity);
        when(businessObjectFormatExternalInterfaceDao
            .getBusinessObjectFormatExternalInterfaceByBusinessObjectFormatAndExternalInterface(businessObjectFormatEntity, externalInterfaceEntity))
            .thenReturn(businessObjectFormatExternalInterfaceEntity);

        // Call the method under test.
        BusinessObjectFormatExternalInterfaceEntity result =
            businessObjectFormatExternalInterfaceDaoHelper.getBusinessObjectFormatExternalInterfaceEntity(businessObjectFormatExternalInterfaceKey);

        // Validate the results.
        assertEquals(businessObjectFormatExternalInterfaceEntity, result);

        // Verify the external calls.
        verify(businessObjectFormatDao).getBusinessObjectFormatByAltKey(businessObjectFormatKey);
        verify(externalInterfaceDao).getExternalInterfaceByName(EXTERNAL_INTERFACE);
        verify(businessObjectFormatExternalInterfaceDao)
            .getBusinessObjectFormatExternalInterfaceByBusinessObjectFormatAndExternalInterface(businessObjectFormatEntity, externalInterfaceEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetBusinessObjectFormatExternalInterfaceEntityBusinessObjectFormatExternalInterfaceNoExists()
    {
        // Create a business object format to external interface mapping key.
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey =
            new BusinessObjectFormatExternalInterfaceKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE);

        // Create a version-less business object format key.
        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Create a external interface entity.
        ExternalInterfaceEntity externalInterfaceEntity = new ExternalInterfaceEntity();

        // Mock the external calls.
        when(businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(externalInterfaceDao.getExternalInterfaceByName(EXTERNAL_INTERFACE)).thenReturn(externalInterfaceEntity);
        when(businessObjectFormatExternalInterfaceDao
            .getBusinessObjectFormatExternalInterfaceByBusinessObjectFormatAndExternalInterface(businessObjectFormatEntity, externalInterfaceEntity))
            .thenReturn(null);

        // Specify the expected exception.
        expectedException.expect(ObjectNotFoundException.class);
        expectedException.expectMessage(String.format("Business object format to external interface mapping with \"%s\" namespace, " +
            "\"%s\" business object definition name, \"%s\" business object format usage, \"%s\" business object format file type, and " +
            "\"%s\" external interface name doesn't exist.", NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE));

        // Call the method under test.
        businessObjectFormatExternalInterfaceDaoHelper.getBusinessObjectFormatExternalInterfaceEntity(businessObjectFormatExternalInterfaceKey);

        // Verify the external calls.
        verify(businessObjectFormatDao).getBusinessObjectFormatByAltKey(businessObjectFormatKey);
        verify(externalInterfaceDao).getExternalInterfaceByName(EXTERNAL_INTERFACE);
        verify(businessObjectFormatExternalInterfaceDao)
            .getBusinessObjectFormatExternalInterfaceByBusinessObjectFormatAndExternalInterface(businessObjectFormatEntity, externalInterfaceEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetBusinessObjectFormatExternalInterfaceEntityBusinessObjectFormatNoExists()
    {
        // Create a business object format to external interface mapping key.
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey =
            new BusinessObjectFormatExternalInterfaceKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE);

        // Create a version-less business object format key.
        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null);

        // Mock the external calls.
        when(businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKey)).thenReturn(null);

        // Specify the expected exception.
        expectedException.expect(ObjectNotFoundException.class);
        expectedException.expectMessage(String.format("Business object format to external interface mapping with \"%s\" namespace, " +
            "\"%s\" business object definition name, \"%s\" business object format usage, \"%s\" business object format file type, and " +
            "\"%s\" external interface name doesn't exist.", NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE));

        // Call the method under test.
        businessObjectFormatExternalInterfaceDaoHelper.getBusinessObjectFormatExternalInterfaceEntity(businessObjectFormatExternalInterfaceKey);

        // Verify the external calls.
        verify(businessObjectFormatDao).getBusinessObjectFormatByAltKey(businessObjectFormatKey);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetBusinessObjectFormatExternalInterfaceEntityExternalInterfaceNoExists()
    {
        // Create a business object format to external interface mapping key.
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey =
            new BusinessObjectFormatExternalInterfaceKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE);

        // Create a version-less business object format key.
        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        // Mock the external calls.
        when(businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKey)).thenReturn(businessObjectFormatEntity);
        when(externalInterfaceDao.getExternalInterfaceByName(EXTERNAL_INTERFACE)).thenReturn(null);

        // Specify the expected exception.
        expectedException.expect(ObjectNotFoundException.class);
        expectedException.expectMessage(String.format("Business object format to external interface mapping with \"%s\" namespace, " +
            "\"%s\" business object definition name, \"%s\" business object format usage, \"%s\" business object format file type, and " +
            "\"%s\" external interface name doesn't exist.", NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE));

        // Call the method under test.
        businessObjectFormatExternalInterfaceDaoHelper.getBusinessObjectFormatExternalInterfaceEntity(businessObjectFormatExternalInterfaceKey);

        // Verify the external calls.
        verify(businessObjectFormatDao).getBusinessObjectFormatByAltKey(businessObjectFormatKey);
        verify(externalInterfaceDao).getExternalInterfaceByName(EXTERNAL_INTERFACE);
        verifyNoMoreInteractionsHelper();
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(businessObjectFormatDao, businessObjectFormatExternalInterfaceDao, externalInterfaceDao);
    }
}
