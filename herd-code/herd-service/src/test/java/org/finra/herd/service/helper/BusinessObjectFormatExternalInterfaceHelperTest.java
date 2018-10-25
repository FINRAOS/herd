package org.finra.herd.service.helper;

import static org.finra.herd.dao.AbstractDaoTest.BDEF_NAME;
import static org.finra.herd.dao.AbstractDaoTest.BDEF_NAME_2;
import static org.finra.herd.dao.AbstractDaoTest.EXTERNAL_INTERFACE;
import static org.finra.herd.dao.AbstractDaoTest.EXTERNAL_INTERFACE_2;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_FILE_TYPE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_FILE_TYPE_CODE_2;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_USAGE_CODE;
import static org.finra.herd.dao.AbstractDaoTest.FORMAT_USAGE_CODE_2;
import static org.finra.herd.dao.AbstractDaoTest.NAMESPACE;
import static org.finra.herd.dao.AbstractDaoTest.NAMESPACE_2;
import static org.finra.herd.service.AbstractServiceTest.ID;
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

import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterface;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatExternalInterfaceKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatExternalInterfaceEntity;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.NamespaceEntity;

public class BusinessObjectFormatExternalInterfaceHelperTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @InjectMocks
    private BusinessObjectFormatExternalInterfaceHelper businessObjectFormatExternalInterfaceHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateBusinessObjectFormatExternalInterfaceFromEntity()
    {
        // Create a namespace entity.
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);

        // Create a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(BDEF_NAME);

        // Create a file type entity.
        FileTypeEntity fileTypeEntity = new FileTypeEntity();
        fileTypeEntity.setCode(FORMAT_FILE_TYPE_CODE);

        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectFormatEntity.setUsage(FORMAT_USAGE_CODE);
        businessObjectFormatEntity.setFileType(fileTypeEntity);

        // Create a external interface entity.
        ExternalInterfaceEntity externalInterfaceEntity = new ExternalInterfaceEntity();
        externalInterfaceEntity.setCode(EXTERNAL_INTERFACE);

        // Create a business object format to external interface mapping entity.
        BusinessObjectFormatExternalInterfaceEntity businessObjectFormatExternalInterfaceEntity = new BusinessObjectFormatExternalInterfaceEntity();
        businessObjectFormatExternalInterfaceEntity.setId(ID);
        businessObjectFormatExternalInterfaceEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectFormatExternalInterfaceEntity.setExternalInterface(externalInterfaceEntity);

        // Call the method under test.
        BusinessObjectFormatExternalInterface result =
            businessObjectFormatExternalInterfaceHelper.createBusinessObjectFormatExternalInterfaceFromEntity(businessObjectFormatExternalInterfaceEntity);

        // Validate the results.
        assertEquals(new BusinessObjectFormatExternalInterface(ID,
            new BusinessObjectFormatExternalInterfaceKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE)), result);

        // Verify the external calls.
        verifyNoMoreInteractions(alternateKeyHelper);
    }

    @Test
    public void testValidateAndTrimBusinessObjectFormatExternalInterfaceCreateRequest()
    {
        // Create a business object format to external interface mapping key.
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey =
            new BusinessObjectFormatExternalInterfaceKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE);

        // Create a business object format to external interface mapping create request.
        BusinessObjectFormatExternalInterfaceCreateRequest businessObjectFormatExternalInterfaceCreateRequest =
            new BusinessObjectFormatExternalInterfaceCreateRequest(businessObjectFormatExternalInterfaceKey);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("namespace", NAMESPACE)).thenReturn(NAMESPACE_2);
        when(alternateKeyHelper.validateStringParameter("business object definition name", BDEF_NAME)).thenReturn(BDEF_NAME_2);
        when(alternateKeyHelper.validateStringParameter("business object format usage", FORMAT_USAGE_CODE)).thenReturn(FORMAT_USAGE_CODE_2);
        when(alternateKeyHelper.validateStringParameter("business object format file type", FORMAT_FILE_TYPE_CODE)).thenReturn(FORMAT_FILE_TYPE_CODE_2);
        when(alternateKeyHelper.validateStringParameter("An", "external interface name", EXTERNAL_INTERFACE)).thenReturn(EXTERNAL_INTERFACE_2);

        // Call the method under test.
        businessObjectFormatExternalInterfaceHelper
            .validateAndTrimBusinessObjectFormatExternalInterfaceCreateRequest(businessObjectFormatExternalInterfaceCreateRequest);

        // Validate the results.
        assertEquals(new BusinessObjectFormatExternalInterfaceCreateRequest(
                new BusinessObjectFormatExternalInterfaceKey(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, EXTERNAL_INTERFACE_2)),
            businessObjectFormatExternalInterfaceCreateRequest);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("namespace", NAMESPACE);
        verify(alternateKeyHelper).validateStringParameter("business object definition name", BDEF_NAME);
        verify(alternateKeyHelper).validateStringParameter("business object format usage", FORMAT_USAGE_CODE);
        verify(alternateKeyHelper).validateStringParameter("business object format file type", FORMAT_FILE_TYPE_CODE);
        verify(alternateKeyHelper).validateStringParameter("An", "external interface name", EXTERNAL_INTERFACE);
        verifyNoMoreInteractions(alternateKeyHelper);
    }

    @Test
    public void testValidateAndTrimBusinessObjectFormatExternalInterfaceCreateRequestBusinessObjectFormatExternalInterfaceCreateRequestIsNull()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A business object format to external interface mapping create request must be specified.");

        // Call the method under test.
        businessObjectFormatExternalInterfaceHelper.validateAndTrimBusinessObjectFormatExternalInterfaceCreateRequest(null);

        // Verify the external calls.
        verifyNoMoreInteractions(alternateKeyHelper);
    }

    @Test
    public void testValidateAndTrimBusinessObjectFormatExternalInterfaceKey()
    {
        // Create a business object format to external interface mapping key.
        BusinessObjectFormatExternalInterfaceKey businessObjectFormatExternalInterfaceKey =
            new BusinessObjectFormatExternalInterfaceKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("namespace", NAMESPACE)).thenReturn(NAMESPACE_2);
        when(alternateKeyHelper.validateStringParameter("business object definition name", BDEF_NAME)).thenReturn(BDEF_NAME_2);
        when(alternateKeyHelper.validateStringParameter("business object format usage", FORMAT_USAGE_CODE)).thenReturn(FORMAT_USAGE_CODE_2);
        when(alternateKeyHelper.validateStringParameter("business object format file type", FORMAT_FILE_TYPE_CODE)).thenReturn(FORMAT_FILE_TYPE_CODE_2);
        when(alternateKeyHelper.validateStringParameter("An", "external interface name", EXTERNAL_INTERFACE)).thenReturn(EXTERNAL_INTERFACE_2);

        // Call the method under test.
        businessObjectFormatExternalInterfaceHelper.validateAndTrimBusinessObjectFormatExternalInterfaceKey(businessObjectFormatExternalInterfaceKey);

        // Validate the results.
        assertEquals(new BusinessObjectFormatExternalInterfaceKey(NAMESPACE_2, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, EXTERNAL_INTERFACE_2),
            businessObjectFormatExternalInterfaceKey);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("namespace", NAMESPACE);
        verify(alternateKeyHelper).validateStringParameter("business object definition name", BDEF_NAME);
        verify(alternateKeyHelper).validateStringParameter("business object format usage", FORMAT_USAGE_CODE);
        verify(alternateKeyHelper).validateStringParameter("business object format file type", FORMAT_FILE_TYPE_CODE);
        verify(alternateKeyHelper).validateStringParameter("An", "external interface name", EXTERNAL_INTERFACE);
        verifyNoMoreInteractions(alternateKeyHelper);
    }

    @Test
    public void testValidateAndTrimBusinessObjectFormatExternalInterfaceKeyBusinessObjectFormatExternalInterfaceKeyIsNull()
    {
        // Specify the expected exception.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("A business object format to external interface mapping key must be specified.");

        // Call the method under test.
        businessObjectFormatExternalInterfaceHelper.validateAndTrimBusinessObjectFormatExternalInterfaceKey(null);

        // Verify the external calls.
        verifyNoMoreInteractions(alternateKeyHelper);
    }
}
