package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.AllowedAttributeValuesCreateRequest;
import org.finra.herd.model.api.xml.AllowedAttributeValuesDeleteRequest;
import org.finra.herd.model.api.xml.AllowedAttributeValuesInformation;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.service.AllowedAttributeValueService;

/**
 * This class tests the functionality of allowed attribute value rest controller
 */
public class AllowedAttributeValueRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private AllowedAttributeValueRestController allowedAttributeValueRestController;

    @Mock
    private AllowedAttributeValueService allowedAttributeValueService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateAllowedAttributeValue()
    {
        // Create attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(NAMESPACE_CODE, ATTRIBUTE_VALUE_LIST_NAME);
        AllowedAttributeValuesCreateRequest request = new AllowedAttributeValuesCreateRequest(attributeValueListKey, Arrays.asList(ALLOWED_ATTRIBUTE_VALUE));

        // Create the allowed attribute values information.
        AllowedAttributeValuesInformation allowedAttributeValuesInformation = new AllowedAttributeValuesInformation();
        allowedAttributeValuesInformation.setAttributeValueListKey(attributeValueListKey);
        allowedAttributeValuesInformation.setAllowedAttributeValues(Arrays.asList(ALLOWED_ATTRIBUTE_VALUE));

        // Mock calls to external method.
        when(allowedAttributeValueService.createAllowedAttributeValues(request)).thenReturn(allowedAttributeValuesInformation);

        // Call the method under test.
        AllowedAttributeValuesInformation response = allowedAttributeValueRestController.createAllowedAttributeValues(request);

        // Verify the external calls.
        verify(allowedAttributeValueService).createAllowedAttributeValues(request);
        verifyNoMoreInteractions(allowedAttributeValueService);

        // Validate the response.
        assertEquals(allowedAttributeValuesInformation, response);
    }

    @Test
    public void testDeleteAllowedAttributeValue()
    {
        // Create attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(NAMESPACE_CODE, ATTRIBUTE_VALUE_LIST_NAME);
        AllowedAttributeValuesDeleteRequest request = new AllowedAttributeValuesDeleteRequest(attributeValueListKey, Arrays.asList(ALLOWED_ATTRIBUTE_VALUE));

        // Create the allowed attribute values information.
        AllowedAttributeValuesInformation allowedAttributeValuesInformation = new AllowedAttributeValuesInformation();
        allowedAttributeValuesInformation.setAttributeValueListKey(attributeValueListKey);
        allowedAttributeValuesInformation.setAllowedAttributeValues(Arrays.asList(ALLOWED_ATTRIBUTE_VALUE));

        // Mock calls to external method.
        when(allowedAttributeValueService.deleteAllowedAttributeValues(request)).thenReturn(allowedAttributeValuesInformation);

        // Call the method under test.
        AllowedAttributeValuesInformation response = allowedAttributeValueRestController.deleteAllowedAttributeValues(request);

        // Verify the external calls.
        verify(allowedAttributeValueService).deleteAllowedAttributeValues(request);
        verifyNoMoreInteractions(allowedAttributeValueService);

        // Validate the response.
        assertEquals(allowedAttributeValuesInformation, response);
    }

    @Test
    public void testGetAllowedAttributeValue()
    {
        // Create attribute value list key.
        AttributeValueListKey attributeValueListKey = new AttributeValueListKey(NAMESPACE_CODE, ATTRIBUTE_VALUE_LIST_NAME);

        // Create the allowed attribute values information.
        AllowedAttributeValuesInformation allowedAttributeValuesInformation = new AllowedAttributeValuesInformation();
        allowedAttributeValuesInformation.setAttributeValueListKey(attributeValueListKey);
        allowedAttributeValuesInformation.setAllowedAttributeValues(Arrays.asList(ALLOWED_ATTRIBUTE_VALUE));

        // Mock calls to external method.
        when(allowedAttributeValueService.getAllowedAttributeValues(attributeValueListKey)).thenReturn(allowedAttributeValuesInformation);

        // Call the method under test.
        AllowedAttributeValuesInformation response = allowedAttributeValueRestController.getAllowedAttributeValues(NAMESPACE_CODE, ATTRIBUTE_VALUE_LIST_NAME);

        // Verify the external calls.
        verify(allowedAttributeValueService).getAllowedAttributeValues(attributeValueListKey);
        verifyNoMoreInteractions(allowedAttributeValueService);

        // Validate the response.
        assertEquals(allowedAttributeValuesInformation, response);
    }
}
