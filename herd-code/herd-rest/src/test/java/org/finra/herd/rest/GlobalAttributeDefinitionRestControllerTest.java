package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.GlobalAttributeDefinition;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionCreateRequest;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKey;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKeys;
import org.finra.herd.service.GlobalAttributeDefinitionService;

/**
 * This class tests the functionality of global attribute definition rest controller
 */
public class GlobalAttributeDefinitionRestControllerTest extends AbstractRestTest
{
    @Mock
    private GlobalAttributeDefinitionService globalAttributeDefinitionService;

    @InjectMocks
    private GlobalAttributeDefinitionRestController globalAttributeDefinitionRestController;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateGlobalAttributeDefinition()
    {
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);
        GlobalAttributeDefinitionCreateRequest request = new GlobalAttributeDefinitionCreateRequest(globalAttributeDefinitionKey);

        //create a global attribute definition
        GlobalAttributeDefinition globalAttributeDefinition = new GlobalAttributeDefinition();
        globalAttributeDefinition.setId(1);
        globalAttributeDefinition.setGlobalAttributeDefinitionKey(globalAttributeDefinitionKey);

        //mock calls to external method
        when(globalAttributeDefinitionService.createGlobalAttributeDefinition(request)).thenReturn(globalAttributeDefinition);

        //call method under  test
        GlobalAttributeDefinition response = globalAttributeDefinitionRestController.createGlobalAttributeDefinition(request);

        //verify
        verify(globalAttributeDefinitionService).createGlobalAttributeDefinition(request);
        verifyNoMoreInteractions(globalAttributeDefinitionService);

        //validate
        assertEquals(globalAttributeDefinition, response);
    }

    @Test
    public void testDeleteGlobalAttributeDefinition()
    {
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);

        //create a global attribute definition
        GlobalAttributeDefinition globalAttributeDefinition = new GlobalAttributeDefinition();
        globalAttributeDefinition.setId(1);
        globalAttributeDefinition.setGlobalAttributeDefinitionKey(globalAttributeDefinitionKey);

        //mock calls to external method
        when(globalAttributeDefinitionService.deleteGlobalAttributeDefinition(globalAttributeDefinitionKey)).thenReturn(globalAttributeDefinition);

        //call method under test
        GlobalAttributeDefinition response = globalAttributeDefinitionRestController
            .deleteGlobalAttributeDefinition(globalAttributeDefinitionKey.getGlobalAttributeDefinitionLevel(),
                globalAttributeDefinitionKey.getGlobalAttributeDefinitionName());

        //verify the calls
        verify(globalAttributeDefinitionService).deleteGlobalAttributeDefinition(globalAttributeDefinitionKey);
        verifyNoMoreInteractions(globalAttributeDefinitionService);

        //validate
        assertEquals(globalAttributeDefinition, response);
    }

    @Test
    public void testGetGlobalAttributeDefinition()
    {
        //create the keys
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey1 =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_2);
        GlobalAttributeDefinitionKeys globalAttributeDefinitionKeys =
            new GlobalAttributeDefinitionKeys(Arrays.asList(globalAttributeDefinitionKey, globalAttributeDefinitionKey1));

        // Mock calls to external methods
        when(globalAttributeDefinitionService.getGlobalAttributeDefinitionKeys()).thenReturn(globalAttributeDefinitionKeys);

        //call method under test
        GlobalAttributeDefinitionKeys response = globalAttributeDefinitionRestController.getGlobalAttributeDefinitions();

        //verify
        verify(globalAttributeDefinitionService).getGlobalAttributeDefinitionKeys();
        verifyNoMoreInteractions(globalAttributeDefinitionService);

        //validate
        assertNotNull(response);
        assertEquals(response.getGlobalAttributeDefinitionKeys(), Arrays.asList(globalAttributeDefinitionKey, globalAttributeDefinitionKey1));
    }
}
