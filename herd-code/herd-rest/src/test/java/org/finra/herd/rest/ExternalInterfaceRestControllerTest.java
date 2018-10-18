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

import org.finra.herd.model.api.xml.ExternalInterface;
import org.finra.herd.model.api.xml.ExternalInterfaceCreateRequest;
import org.finra.herd.model.api.xml.ExternalInterfaceKey;
import org.finra.herd.model.api.xml.ExternalInterfaceKeys;
import org.finra.herd.model.api.xml.ExternalInterfaceUpdateRequest;
import org.finra.herd.service.ExternalInterfaceService;

/**
 * This class tests various functionality within the external interface REST controller.
 */
public class ExternalInterfaceRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private ExternalInterfaceRestController externalInterfaceRestController;

    @Mock
    private ExternalInterfaceService externalInterfaceService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateExternalInterface()
    {
        ExternalInterfaceKey externalInterfaceKey = new ExternalInterfaceKey(EXTERNAL_INTERFACE);
        ExternalInterfaceCreateRequest externalInterfaceCreateRequest =
            new ExternalInterfaceCreateRequest(externalInterfaceKey, DISPLAY_NAME_FIELD, DESCRIPTION);

        // Create an external interface.
        ExternalInterface externalInterface = new ExternalInterface(externalInterfaceKey, DISPLAY_NAME_FIELD, DESCRIPTION);

        when(externalInterfaceService.createExternalInterface(externalInterfaceCreateRequest)).thenReturn(externalInterface);

        ExternalInterface resultExternalInterface =
            externalInterfaceRestController.createExternalInterface(new ExternalInterfaceCreateRequest(externalInterfaceKey, DISPLAY_NAME_FIELD, DESCRIPTION));

        // Validate the returned object.
        assertEquals(new ExternalInterface(externalInterfaceKey, DISPLAY_NAME_FIELD, DESCRIPTION), resultExternalInterface);

        // Verify the external calls.
        verify(externalInterfaceService).createExternalInterface(externalInterfaceCreateRequest);
        verifyNoMoreInteractions(externalInterfaceService);

        // Validate the returned object.
        assertEquals(externalInterface, resultExternalInterface);
    }

    @Test
    public void testDeleteExternalInterface()
    {
        ExternalInterfaceKey externalInterfaceKey = new ExternalInterfaceKey(EXTERNAL_INTERFACE);

        // Create an external interface.
        ExternalInterface externalInterface = new ExternalInterface(externalInterfaceKey, DISPLAY_NAME_FIELD, DESCRIPTION);

        when(externalInterfaceService.deleteExternalInterface(new ExternalInterfaceKey(EXTERNAL_INTERFACE))).thenReturn(externalInterface);

        ExternalInterface deletedExternalInterface = externalInterfaceRestController.deleteExternalInterface(EXTERNAL_INTERFACE);

        // Verify the external calls.
        verify(externalInterfaceService).deleteExternalInterface(new ExternalInterfaceKey(EXTERNAL_INTERFACE));
        verifyNoMoreInteractions(externalInterfaceService);

        // Validate the returned object.
        assertEquals(externalInterface, deletedExternalInterface);
    }

    @Test
    public void testGetExternalInterface()
    {
        ExternalInterfaceKey externalInterfaceKey = new ExternalInterfaceKey(EXTERNAL_INTERFACE);

        // Create an external interface.
        ExternalInterface externalInterface = new ExternalInterface(externalInterfaceKey, DISPLAY_NAME_FIELD, DESCRIPTION);

        when(externalInterfaceService.getExternalInterface(new ExternalInterfaceKey(EXTERNAL_INTERFACE))).thenReturn(externalInterface);

        // Retrieve the external interface.
        ExternalInterface resultExternalInterface = externalInterfaceRestController.getExternalInterface(EXTERNAL_INTERFACE);

        // Verify the external calls.
        verify(externalInterfaceService).getExternalInterface(new ExternalInterfaceKey(EXTERNAL_INTERFACE));
        verifyNoMoreInteractions(externalInterfaceService);

        // Validate the returned object.
        assertEquals(externalInterface, resultExternalInterface);
    }

    @Test
    public void testGetExternalInterfaces()
    {
        ExternalInterfaceKeys externalInterfaceKeys =
            new ExternalInterfaceKeys(Arrays.asList(new ExternalInterfaceKey(EXTERNAL_INTERFACE), new ExternalInterfaceKey(EXTERNAL_INTERFACE_2)));

        when(externalInterfaceService.getExternalInterfaces()).thenReturn(externalInterfaceKeys);

        // Retrieve a list of external interface keys.
        ExternalInterfaceKeys resultExternalInterfaceKeys = externalInterfaceRestController.getExternalInterfaces();

        // Verify the external calls.
        verify(externalInterfaceService).getExternalInterfaces();
        verifyNoMoreInteractions(externalInterfaceService);

        // Validate the returned object.
        assertEquals(externalInterfaceKeys, resultExternalInterfaceKeys);
    }

    @Test
    public void testUpdateExternalInterface()
    {
        ExternalInterfaceKey externalInterfaceKey = new ExternalInterfaceKey(EXTERNAL_INTERFACE);
        ExternalInterfaceUpdateRequest externalInterfaceUpdateRequest = new ExternalInterfaceUpdateRequest(DISPLAY_NAME_FIELD, DESCRIPTION);

        // Create an external interface.
        ExternalInterface externalInterface = new ExternalInterface(externalInterfaceKey, DISPLAY_NAME_FIELD, DESCRIPTION);

        when(externalInterfaceService.updateExternalInterface(externalInterfaceKey, externalInterfaceUpdateRequest)).thenReturn(externalInterface);

        ExternalInterface resultExternalInterface =
            externalInterfaceRestController.updateExternalInterface(EXTERNAL_INTERFACE, externalInterfaceUpdateRequest);

        // Validate the returned object.
        assertEquals(new ExternalInterface(externalInterfaceKey, DISPLAY_NAME_FIELD, DESCRIPTION), resultExternalInterface);

        // Verify the external calls.
        verify(externalInterfaceService).updateExternalInterface(externalInterfaceKey, externalInterfaceUpdateRequest);
        verifyNoMoreInteractions(externalInterfaceService);

        // Validate the returned object.
        assertEquals(externalInterface, resultExternalInterface);
    }
}
