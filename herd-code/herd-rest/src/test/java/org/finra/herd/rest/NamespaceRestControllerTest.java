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

import org.finra.herd.model.api.xml.Namespace;
import org.finra.herd.model.api.xml.NamespaceCreateRequest;
import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.api.xml.NamespaceKeys;
import org.finra.herd.service.NamespaceService;

/**
 * This class tests various functionality within the namespace REST controller.
 */
public class NamespaceRestControllerTest extends AbstractRestTest
{
    @Mock
    private NamespaceService namespaceService;

    @InjectMocks
    private NamespaceRestController namespaceRestController;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateNamespace() throws Exception
    {
        NamespaceCreateRequest request = namespaceServiceTestHelper.createNamespaceCreateRequest(NAMESPACE);
        Namespace namespace = new Namespace(NAMESPACE);
        when(namespaceService.createNamespace(request)).thenReturn(namespace);
        // Create a namespace.
        Namespace resultNamespace = namespaceRestController.createNamespace(request);

        // Validate the returned object.
        namespaceServiceTestHelper.validateNamespace(NAMESPACE, resultNamespace);

        // Verify the external calls.
        verify(namespaceService).createNamespace(request);
        verifyNoMoreInteractions(namespaceService);
        // Validate the returned object.
        assertEquals(namespace, resultNamespace);
    }

    @Test
    public void testGetNamespace() throws Exception
    {
        Namespace namespace = new Namespace(NAMESPACE);
        when(namespaceService.getNamespace(new NamespaceKey(NAMESPACE))).thenReturn(namespace);

        // Retrieve the namespace.
        Namespace resultNamespace = namespaceRestController.getNamespace(NAMESPACE);

        // Verify the external calls.
        verify(namespaceService).getNamespace(new NamespaceKey(NAMESPACE));
        verifyNoMoreInteractions(namespaceService);
        // Validate the returned object.
        assertEquals(namespace, resultNamespace);
    }

    @Test
    public void testGetNamespaces() throws Exception
    {
        NamespaceKeys namespaceKeys = new NamespaceKeys(Arrays.asList(new NamespaceKey(NAMESPACE), new NamespaceKey(NAMESPACE_2)));
        when(namespaceService.getNamespaces()).thenReturn(namespaceKeys);

        // Retrieve a list of namespace keys.
        NamespaceKeys resultNamespaceKeys = namespaceRestController.getNamespaces();

        // Verify the external calls.
        verify(namespaceService).getNamespaces();
        verifyNoMoreInteractions(namespaceService);
        // Validate the returned object.
        assertEquals(namespaceKeys, resultNamespaceKeys);
    }

    @Test
    public void testDeleteNamespace() throws Exception
    {
        Namespace namespace = new Namespace(NAMESPACE);

        when(namespaceService.deleteNamespace(new NamespaceKey(NAMESPACE))).thenReturn(namespace);

        // Delete this namespace.
        Namespace deletedNamespace = namespaceRestController.deleteNamespace(NAMESPACE);

        // Verify the external calls.
        verify(namespaceService).deleteNamespace(new NamespaceKey(NAMESPACE));
        verifyNoMoreInteractions(namespaceService);
        // Validate the returned object.
        assertEquals(namespace, deletedNamespace);
    }
}
