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
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.Namespace;
import org.finra.herd.model.api.xml.NamespaceCreateRequest;
import org.finra.herd.model.api.xml.NamespaceKey;
import org.finra.herd.model.api.xml.NamespaceKeys;
import org.finra.herd.model.api.xml.NamespaceSearchFilter;
import org.finra.herd.model.api.xml.NamespaceSearchKey;
import org.finra.herd.model.api.xml.NamespaceSearchRequest;
import org.finra.herd.model.api.xml.NamespaceSearchResponse;
import org.finra.herd.model.api.xml.NamespaceUpdateRequest;
import org.finra.herd.service.NamespaceService;
import org.finra.herd.service.impl.NamespaceServiceImpl;

/**
 * This class tests various functionality within the namespace REST controller.
 */
public class NamespaceRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private NamespaceRestController namespaceRestController;

    @Mock
    private NamespaceService namespaceService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateNamespace()
    {
        NamespaceCreateRequest request = new NamespaceCreateRequest(NAMESPACE, CHARGE_CODE);
        Namespace namespace = new Namespace(NAMESPACE, CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX);

        // Mock the external calls.
        when(namespaceService.createNamespace(request)).thenReturn(namespace);

        // Call the method under test.
        Namespace resultNamespace = namespaceRestController.createNamespace(request);

        // Validate the returned object.
        assertEquals(namespace, resultNamespace);

        // Verify the external calls.
        verify(namespaceService).createNamespace(request);
        verifyNoMoreInteractions(namespaceService);
    }

    @Test
    public void testDeleteNamespace()
    {
        Namespace namespace = new Namespace(NAMESPACE, CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX);

        // Mock the external calls.
        when(namespaceService.deleteNamespace(new NamespaceKey(NAMESPACE))).thenReturn(namespace);

        // Call the method under test.
        Namespace deletedNamespace = namespaceRestController.deleteNamespace(NAMESPACE);

        // Validate the returned object.
        assertEquals(namespace, deletedNamespace);

        // Verify the external calls.
        verify(namespaceService).deleteNamespace(new NamespaceKey(NAMESPACE));
        verifyNoMoreInteractions(namespaceService);
    }

    @Test
    public void testGetNamespace()
    {
        Namespace namespace = new Namespace(NAMESPACE, CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX);

        // Mock the external calls.
        when(namespaceService.getNamespace(new NamespaceKey(NAMESPACE))).thenReturn(namespace);

        // Call the method under test.
        Namespace resultNamespace = namespaceRestController.getNamespace(NAMESPACE);

        // Validate the returned object.
        assertEquals(namespace, resultNamespace);

        // Verify the external calls.
        verify(namespaceService).getNamespace(new NamespaceKey(NAMESPACE));
        verifyNoMoreInteractions(namespaceService);
    }

    @Test
    public void testGetNamespaces()
    {
        NamespaceKeys namespaceKeys = new NamespaceKeys(Arrays.asList(new NamespaceKey(NAMESPACE), new NamespaceKey(NAMESPACE_2)));

        // Mock the external calls.
        when(namespaceService.getNamespaces()).thenReturn(namespaceKeys);

        // Call the method under test.
        NamespaceKeys resultNamespaceKeys = namespaceRestController.getNamespaces();

        // Validate the returned object.
        assertEquals(namespaceKeys, resultNamespaceKeys);

        // Verify the external calls.
        verify(namespaceService).getNamespaces();
        verifyNoMoreInteractions(namespaceService);
    }

    @Test
    public void testUpdateNamespace()
    {
        NamespaceUpdateRequest request = new NamespaceUpdateRequest(CHARGE_CODE);
        Namespace namespace = new Namespace(NAMESPACE, CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX);

        // Mock the external calls.
        when(namespaceService.updateNamespaces(new NamespaceKey(NAMESPACE), request)).thenReturn(namespace);

        // Call the method under test.
        Namespace result = namespaceRestController.updateNamespaces(NAMESPACE, request);

        // Validate the returned object.
        assertEquals(namespace, result);

        // Verify the external calls.
        verify(namespaceService).updateNamespaces(new NamespaceKey(NAMESPACE), request);
        verifyNoMoreInteractions(namespaceService);
    }

    @Test
    public void testSearchNamespaces()
    {
        NamespaceSearchResponse namespaceSearchResponse = new NamespaceSearchResponse(Arrays
            .asList(new Namespace(NAMESPACE, NAMESPACE_CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX),
                new Namespace(NAMESPACE_2, NAMESPACE_CHARGE_CODE, NAMESPACE_S3_KEY_PREFIX_2)));

        NamespaceSearchRequest namespaceSearchRequest =
            new NamespaceSearchRequest(Arrays.asList(new NamespaceSearchFilter(Arrays.asList(new NamespaceSearchKey(NAMESPACE_CHARGE_CODE, false)))));

        Set<String> fields = Sets.newHashSet(NamespaceServiceImpl.CHARGE_CODE_FIELD, NamespaceServiceImpl.S3_KEY_PREFIX_FIELD);

        when(namespaceService.searchNamespaces(namespaceSearchRequest, fields)).thenReturn(namespaceSearchResponse);

        // Search the namespaces
        NamespaceSearchResponse resultNamespaceSearchResponse = namespaceRestController.searchNamespaces(namespaceSearchRequest, fields);

        // Verify the external calls
        verify(namespaceService).searchNamespaces(namespaceSearchRequest, fields);
        verifyNoMoreInteractions(namespaceService);

        // Validate the returned object.
        assertEquals(namespaceSearchResponse, resultNamespaceSearchResponse);

    }
}
