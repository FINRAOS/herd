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

import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageAttributesUpdateRequest;
import org.finra.herd.model.api.xml.StorageCreateRequest;
import org.finra.herd.model.api.xml.StorageKey;
import org.finra.herd.model.api.xml.StorageKeys;
import org.finra.herd.model.api.xml.StorageUpdateRequest;
import org.finra.herd.service.StorageService;

/**
 * This class tests various functionality within the storage REST controller.
 */
public class StorageRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private StorageRestController storageRestController;

    @Mock
    private StorageService storageService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateStorage()
    {
        // Create a storage create request.
        StorageCreateRequest storageCreateRequest = new StorageCreateRequest(STORAGE_NAME, STORAGE_PLATFORM_CODE, NO_ATTRIBUTES);

        // Create a storage.
        Storage storage = new Storage(STORAGE_NAME, STORAGE_PLATFORM_CODE, NO_ATTRIBUTES);

        // Mock the external calls.
        when(storageService.createStorage(storageCreateRequest)).thenReturn(storage);

        // Call the method under test.
        Storage result = storageRestController.createStorage(storageCreateRequest);

        // Verify the external calls.
        verify(storageService).createStorage(storageCreateRequest);
        verifyNoMoreInteractions(storageService);

        // Validate the returned object.
        assertEquals(storage, result);
    }

    @Test
    public void testDeleteStorage()
    {
        // Create a storage key.
        StorageKey storageKey = new StorageKey(STORAGE_NAME);

        // Create a storage.
        Storage storage = new Storage(STORAGE_NAME, STORAGE_PLATFORM_CODE, NO_ATTRIBUTES);

        // Mock the external calls.
        when(storageService.deleteStorage(storageKey)).thenReturn(storage);

        // Call the method under test.
        Storage result = storageRestController.deleteStorage(STORAGE_NAME);

        // Verify the external calls.
        verify(storageService).deleteStorage(storageKey);
        verifyNoMoreInteractions(storageService);

        // Validate the returned object.
        assertEquals(storage, result);
    }

    @Test
    public void testGetStorage()
    {
        // Create a storage key.
        StorageKey storageKey = new StorageKey(STORAGE_NAME);

        // Create a storage.
        Storage storage = new Storage(STORAGE_NAME, STORAGE_PLATFORM_CODE, NO_ATTRIBUTES);

        // Mock the external calls.
        when(storageService.getStorage(storageKey)).thenReturn(storage);

        // Call the method under test.
        Storage result = storageRestController.getStorage(STORAGE_NAME);

        // Verify the external calls.
        verify(storageService).getStorage(storageKey);
        verifyNoMoreInteractions(storageService);

        // Validate the returned object.
        assertEquals(storage, result);
    }

    @Test
    public void testGetStorages()
    {
        // Get a list of storage keys.
        StorageKeys storageKeys = new StorageKeys(Arrays.asList(new StorageKey(STORAGE_NAME), new StorageKey(STORAGE_NAME_2)));

        // Mock the external calls.
        when(storageService.getAllStorage()).thenReturn(storageKeys);

        // Call the method under test.
        StorageKeys result = storageRestController.getStorages();

        // Verify the external calls.
        verify(storageService).getAllStorage();
        verifyNoMoreInteractions(storageService);

        // Validate the returned object.
        assertEquals(storageKeys, result);
    }

    @Test
    public void testUpdateStorage()
    {
        // Create a storage key.
        StorageKey storageKey = new StorageKey(STORAGE_NAME);

        // Create a storage update request.
        StorageUpdateRequest storageUpdateRequest = new StorageUpdateRequest();

        // Create a storage.
        Storage storage = new Storage(STORAGE_NAME, STORAGE_PLATFORM_CODE, NO_ATTRIBUTES);

        // Mock the external calls.
        when(storageService.updateStorage(storageKey, storageUpdateRequest)).thenReturn(storage);

        // Call the method under test.
        Storage result = storageRestController.updateStorage(STORAGE_NAME, storageUpdateRequest);

        // Verify the external calls.
        verify(storageService).updateStorage(storageKey, storageUpdateRequest);
        verifyNoMoreInteractions(storageService);

        // Validate the returned object.
        assertEquals(storage, result);
    }

    @Test
    public void testUpdateStorageAttributes()
    {
        // Create a storage key.
        StorageKey storageKey = new StorageKey(STORAGE_NAME);

        // Create a storage update request.
        StorageAttributesUpdateRequest storageAttributesUpdateRequest =
            new StorageAttributesUpdateRequest(businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Create a storage.
        Storage storage = new Storage(STORAGE_NAME, STORAGE_PLATFORM_CODE, businessObjectDefinitionServiceTestHelper.getNewAttributes());

        // Mock the external calls.
        when(storageService.updateStorageAttributes(storageKey, storageAttributesUpdateRequest)).thenReturn(storage);

        // Call the method under test.
        Storage result = storageRestController.updateStorageAttributes(STORAGE_NAME, storageAttributesUpdateRequest);

        // Verify the external calls.
        verify(storageService).updateStorageAttributes(storageKey, storageAttributesUpdateRequest);
        verifyNoMoreInteractions(storageService);

        // Validate the returned object.
        assertEquals(storage, result);
    }
}
