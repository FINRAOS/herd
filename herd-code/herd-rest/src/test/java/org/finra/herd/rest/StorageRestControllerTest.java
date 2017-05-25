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
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageCreateRequest;
import org.finra.herd.model.api.xml.StorageKey;
import org.finra.herd.model.api.xml.StorageKeys;
import org.finra.herd.model.api.xml.StorageUpdateRequest;
import org.finra.herd.model.dto.StorageAlternateKeyDto;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.service.StorageService;

/**
 * This class tests various functionality within the storage REST controller.
 */
public class StorageRestControllerTest extends AbstractRestTest
{
    @Mock
    private StorageService storageService;

    @InjectMocks
    private StorageRestController storageRestController;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateStorage() throws Exception
    {
        // Create and persist a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();
        Storage storage = new Storage();
        storage.setName(storageCreateRequest.getName());
        storage.setAttributes(storageCreateRequest.getAttributes());

        when(storageService.createStorage(storageCreateRequest)).thenReturn(storage);

        Storage resultStorage = storageRestController.createStorage(storageCreateRequest);

        // Verify the external calls.
        verify(storageService).createStorage(storageCreateRequest);
        verifyNoMoreInteractions(storageService);

        // Validate the returned object.
        assertEquals(storage, resultStorage);
    }

    @Test
    public void testUpdateStorage() throws Exception
    {
        // Create a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();
        Storage storage = new Storage();
        String storageName = storageCreateRequest.getName();
        storage.setName(storageName);
        storage.setAttributes(storageCreateRequest.getAttributes());

        // Update the storage platform which is valid.
        StorageUpdateRequest storageUpdateRequest = new StorageUpdateRequest();
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().withStorageName(storageName).build();
        when(storageService.updateStorage(alternateKey, storageUpdateRequest)).thenReturn(storage);

        Storage resultStorage = storageRestController.updateStorage(storageCreateRequest.getName(), storageUpdateRequest);

        // Verify the external calls.
        verify(storageService).updateStorage(alternateKey, storageUpdateRequest);
        verifyNoMoreInteractions(storageService);

        // Validate the returned object.
        assertEquals(storage, resultStorage);
    }

    @Test
    public void testGetStorage() throws Exception
    {
        // Create and persist a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();
        Storage storage = new Storage();
        String storageName = storageCreateRequest.getName();
        storage.setName(storageName);
        storage.setAttributes(storageCreateRequest.getAttributes());
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().withStorageName(storageName).build();

        when(storageService.getStorage(alternateKey)).thenReturn(storage);

        // Retrieve the storage by it's name which is valid.
        Storage resultStorage = storageRestController.getStorage(storageName);
        // Verify the external calls.
        verify(storageService).getStorage(alternateKey);
        verifyNoMoreInteractions(storageService);

        // Validate the returned object.
        assertEquals(storage, resultStorage);
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testDeleteStorage() throws Exception
    {
        // Create and persist a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();
        Storage storage = new Storage();
        String storageName = storageCreateRequest.getName();
        storage.setName(storageName);
        storage.setAttributes(storageCreateRequest.getAttributes());
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().withStorageName(storageName).build();
        ObjectNotFoundException exception = new ObjectNotFoundException("object not found");

        when(storageService.deleteStorage(alternateKey)).thenThrow(exception);

        storageRestController.deleteStorage(storageName);
        // Verify the external calls.
        verify(storageService).deleteStorage(alternateKey);
        verifyNoMoreInteractions(storageService);
    }

    @Test
    public void testGetStorages() throws Exception
    {
        // Get a list of test storage keys.
        List<StorageKey> testStorageKeys = Arrays.asList(new StorageKey(STORAGE_NAME), new StorageKey(STORAGE_NAME_2));
        StorageKeys storageKeys = new StorageKeys(testStorageKeys);

        when(storageService.getStorages()).thenReturn(storageKeys);
        // Retrieve a list of storage keys.
        StorageKeys resultStorageKeys = storageRestController.getStorages();

        // Verify the external calls.
        verify(storageService).getStorages();
        verifyNoMoreInteractions(storageService);
        // Validate the returned object.
        assertEquals(storageKeys, resultStorageKeys);
    }

    /**
     * Creates (but does not persist) a new valid storage create request.
     *
     * @return a new storage.
     */
    private StorageCreateRequest getNewStorageCreateRequest()
    {
        String name = "StorageTest" + getRandomSuffix();
        StorageCreateRequest storageRequest = new StorageCreateRequest();
        storageRequest.setStoragePlatformName(StoragePlatformEntity.S3);
        storageRequest.setName(name);
        storageRequest.setAttributes(businessObjectDefinitionServiceTestHelper.getNewAttributes());
        return storageRequest;
    }
}
