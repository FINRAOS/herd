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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageCreateRequest;
import org.finra.herd.model.api.xml.StorageKey;
import org.finra.herd.model.api.xml.StorageKeys;
import org.finra.herd.model.api.xml.StorageUpdateRequest;
import org.finra.herd.model.jpa.StoragePlatformEntity;

/**
 * This class tests various functionality within the storage REST controller.
 */
public class StorageRestControllerTest extends AbstractRestTest
{
    @Test
    public void testCreateStorage() throws Exception
    {
        // Create and persist a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();

        Storage storage = storageRestController.createStorage(storageCreateRequest);

        assertNotNull(storage);
        assertTrue(storage.getName().equals(storageCreateRequest.getName()));

        // Check if result list of attributes matches to the list from the create request.
        businessObjectDefinitionServiceTestHelper.validateAttributes(storageCreateRequest.getAttributes(), storage.getAttributes());
    }

    @Test
    public void testUpdateStorage() throws Exception
    {
        // Create a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();
        storageRestController.createStorage(storageCreateRequest);

        // Update the storage platform which is valid.
        StorageUpdateRequest storageUpdateRequest = new StorageUpdateRequest();

        // TODO: Update various attributes of the storage update request in the future when there is something to update.

        storageRestController.updateStorage(storageCreateRequest.getName(), storageUpdateRequest);

        // TODO: Add asserts to ensure fields that were update indeed got updated.
    }

    @Test
    public void testGetStorage() throws Exception
    {
        // Create and persist a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();
        String name = storageCreateRequest.getName();
        Storage storage = storageRestController.createStorage(storageCreateRequest);

        // Retrieve the storage by it's name which is valid.
        storage = storageRestController.getStorage(storage.getName());
        assertNotNull(storage);
        assertTrue(storage.getName().equals(name));
    }

    @Test(expected = ObjectNotFoundException.class)
    public void testDeleteStorage() throws Exception
    {
        // Create and persist a valid storage.
        StorageCreateRequest storageCreateRequest = getNewStorageCreateRequest();
        String name = storageCreateRequest.getName();
        Storage storage = storageRestController.createStorage(storageCreateRequest);

        // Delete the storage by it's name which is valid.
        storage = storageRestController.deleteStorage(storage.getName());
        assertNotNull(storage);
        assertTrue(storage.getName().equals(name));

        // Retrieve the storage by it's name and verify that it doesn't exist.
        storageRestController.getStorage(storage.getName());
    }

    @Test
    public void testGetStorages() throws Exception
    {
        // Get a list of test storage keys.
        List<StorageKey> testStorageKeys = Arrays.asList(new StorageKey(STORAGE_NAME), new StorageKey(STORAGE_NAME_2));

        // Create and persist storage entities.
        for (StorageKey key : testStorageKeys)
        {
            storageDaoTestHelper.createStorageEntity(key.getStorageName());
        }

        // Retrieve a list of storage keys.
        StorageKeys resultStorageKeys = storageRestController.getStorages();

        // Validate the returned object.
        assertNotNull(resultStorageKeys);
        assertNotNull(resultStorageKeys.getStorageKeys());
        assertTrue(resultStorageKeys.getStorageKeys().size() >= testStorageKeys.size());
        for (StorageKey key : testStorageKeys)
        {
            assertTrue(resultStorageKeys.getStorageKeys().contains(key));
        }
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
