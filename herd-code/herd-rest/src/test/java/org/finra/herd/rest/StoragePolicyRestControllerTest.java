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

import org.finra.herd.model.api.xml.StoragePolicy;
import org.finra.herd.model.api.xml.StoragePolicyCreateRequest;
import org.finra.herd.model.api.xml.StoragePolicyFilter;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.api.xml.StoragePolicyKeys;
import org.finra.herd.model.api.xml.StoragePolicyRule;
import org.finra.herd.model.api.xml.StoragePolicyTransition;
import org.finra.herd.model.api.xml.StoragePolicyUpdateRequest;
import org.finra.herd.model.jpa.StoragePolicyStatusEntity;
import org.finra.herd.service.StoragePolicyService;

/**
 * This class tests various functionality within the business object data notification registration REST controller.
 */
public class StoragePolicyRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private StoragePolicyRestController storagePolicyRestController;

    @Mock
    private StoragePolicyService storagePolicyService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateStoragePolicy()
    {
        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        StoragePolicy storagePolicy = new StoragePolicy(ID, storagePolicyKey, new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
            new StoragePolicyFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, DO_NOT_TRANSITION_LATEST_VALID),
            new StoragePolicyTransition(STORAGE_POLICY_TRANSITION_TYPE), StoragePolicyStatusEntity.ENABLED);

        StoragePolicyCreateRequest request = storagePolicyServiceTestHelper
            .createStoragePolicyCreateRequest(storagePolicyKey, STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME,
                FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, DO_NOT_TRANSITION_LATEST_VALID, STORAGE_POLICY_TRANSITION_TYPE,
                StoragePolicyStatusEntity.ENABLED);

        when(storagePolicyService.createStoragePolicy(request)).thenReturn(storagePolicy);

        // Create a storage policy.
        StoragePolicy resultStoragePolicy = storagePolicyRestController.createStoragePolicy(request);
        // Verify the external calls.
        verify(storagePolicyService).createStoragePolicy(request);
        verifyNoMoreInteractions(storagePolicyService);
        // Validate the returned object.
        assertEquals(storagePolicy, resultStoragePolicy);
    }

    @Test
    public void testDeleteStoragePolicy()
    {
        // Create the objects needed for the test.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        StoragePolicy storagePolicy = new StoragePolicy(ID, storagePolicyKey, new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
            new StoragePolicyFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, DO_NOT_TRANSITION_LATEST_VALID),
            new StoragePolicyTransition(STORAGE_POLICY_TRANSITION_TYPE), StoragePolicyStatusEntity.ENABLED);

        // Setup the mock calls.
        when(storagePolicyService.deleteStoragePolicy(storagePolicyKey)).thenReturn(storagePolicy);

        // Call the method being tested.
        StoragePolicy resultStoragePolicy =
            storagePolicyRestController.deleteStoragePolicy(storagePolicyKey.getNamespace(), storagePolicyKey.getStoragePolicyName());

        // Verify the external calls.
        verify(storagePolicyService).deleteStoragePolicy(storagePolicyKey);
        verifyNoMoreInteractions(storagePolicyService);

        // Validate the returned object.
        assertEquals(storagePolicy, resultStoragePolicy);
    }

    @Test
    public void testGetStoragePolicy()
    {
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        StoragePolicy storagePolicy = new StoragePolicy(ID, storagePolicyKey, new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
            new StoragePolicyFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, DO_NOT_TRANSITION_LATEST_VALID),
            new StoragePolicyTransition(STORAGE_POLICY_TRANSITION_TYPE), StoragePolicyStatusEntity.ENABLED);

        when(storagePolicyService.getStoragePolicy(storagePolicyKey)).thenReturn(storagePolicy);

        StoragePolicy resultStoragePolicy =
            storagePolicyRestController.getStoragePolicy(storagePolicyKey.getNamespace(), storagePolicyKey.getStoragePolicyName());

        // Verify the external calls.
        verify(storagePolicyService).getStoragePolicy(storagePolicyKey);
        verifyNoMoreInteractions(storagePolicyService);
        // Validate the returned object.
        assertEquals(storagePolicy, resultStoragePolicy);
    }

    @Test
    public void testUpdateStoragePolicy()
    {
        // Create a storage policy key.
        StoragePolicyKey storagePolicyKey = new StoragePolicyKey(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME);

        StoragePolicy storagePolicy = new StoragePolicy(ID, storagePolicyKey, new StoragePolicyRule(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE),
            new StoragePolicyFilter(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, STORAGE_NAME, DO_NOT_TRANSITION_LATEST_VALID),
            new StoragePolicyTransition(STORAGE_POLICY_TRANSITION_TYPE), StoragePolicyStatusEntity.ENABLED);

        StoragePolicyUpdateRequest request = storagePolicyServiceTestHelper
            .createStoragePolicyUpdateRequest(STORAGE_POLICY_RULE_TYPE, STORAGE_POLICY_RULE_VALUE, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE,
                FORMAT_FILE_TYPE_CODE, STORAGE_NAME, DO_NOT_TRANSITION_LATEST_VALID, STORAGE_POLICY_TRANSITION_TYPE, StoragePolicyStatusEntity.DISABLED);

        when(storagePolicyService.updateStoragePolicy(storagePolicyKey, request)).thenReturn(storagePolicy);

        // Update a storage policy.
        StoragePolicy resultStoragePolicy = storagePolicyRestController.updateStoragePolicy(STORAGE_POLICY_NAMESPACE_CD, STORAGE_POLICY_NAME, request);

        // Verify the external calls.
        verify(storagePolicyService).updateStoragePolicy(storagePolicyKey, request);
        verifyNoMoreInteractions(storagePolicyService);
        // Validate the returned object.
        assertEquals(storagePolicy, resultStoragePolicy);
    }

    @Test
    public void testGetStoragePolicyKeys()
    {
        // Create an storage policy keys.
        StoragePolicyKeys storagePolicyKeys =
            new StoragePolicyKeys(Arrays.asList(new StoragePolicyKey(NAMESPACE, STORAGE_POLICY_NAME)));

        // Mock the external calls.
        when(storagePolicyService.getStoragePolicyKeys(NAMESPACE)).thenReturn(storagePolicyKeys);

        // Call the method under test.
        StoragePolicyKeys result = storagePolicyRestController.getStoragePolicyKeys(NAMESPACE);

        // Verify the external calls.
        verify(storagePolicyService).getStoragePolicyKeys(NAMESPACE);
        verifyNoMoreInteractions(storagePolicyService);

        // Validate the results.
        assertEquals(storagePolicyKeys, result);
    }
}
