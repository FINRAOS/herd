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

import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.StoragePolicy;
import org.finra.herd.model.api.xml.StoragePolicyCreateRequest;
import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.api.xml.StoragePolicyKeys;
import org.finra.herd.model.api.xml.StoragePolicyUpdateRequest;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.StoragePolicyService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles storage policy REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Storage Policy")
public class StoragePolicyRestController extends HerdBaseController
{
    public static final String STORAGE_POLICIES_URI_PREFIX = "/storagePolicies";

    @Autowired
    private StoragePolicyService storagePolicyService;

    /**
     * Creates a new storage policy.
     * <p>Requires WRITE permission on storage policy namespace and storage policy filter namespace</p>
     *
     * @param request the information needed to create the storage policy
     *
     * @return the created storage policy
     */
    @RequestMapping(value = STORAGE_POLICIES_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_STORAGE_POLICIES_POST)
    public StoragePolicy createStoragePolicy(@RequestBody StoragePolicyCreateRequest request)
    {
        return storagePolicyService.createStoragePolicy(request);
    }

    /**
     * Updates an existing storage policy by key.
     * <p>Requires WRITE permission on storage policy namespace and storage policy filter namespace</p>
     *
     * @param namespace the namespace
     * @param storagePolicyName the storage policy name
     * @param request the information needed to update the storage policy
     *
     * @return the updated storage policy
     */
    @RequestMapping(value = STORAGE_POLICIES_URI_PREFIX + "/namespaces/{namespace}/storagePolicyNames/{storagePolicyName}",
        method = RequestMethod.PUT,
        consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_STORAGE_POLICIES_PUT)
    public StoragePolicy updateStoragePolicy(@PathVariable("namespace") String namespace, @PathVariable("storagePolicyName") String storagePolicyName,
        @RequestBody StoragePolicyUpdateRequest request)
    {
        return storagePolicyService.updateStoragePolicy(new StoragePolicyKey(namespace, storagePolicyName), request);
    }

    /**
     * Gets an existing storage policy by key.
     * <p>Requires READ permission on namespace</p>
     *
     * @param namespace the namespace
     * @param storagePolicyName the storage policy name
     *
     * @return the retrieved storage policy
     */
    @RequestMapping(value = STORAGE_POLICIES_URI_PREFIX + "/namespaces/{namespace}/storagePolicyNames/{storagePolicyName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_STORAGE_POLICIES_GET)
    public StoragePolicy getStoragePolicy(@PathVariable("namespace") String namespace, @PathVariable("storagePolicyName") String storagePolicyName)
    {
        return storagePolicyService.getStoragePolicy(new StoragePolicyKey(namespace, storagePolicyName));
    }

    /**
     * Gets a list of keys for all storage policies defined in the system for the specified namespace. <p>Requires READ permission on namespace</p>
     *
     * @param namespace the namespace
     * @return the storage policy keys
     */
    @RequestMapping(value = STORAGE_POLICIES_URI_PREFIX + "/namespaces/{namespace}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_STORAGE_POLICIES_ALL_GET)
    public StoragePolicyKeys getStoragePolicyKeys(@PathVariable("namespace") String namespace)
    {
        return storagePolicyService.getStoragePolicyKeys(namespace);
    }

    /**
     * Deletes an existing storage policy by key.
     * <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace
     * @param storagePolicyName the storage policy name
     *
     * @return the deleted storage policy
     */
    @RequestMapping(value = STORAGE_POLICIES_URI_PREFIX + "/namespaces/{namespace}/storagePolicyNames/{storagePolicyName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_STORAGE_POLICIES_DELETE)
    public StoragePolicy deleteStoragePolicy(@PathVariable("namespace") String namespace, @PathVariable("storagePolicyName") String storagePolicyName)
    {
        return storagePolicyService.deleteStoragePolicy(new StoragePolicyKey(namespace, storagePolicyName));
    }
}
