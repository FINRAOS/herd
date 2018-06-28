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

import org.finra.herd.model.api.xml.Storage;
import org.finra.herd.model.api.xml.StorageAttributesUpdateRequest;
import org.finra.herd.model.api.xml.StorageCreateRequest;
import org.finra.herd.model.api.xml.StorageKey;
import org.finra.herd.model.api.xml.StorageKeys;
import org.finra.herd.model.api.xml.StorageUpdateRequest;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.StorageService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles storage REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Storage")
class StorageRestController extends HerdBaseController
{
    private static final String STORAGE_URI_PREFIX = "/storages";

    @Autowired
    private StorageService storageService;

    /**
     * Creates a new storage.
     *
     * @param storageCreateRequest the information needed to create the storage
     *
     * @return the created storage information
     */
    @RequestMapping(value = STORAGE_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_STORAGES_POST)
    Storage createStorage(@RequestBody StorageCreateRequest storageCreateRequest)
    {
        return storageService.createStorage(storageCreateRequest);
    }

    /**
     * Deletes an existing storage by name.
     *
     * @param storageName the storage name
     *
     * @return the storage information of the storage that got deleted
     */
    @RequestMapping(value = STORAGE_URI_PREFIX + "/{storageName}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_STORAGES_DELETE)
    Storage deleteStorage(@PathVariable("storageName") String storageName)
    {
        return storageService.deleteStorage(new StorageKey(storageName));
    }

    /**
     * Gets an existing storage by name.
     *
     * @param storageName the storage name
     *
     * @return the storage information
     */
    @RequestMapping(value = STORAGE_URI_PREFIX + "/{storageName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_STORAGES_GET)
    Storage getStorage(@PathVariable("storageName") String storageName)
    {
        return storageService.getStorage(new StorageKey(storageName));
    }

    /**
     * Gets a list of storage keys for all storage defined in the system.
     *
     * @return the list of storage keys
     */
    @RequestMapping(value = STORAGE_URI_PREFIX, method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_STORAGES_ALL_GET)
    StorageKeys getStorages()
    {
        return storageService.getAllStorage();
    }

    /**
     * Updates an existing storage.
     *
     * @param storageName the name of the storage to update
     * @param storageUpdateRequest the information needed to update the storage
     *
     * @return the updated storage information
     */
    @RequestMapping(value = STORAGE_URI_PREFIX + "/{storageName}", method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_STORAGES_PUT)
    Storage updateStorage(@PathVariable("storageName") String storageName, @RequestBody StorageUpdateRequest storageUpdateRequest)
    {
        return storageService.updateStorage(new StorageKey(storageName), storageUpdateRequest);
    }

    /**
     * Updates an existing storage attributes by storage name.
     * <p>
     * This endpoint replaces the entire list of attributes on the Storage with the contents of the request. Observe this example:
     *   <ol>
     *       <li>Three attributes present on the Storage.</li>
     *       <li>This endpoint is called with a single attribute in the request with an updated value.</li>
     *       <li>After this operation the Storage will have only one attribute – which is probably not the desired outcome.</li>
     *       <li>Instead, supply all existing attributes and provide updated values and additional attributes as needed.
     *       The only case when an existing attribute should be left out is to remove the attribute.</li>
     *   </ol>
     * </p>
     *
     * @param storageName the name of the storage
     * @param storageAttributesUpdateRequest the information needed to update storage attributes
     *
     * @return the updated storage information
     */
    @RequestMapping(value = "/storageAttributes/storages/{storageName}", method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_STORAGE_ATTRIBUTES_PUT)
    Storage updateStorageAttributes(@PathVariable("storageName") String storageName, @RequestBody StorageAttributesUpdateRequest storageAttributesUpdateRequest)
    {
        return storageService.updateStorageAttributes(new StorageKey(storageName), storageAttributesUpdateRequest);
    }
}
