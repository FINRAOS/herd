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
import org.finra.herd.model.api.xml.StorageCreateRequest;
import org.finra.herd.model.api.xml.StorageKeys;
import org.finra.herd.model.api.xml.StorageUpdateRequest;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.model.dto.StorageAlternateKeyDto;
import org.finra.herd.service.StorageService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles storage REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Storage")
public class StorageRestController extends HerdBaseController
{
    public static final String STORAGES_URI_PREFIX = "/storages";

    @Autowired
    private StorageService storageService;

    /**
     * Creates a new storage.
     *
     * @param request the information needed to create the storage
     *
     * @return the created storage information
     */
    @RequestMapping(value = STORAGES_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_STORAGES_POST)
    public Storage createStorage(@RequestBody StorageCreateRequest request)
    {
        return storageService.createStorage(request);
    }

    /**
     * Updates an existing storage.
     *
     * @param storageName the name of the storage to update
     * @param request the information needed to update the storage
     *
     * @return the updated storage information
     */
    @RequestMapping(value = STORAGES_URI_PREFIX + "/{storageName}", method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_STORAGES_PUT)
    public Storage updateStorage(@PathVariable("storageName") String storageName, @RequestBody StorageUpdateRequest request)
    {
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().withStorageName(storageName).build();
        return storageService.updateStorage(alternateKey, request);
    }

    /**
     * Gets an existing storage by name.
     *
     * @param storageName the storage name
     *
     * @return the storage information
     */
    @RequestMapping(value = STORAGES_URI_PREFIX + "/{storageName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_STORAGES_GET)
    public Storage getStorage(@PathVariable("storageName") String storageName)
    {
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().withStorageName(storageName).build();
        return storageService.getStorage(alternateKey);
    }

    /**
     * Deletes an existing storage by name.
     *
     * @param storageName the storage name
     *
     * @return the storage information of the storage that got deleted
     */
    @RequestMapping(value = STORAGES_URI_PREFIX + "/{storageName}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_STORAGES_DELETE)
    public Storage deleteStorage(@PathVariable("storageName") String storageName)
    {
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().withStorageName(storageName).build();
        return storageService.deleteStorage(alternateKey);
    }

    /**
     * Gets a list of namespace keys for all namespaces defined in the system.
     *
     * @return the list of namespace keys
     */
    @RequestMapping(value = STORAGES_URI_PREFIX, method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_STORAGES_ALL_GET)
    public StorageKeys getStorages()
    {
        return storageService.getStorages();
    }
}
