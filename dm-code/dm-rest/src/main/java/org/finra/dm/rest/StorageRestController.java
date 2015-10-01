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
package org.finra.dm.rest;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.finra.dm.model.dto.SecurityFunctions;
import org.finra.dm.model.dto.StorageAlternateKeyDto;
import org.finra.dm.model.api.xml.Storage;
import org.finra.dm.model.api.xml.StorageBusinessObjectDefinitionDailyUploadStats;
import org.finra.dm.model.api.xml.StorageCreateRequest;
import org.finra.dm.model.api.xml.StorageDailyUploadStats;
import org.finra.dm.model.api.xml.StorageKeys;
import org.finra.dm.model.api.xml.StorageUpdateRequest;
import org.finra.dm.service.StorageService;
import org.finra.dm.service.helper.DmHelper;
import org.finra.dm.ui.constants.UiConstants;

/**
 * The REST controller that handles storage REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
public class StorageRestController extends DmBaseController
{
    public static final String STORAGES_URI_PREFIX = "/storages";

    @Autowired
    private StorageService storageService;

    @Autowired
    private DmHelper dmHelper;

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
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(storageName).build();
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
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(storageName).build();
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
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(storageName).build();
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

    /**
     * Gets cumulative daily upload statistics for the storage for the specified upload date.  If the upload date is not specified, returns the upload stats for
     * the past 7 calendar days plus today (8 days total).
     *
     * @param storageName the storage name
     * @param uploadDateString the upload date in YYYY-MM-DD format (optional)
     *
     * @return the upload statistics
     */
    @RequestMapping(value = STORAGES_URI_PREFIX + "/{storageName}/storageDailyUploadStats", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_STORAGES_UPLOAD_STATS_GET)
    public StorageDailyUploadStats getStorageUploadStats(@PathVariable("storageName") String storageName,
        @RequestParam(value = "uploadDate", required = false) String uploadDateString)
    {
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(storageName).build();
        Date uploadDate = dmHelper.getDateFromString(uploadDateString);
        return storageService.getStorageUploadStats(alternateKey, uploadDate);
    }

    /**
     * Retrieves daily upload statistics for the storage by business object definition for the specified upload date.  If the upload date is not specified,
     * returns the upload stats for the past 7 calendar days plus today (8 days total).
     *
     * @param storageName the storage name
     * @param uploadDateString the upload date in YYYY-MM-DD format (optional)
     *
     * @return the upload statistics
     */
    @RequestMapping(value = STORAGES_URI_PREFIX + "/{storageName}/storageDailyUploadStatsByBusinessObjectDefinition", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_STORAGES_UPLOAD_STATS_GET)
    public StorageBusinessObjectDefinitionDailyUploadStats getStorageUploadStatsByBusinessObjectDefinition(@PathVariable("storageName") String storageName,
        @RequestParam(value = "uploadDate", required = false) String uploadDateString)
    {
        StorageAlternateKeyDto alternateKey = StorageAlternateKeyDto.builder().storageName(storageName).build();
        Date uploadDate = dmHelper.getDateFromString(uploadDateString);
        return storageService.getStorageUploadStatsByBusinessObjectDefinition(alternateKey, uploadDate);
    }
}
