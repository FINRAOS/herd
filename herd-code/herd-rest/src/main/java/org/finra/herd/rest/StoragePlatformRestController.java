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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.StoragePlatform;
import org.finra.herd.model.api.xml.StoragePlatforms;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.StoragePlatformService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles storage platform REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Storage Platform")
public class StoragePlatformRestController extends HerdBaseController
{
    @Autowired
    private StoragePlatformService storagePlatformService;

    /**
     * Gets an existing storage platform by name.
     *
     * @param name the storage platform name.
     *
     * @return the storage platform information.
     */
    @RequestMapping(value = "/storagePlatforms/{name}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_STORAGE_PLATFORMS_GET)
    public StoragePlatform getStoragePlatform(@PathVariable("name") String name)
    {
        return storagePlatformService.getStoragePlatform(name);
    }

    /**
     * Gets a list of existing storage platforms.
     *
     * @return the storage platforms.
     */
    @RequestMapping(value = "/storagePlatforms", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_STORAGE_PLATFORMS_ALL_GET)
    public StoragePlatforms getStoragePlatforms()
    {
        return storagePlatformService.getStoragePlatforms();
    }
}
