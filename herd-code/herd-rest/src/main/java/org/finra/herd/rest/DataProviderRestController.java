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

import org.finra.herd.model.api.xml.DataProvider;
import org.finra.herd.model.api.xml.DataProviderCreateRequest;
import org.finra.herd.model.api.xml.DataProviderKey;
import org.finra.herd.model.api.xml.DataProviderKeys;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.DataProviderService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles data provider REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "DataProvider")
public class DataProviderRestController
{
    @Autowired
    private DataProviderService dataProviderService;

    /**
     * Creates a new data provider.
     *
     * @param request the information needed to create the data provider
     *
     * @return the created data provider
     */
    @RequestMapping(value = "/dataProviders", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_DATA_PROVIDERS_POST)
    public DataProvider createDataProvider(@RequestBody DataProviderCreateRequest request)
    {
        return dataProviderService.createDataProvider(request);
    }

    /**
     * Gets an existing data provider by data provider name.
     *
     * @param dataProviderName the data provider name
     *
     * @return the retrieved data provider
     */
    @RequestMapping(value = "/dataProviders/{dataProviderName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_DATA_PROVIDERS_GET)
    public DataProvider getDataProvider(@PathVariable("dataProviderName") String dataProviderName)
    {
        return dataProviderService.getDataProvider(new DataProviderKey(dataProviderName));
    }

    /**
     * Deletes an existing data provider by data provider name.
     *
     * @param dataProviderName the data provider name
     *
     * @return the data provider that got deleted
     */
    @RequestMapping(value = "/dataProviders/{dataProviderName}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_DATA_PROVIDERS_DELETE)
    public DataProvider deleteDataProvider(@PathVariable("dataProviderName") String dataProviderName)
    {
        return dataProviderService.deleteDataProvider(new DataProviderKey(dataProviderName));
    }

    /**
     * Gets a list of data provider keys for all data providers defined in the system.
     *
     * @return the list of data provider keys
     */
    @RequestMapping(value = "/dataProviders", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_DATA_PROVIDERS_ALL_GET)
    public DataProviderKeys getDataProviders()
    {
        return dataProviderService.getDataProviders();
    }
}
