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
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitCreateResponse;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.BusinessObjectDataStorageUnitService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller to handle business object data storage unit requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Business Object Data Storage Unit")
public class BusinessObjectDataStorageUnitRestController extends HerdBaseController
{
    @Autowired
    private BusinessObjectDataStorageUnitService businessObjectDataStorageUnitService;

    /**
     * Creates new storage unit for a given business object data and storage. <p>Requires WRITE permission on namespace</p>
     *
     * @param request the create business object data storage unit create request
     *
     * @return the create business object data storage unit create response
     */
    @RequestMapping(value = "/businessObjectDataStorageUnits", method = RequestMethod.POST)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_STORAGES_UNITS_POST)
    public BusinessObjectDataStorageUnitCreateResponse createBusinessObjectDataStorageUnit(@RequestBody BusinessObjectDataStorageUnitCreateRequest request)
    {
        return businessObjectDataStorageUnitService.createBusinessObjectDataStorageUnit(request);
    }
}
