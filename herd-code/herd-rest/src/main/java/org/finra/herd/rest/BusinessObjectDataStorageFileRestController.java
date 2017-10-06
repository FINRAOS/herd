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

import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageFilesCreateResponse;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.BusinessObjectDataStorageFileService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller to handle business object data storage file requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Business Object Data Storage File")
public class BusinessObjectDataStorageFileRestController extends HerdBaseController
{
    @Autowired
    private BusinessObjectDataStorageFileService businessObjectDataStorageFileService;

    /**
     * Creates new file(s) for a given business object data and storage. <p>Addition of storage files is only allowed for business object data with a
     * pre-registration status.</p> <p>Requires WRITE permission on namespace</p>
     *
     * @param businessObjectDataStorageFilesCreateRequest - the create file request
     *
     * @return response
     */
    @RequestMapping(value = "/businessObjectDataStorageFiles", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_STORAGES_FILES_POST)
    public BusinessObjectDataStorageFilesCreateResponse createBusinessObjectDataStorageFiles(
        @RequestBody BusinessObjectDataStorageFilesCreateRequest businessObjectDataStorageFilesCreateRequest)
    {
        return businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(businessObjectDataStorageFilesCreateRequest);
    }
}
