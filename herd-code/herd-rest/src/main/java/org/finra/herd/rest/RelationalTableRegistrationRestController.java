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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.RelationalTableRegistrationCreateRequest;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.RelationalTableRegistrationService;
import org.finra.herd.ui.constants.UiConstants;

@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Relational Table Registration")
public class RelationalTableRegistrationRestController extends HerdBaseController
{
    @Autowired
    private RelationalTableRegistrationService relationalTableRegistrationService;

    /**
     * Creates relational table registration
     *
     * @param relationalTableRegistrationCreateRequest relational table registration create request
     * @param appendToExistingBusinessObjectDefinition boolean flag that determines if the format should be appended to an existing business object definition
     *
     * @return business object data
     */
    @RequestMapping(value = "/relationalTableRegistrations", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_RELATIONAL_TABLE_REGISTRATIONS_POST)
    public BusinessObjectData createRelationalTableRegistration(@RequestBody RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest,
        @RequestParam(value = "appendToExistingBusinessObjectDefinition", required = false, defaultValue = "false")
            Boolean appendToExistingBusinessObjectDefinition)
    {
        return relationalTableRegistrationService
            .createRelationalTableRegistration(relationalTableRegistrationCreateRequest, appendToExistingBusinessObjectDefinition);
    }
}
