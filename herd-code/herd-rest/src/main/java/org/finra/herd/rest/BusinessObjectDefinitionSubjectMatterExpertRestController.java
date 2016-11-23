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

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpert;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertKeys;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.BusinessObjectDefinitionSubjectMatterExpertService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles business object definition subject matter expert requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Business Object Definition Subject Matter Expert")
public class BusinessObjectDefinitionSubjectMatterExpertRestController extends HerdBaseController
{
    public static final String BUSINESS_OBJECT_DEFINITION_SUBJECT_MATTER_EXPERTS_URI_PREFIX = "/businessObjectDefinitionSubjectMatterExperts";

    @Autowired
    private BusinessObjectDefinitionSubjectMatterExpertService businessObjectDefinitionSubjectMatterExpertService;

    /**
     * Creates a new business object definition subject matter expert.
     *
     * @param request the information needed to create a business object definition subject matter expert
     *
     * @return the newly created business object definition subject matter expert
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_SUBJECT_MATTER_EXPERTS_URI_PREFIX, method = RequestMethod.POST,
        consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_SUBJECT_MATTER_EXPERTS_POST)
    public BusinessObjectDefinitionSubjectMatterExpert createBusinessObjectDefinitionSubjectMatterExpert(
        @RequestBody BusinessObjectDefinitionSubjectMatterExpertCreateRequest request)
    {
        return businessObjectDefinitionSubjectMatterExpertService.createBusinessObjectDefinitionSubjectMatterExpert(request);
    }

    /**
     * Deletes an existing business object definition subject matter expert.
     *
     * @param namespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     * @param userId the user id of the subject matter expert
     *
     * @return the business object definition subject matter expert that got deleted
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_SUBJECT_MATTER_EXPERTS_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/userIds/{userId}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_SUBJECT_MATTER_EXPERTS_DELETE)
    public BusinessObjectDefinitionSubjectMatterExpert deleteBusinessObjectDefinitionSubjectMatterExpert(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName, @PathVariable("userId") String userId)
    {
        return businessObjectDefinitionSubjectMatterExpertService.deleteBusinessObjectDefinitionSubjectMatterExpert(
            new BusinessObjectDefinitionSubjectMatterExpertKey(namespace, businessObjectDefinitionName, userId));
    }

    /**
     * Gets a list of keys for all existing business object definition subject matter experts for the specified business object definition.
     *
     * @param namespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     *
     * @return the list of business object definition subject matter expert keys
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_SUBJECT_MATTER_EXPERTS_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_SUBJECT_MATTER_EXPERTS_BY_BUSINESS_OBJECT_DEFINITION_GET)
    public BusinessObjectDefinitionSubjectMatterExpertKeys getBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition(
        @PathVariable("namespace") String namespace, @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName)
    {
        return businessObjectDefinitionSubjectMatterExpertService.getBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition(
            new BusinessObjectDefinitionKey(namespace, businessObjectDefinitionName));
    }
}
