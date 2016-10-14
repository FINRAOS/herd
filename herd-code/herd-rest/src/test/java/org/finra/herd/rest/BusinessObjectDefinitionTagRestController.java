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
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTag;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionTagKeys;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.BusinessObjectDefinitionTagService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles business object definition tag requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Business Object Definition Tag")
public class BusinessObjectDefinitionTagRestController extends HerdBaseController
{
    public static final String BUSINESS_OBJECT_DEFINITION_TAGS_URI_PREFIX = "/businessObjectDefinitionTags";

    @Autowired
    private BusinessObjectDefinitionTagService businessObjectDefinitionTagService;

    /**
     * Creates a new business object definition tag.
     *
     * @param request the information needed to create a business object definition tag
     *
     * @return the newly created business object definition tag
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_TAGS_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_TAGS_POST)
    public BusinessObjectDefinitionTag createBusinessObjectDefinitionTag(@RequestBody BusinessObjectDefinitionTagCreateRequest request)
    {
        return businessObjectDefinitionTagService.createBusinessObjectDefinitionTag(request);
    }

    /**
     * Deletes an existing business object definition tag.
     *
     * @param namespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     * @param tagTypeCode the tag type reference code of the tag
     * @param tagCode the tag reference code of the tag
     *
     * @return the business object definition tag that got deleted
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_TAGS_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/tagTypes/{tagTypeCode}" +
        "/tagCodes/{tagCode}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_TAGS_DELETE)
    public BusinessObjectDefinitionTag deleteBusinessObjectDefinitionTag(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName, @PathVariable("tagTypeCode") String tagTypeCode,
        @PathVariable("tagCode") String tagCode)
    {
        return businessObjectDefinitionTagService.deleteBusinessObjectDefinitionTag(
            new BusinessObjectDefinitionTagKey(new BusinessObjectDefinitionKey(namespace, businessObjectDefinitionName), new TagKey(tagTypeCode, tagCode)));
    }

    /**
     * Gets a list of keys for all existing business object definition tags that are associated with the specified business object definition.
     *
     * @param namespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     *
     * @return the list of business object definition tag keys
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_TAGS_URI_PREFIX + "/namespaces/{namespace}" +
        "/businessObjectDefinitionNames/{businessObjectDefinitionName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_TAGS_BY_BUSINESS_OBJECT_DEFINITION_GET)
    public BusinessObjectDefinitionTagKeys getBusinessObjectDefinitionTagsByBusinessObjectDefinition(@PathVariable(value = "namespace") String namespace,
        @PathVariable(value = "businessObjectDefinitionName") String businessObjectDefinitionName)
    {
        return businessObjectDefinitionTagService
            .getBusinessObjectDefinitionTagsByBusinessObjectDefinition(new BusinessObjectDefinitionKey(namespace, businessObjectDefinitionName));
    }

    /**
     * Gets a list of keys for all existing business object definition tags that are associated with the specified tag.
     *
     * @param tagTypeCode the tag type reference code
     * @param tagCode the tag reference code
     *
     * @return the list of business object definition tag keys
     */
    @RequestMapping(value = BUSINESS_OBJECT_DEFINITION_TAGS_URI_PREFIX + "/tagTypes/{tagTypeCode}" +
        "/tagCodes/{tagCode}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DEFINITION_TAGS_BY_TAG_GET)
    public BusinessObjectDefinitionTagKeys getBusinessObjectDefinitionTagsByTag(@PathVariable(value = "tagTypeCode") String tagTypeCode,
        @PathVariable(value = "tagCode") String tagCode)
    {
        return businessObjectDefinitionTagService.getBusinessObjectDefinitionTagsByTag(new TagKey(tagTypeCode, tagCode));
    }
}
