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

import org.finra.herd.model.api.xml.AttributeValueList;
import org.finra.herd.model.api.xml.AttributeValueListCreateRequest;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.api.xml.AttributeValueListKeys;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.AttributeValueListService;
import org.finra.herd.ui.constants.UiConstants;

@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Attribute Value List")
public class AttributeValueListRestController extends HerdBaseController
{
    public static final String ATTRIBUTE_VALUE_LIST_URI_PREFIX = "/attributeValueLists";

    @Autowired
    private AttributeValueListService attributeValueListService;

    /**
     * Creates a new attribute value list. <p>Requires WRITE permission on namespace</p>
     *
     * @param request the information needed to create an attribute value list
     *
     * @return the attribute value list information
     */
    @RequestMapping(value = ATTRIBUTE_VALUE_LIST_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_ATTRIBUTE_VALUE_LISTS_POST)
    public AttributeValueList createAttributeValueList(@RequestBody AttributeValueListCreateRequest request)
    {
        return attributeValueListService.createAttributeValueList(request);
    }

    /**
     * Deletes an existing attribute value list. <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace of the attribute value list
     * @param attributeValueListName the name of the attribute value list
     *
     * @return the attribute value list information
     */
    @RequestMapping(value = ATTRIBUTE_VALUE_LIST_URI_PREFIX + "/namespaces/{namespace}/attributeValueListNames/{attributeValueListName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_ATTRIBUTE_VALUE_LISTS_DELETE)
    public AttributeValueList deleteAttributeValueList(@PathVariable("namespace") String namespace,
        @PathVariable("attributeValueListName") String attributeValueListName)
    {
        return attributeValueListService.deleteAttributeValueList(new AttributeValueListKey(namespace, attributeValueListName));
    }

    /**
     * Gets an existing attribute value list. <p>Requires READ permission on namespace</p>
     *
     * @param namespace the namespace of the attribute value list
     * @param attributeValueListName the name of the attribute value list
     *
     * @return the attribute value list information
     */
    @RequestMapping(value = ATTRIBUTE_VALUE_LIST_URI_PREFIX + "/namespaces/{namespace}/attributeValueListNames/{attributeValueListName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_ATTRIBUTE_VALUE_LISTS_GET)
    public AttributeValueList getAttributeValueList(@PathVariable("namespace") String namespace,
        @PathVariable("attributeValueListName") String attributeValueListName)
    {
        return attributeValueListService.getAttributeValueList(new AttributeValueListKey(namespace, attributeValueListName));
    }

    /**
     * Gets a list of keys for all attribute value lists registered in the system that user has access to. <p>Requires READ permission on namespace</p>
     *
     * @return the list of attribute value list keys
     */
    @RequestMapping(value = ATTRIBUTE_VALUE_LIST_URI_PREFIX, method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_ATTRIBUTE_VALUE_LISTS_ALL_GET)
    public AttributeValueListKeys getAttributeValueLists()
    {
        return attributeValueListService.getAttributeValueLists();
    }
}
