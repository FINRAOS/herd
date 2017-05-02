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

import org.finra.herd.model.api.xml.AllowedAttributeValuesCreateRequest;
import org.finra.herd.model.api.xml.AllowedAttributeValuesDeleteRequest;
import org.finra.herd.model.api.xml.AllowedAttributeValuesInformation;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.AllowedAttributeValueService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles allowed attribute value REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Allowed Attribute Value")
public class AllowedAttributeValueRestController extends HerdBaseController
{
    public static final String ALLOWED_ATTRIBUTE_VALUES_URI_PREFIX = "/allowedAttributeValues";

    @Autowired
    private AllowedAttributeValueService allowedAttributeValueService;

    /**
     * Creates a list of allowed attribute values for an existing attribute value list.
     *
     * @param request the information needed to create the allowed attribute values
     *
     * @return the newly created allowed attribute values
     */
    @RequestMapping(value = ALLOWED_ATTRIBUTE_VALUES_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_ALLOWED_ATTRIBUTE_VALUES_POST)
    public AllowedAttributeValuesInformation createAllowedAttributeValues(@RequestBody AllowedAttributeValuesCreateRequest request)
    {
        return allowedAttributeValueService.createAllowedAttributeValues(request);
    }

    /**
     * Retrieves a range of existing allowed attribute values.
     *
     * @param namespace the namespace
     * @param attributeValueListName the attribute value list name
     *
     * @return the allowed attribute values
     */
    @RequestMapping(value = ALLOWED_ATTRIBUTE_VALUES_URI_PREFIX +
        "/namespaces/{namespace}/attributeValueListNames/{attributeValueListName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_ALLOWED_ATTRIBUTE_VALUES_ALL_GET)
    public AllowedAttributeValuesInformation getAllowedAttributeValues(@PathVariable("namespace") String namespace,
        @PathVariable("attributeValueListName") String attributeValueListName)
    {
        return allowedAttributeValueService.getAllowedAttributeValues(new AttributeValueListKey(namespace, attributeValueListName));
    }

    /**
     * Deletes specified allowed attribute values from an existing attribute value list.
     *
     * @param request the allowed attribute values delete request
     *
     * @return the deleted allowed attribute values
     */
    @RequestMapping(value = ALLOWED_ATTRIBUTE_VALUES_URI_PREFIX, method = RequestMethod.DELETE, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_ALLOWED_ATTRIBUTE_VALUES_DELETE)
    public AllowedAttributeValuesInformation deleteAllowedAttributeValues(@RequestBody AllowedAttributeValuesDeleteRequest request)
    {
        return allowedAttributeValueService.deleteAllowedAttributeValues(request);
    }
}
