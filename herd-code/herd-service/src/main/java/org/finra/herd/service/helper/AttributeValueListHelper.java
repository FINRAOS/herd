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
package org.finra.herd.service.helper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.AttributeValueListCreateRequest;
import org.finra.herd.model.api.xml.AttributeValueListKey;

@Component
public class AttributeValueListHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    /**
     * Validates attribute value list create request.
     *
     * @param attributeValueListCreateRequest the attribute value list request
     */
    public void validateAttributeValueListCreateRequest(AttributeValueListCreateRequest attributeValueListCreateRequest)
    {
        Assert.notNull(attributeValueListCreateRequest, "A Attribute value list create request must be specified.");
        validateAttributeValueListKey(attributeValueListCreateRequest.getAttributeValueListKey());
    }

    /**
     * Validates attribute value list key.
     *
     * @param attributeValueListKey the attribute value list key
     */
    public void validateAttributeValueListKey(AttributeValueListKey attributeValueListKey)
    {
        // Validate.
        Assert.notNull(attributeValueListKey, "A attribute value list key must be specified.");
        attributeValueListKey
            .setNamespace(alternateKeyHelper.validateStringParameter("attribute value list namespace code", attributeValueListKey.getNamespace()));
        attributeValueListKey.setAttributeValueListName(
            alternateKeyHelper.validateStringParameter("attribute value list name", attributeValueListKey.getAttributeValueListName()));
    }
}
