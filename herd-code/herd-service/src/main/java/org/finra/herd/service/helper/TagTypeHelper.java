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

import org.finra.herd.model.api.xml.TagTypeKey;

/**
 * A helper class for Tag type related code.
 */
@Component
public class TagTypeHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    /**
     * Validates a tag type key. This method also trims the key parameters.
     *
     * @param tagTypeKey the tag type key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateTagTypeKey(TagTypeKey tagTypeKey) throws IllegalArgumentException
    {
        // Validate.
        Assert.notNull(tagTypeKey, "A tag type key must be specified.");
        tagTypeKey.setTagTypeCode(alternateKeyHelper.validateStringParameter("tag type code", tagTypeKey.getTagTypeCode()));
    }
}
