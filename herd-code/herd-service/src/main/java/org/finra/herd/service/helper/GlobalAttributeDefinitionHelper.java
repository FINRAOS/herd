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

import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKey;

/**
 * A helper class for Global Attribute Definition related code.
 */
@Component
public class GlobalAttributeDefinitionHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    /**
     * Validates a global attribute definition key. This method also trims the key parameters.
     *
     * @param globalAttributeDefinitionKey the global attribute definition key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateGlobalAttributeDefinitionKey(GlobalAttributeDefinitionKey globalAttributeDefinitionKey) throws IllegalArgumentException
    {
        // Validate.
        Assert.notNull(globalAttributeDefinitionKey, "A global attribute definition key must be specified.");
        globalAttributeDefinitionKey.setGlobalAttributeDefinitionLevel(
            alternateKeyHelper.validateStringParameter("global attribute definition level", globalAttributeDefinitionKey.getGlobalAttributeDefinitionLevel()));
        globalAttributeDefinitionKey.setGlobalAttributeDefinitionName(
            alternateKeyHelper.validateStringParameter("global attribute definition name", globalAttributeDefinitionKey.getGlobalAttributeDefinitionName()));
    }

}
