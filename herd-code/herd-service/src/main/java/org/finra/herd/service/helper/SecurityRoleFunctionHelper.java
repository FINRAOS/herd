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

import org.finra.herd.model.api.xml.SecurityRoleFunctionCreateRequest;
import org.finra.herd.model.api.xml.SecurityRoleFunctionKey;

/**
 * A helper class for security role to function mapping related code.
 */
@Component
public class SecurityRoleFunctionHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    /**
     * Validates a security role to function mapping create request. This method also trims the request parameters.
     *
     * @param securityRoleFunctionCreateRequest the security role to function mapping create request
     */
    public void validateAndTrimSecurityRoleFunctionCreateRequest(SecurityRoleFunctionCreateRequest securityRoleFunctionCreateRequest)
    {
        Assert.notNull(securityRoleFunctionCreateRequest, "A security role to function mapping create request must be specified.");
        validateAndTrimSecurityRoleFunctionKey(securityRoleFunctionCreateRequest.getSecurityRoleFunctionKey());
    }

    /**
     * Validates a security role to function mapping key. This method also trims the key parameters.
     *
     * @param securityRoleFunctionKey the security role to function mapping key
     */
    public void validateAndTrimSecurityRoleFunctionKey(SecurityRoleFunctionKey securityRoleFunctionKey)
    {
        Assert.notNull(securityRoleFunctionKey, "A security role to function mapping key must be specified.");
        securityRoleFunctionKey
            .setSecurityRoleName(alternateKeyHelper.validateStringParameter("security role name", securityRoleFunctionKey.getSecurityRoleName()));
        securityRoleFunctionKey
            .setSecurityFunctionName(alternateKeyHelper.validateStringParameter("security function name", securityRoleFunctionKey.getSecurityFunctionName()));
    }
}
