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

import org.finra.herd.model.api.xml.SecurityRoleKey;

/**
 * A helper class for security role related code.
 */
@Component
public class SecurityRoleHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    /**
     * Validates a security role key. This method also trims the key parameters.
     *
     * @param securityRoleKey the security role key
     */
    public void validateAndTrimSecurityRoleKey(SecurityRoleKey securityRoleKey)
    {
        Assert.notNull(securityRoleKey, "A security role key must be specified.");
        securityRoleKey.setSecurityRoleName(alternateKeyHelper.validateStringParameter("security role name", securityRoleKey.getSecurityRoleName()));
    }
}
