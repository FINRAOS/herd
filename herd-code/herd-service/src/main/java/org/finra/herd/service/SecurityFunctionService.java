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
package org.finra.herd.service;

import org.finra.herd.model.api.xml.SecurityFunction;
import org.finra.herd.model.api.xml.SecurityFunctionCreateRequest;
import org.finra.herd.model.api.xml.SecurityFunctionKey;
import org.finra.herd.model.api.xml.SecurityFunctionKeys;

/**
 * The security function service.
 */
public interface SecurityFunctionService
{
    /**
     * Creates a new security function.
     *
     * @param securityFunctionCreateRequest the security function create request
     *
     * @return the created security function
     */
    public SecurityFunction createSecurityFunction(SecurityFunctionCreateRequest securityFunctionCreateRequest);

    /**
     * Gets a security function for the specified key.
     *
     * @param securityFunctionKey the security function key
     *
     * @return the security function
     */
    public SecurityFunction getSecurityFunction(SecurityFunctionKey securityFunctionKey);

    /**
     * Deletes a security function for the specified name.
     *
     * @param securityFunctionKey the security function key
     *
     * @return the security function that was deleted
     */
    public SecurityFunction deleteSecurityFunction(SecurityFunctionKey securityFunctionKey);

    /**
     * Gets a list of security function keys for all security functions defined in the system.
     *
     * @return the security function keys
     */
    public SecurityFunctionKeys getSecurityFunctions();
}
