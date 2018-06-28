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
package org.finra.herd.dao;

import java.util.List;

import org.finra.herd.model.jpa.SecurityFunctionEntity;

public interface SecurityFunctionDao extends BaseJpaDao
{

    /**
     * Gets a security function by it's name.
     *
     * @param securityFunctionName the security function name (case-insensitive)
     *
     * @return the security function for the specified key
     */
    public SecurityFunctionEntity getSecurityFunctionByName(String securityFunctionName);

    /**
     * Gets a list of security functions.
     *
     * @return the list of functions
     */
    public List<String> getSecurityFunctions();

    /**
     * Gets a list of functions for the role.
     *
     * @param roleCd the role code
     *
     * @return the list of functions
     */
    public List<String> getSecurityFunctionsForRole(String roleCd);

    /**
     * Gets a list of security functions that have no roles mapped to them.
     *
     * @return the list of functions
     */
    public List<String> getUnrestrictedSecurityFunctions();
}
