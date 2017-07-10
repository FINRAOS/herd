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

import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.SecurityRoleDao;

@Component
public class SecurityRoleDaoHelper
{
    @Autowired
    private SecurityRoleDao securityRoleDao;

    /**
     * Filters a set of roles based on a list of role values specific for herd.
     *
     * @param roles A given set of roles
     *
     * @return Valid roles from the specified set of roles
     */
    public Set<String> getValidSecurityRoles(Set<String> roles)
    {
        // Copy the roles to a set for easier computation
        Set<String> allPossibleValidRoles = new HashSet<>(securityRoleDao.getAllSecurityRoles());

        // Copy the set of specified roles to another set
        Set<String> incomingRoles = new HashSet<>(roles);

        // The Set of valid roles is the intersection of the two collections
        incomingRoles.retainAll(allPossibleValidRoles);

        // Return valid roles
        return incomingRoles;
    }
}
