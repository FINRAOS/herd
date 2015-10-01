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
package org.finra.dm.app.security;

import java.util.HashSet;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.finra.dm.model.dto.ApplicationUser;

/**
 * Application user builder that builds a trusted user with trusted role.
 */
public class TrustedApplicationUserBuilder implements ApplicationUserBuilder
{
    // TODO Should we make these values configurable.
    public static final String TRUSTED_USER_ID = "TRUSTED_USER";
    public static final String TRUSTED_USER_FIRST_NAME = "TRUSTED_USER_FIRST_NAME";
    public static final String TRUSTED_USER_LAST_NAME = "TRUSTED_USER_LAST_NAME";
    public static final String TRUSTED_USER_EMAIL = "TRUSTED_USER_EMAIL";
    public static final String TRUSTED_USER_ROLE = "TRUSTED_USER_ROLE";

    @Override
    public ApplicationUser build(HttpServletRequest request)
    {
        return buildUser(request, true);
    }

    @Override
    public ApplicationUser buildNoRoles(HttpServletRequest request)
    {
        return buildUser(request, false);
    }

    /**
     * Builds the application user.
     *
     * @param request the HTTP servlet request.
     * @param includeRoles If true, the user's roles will be included. Otherwise, not.
     *
     * @return the application user.
     */
    protected ApplicationUser buildUser(HttpServletRequest request, boolean includeRoles)
    {
        ApplicationUser applicationUser = new ApplicationUser(this.getClass());

        applicationUser.setUserId(TRUSTED_USER_ID);
        applicationUser.setFirstName(TRUSTED_USER_FIRST_NAME);
        applicationUser.setLastName(TRUSTED_USER_LAST_NAME);
        applicationUser.setEmail(TRUSTED_USER_EMAIL);
        applicationUser.setSessionId(request.getSession().getId());

        if (includeRoles)
        {
            Set<String> roles = new HashSet<>();
            roles.add(TRUSTED_USER_ROLE);
            applicationUser.setRoles(roles);
        }

        return applicationUser;
    }
}
