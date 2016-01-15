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
package org.finra.herd.app.security;

import javax.servlet.http.HttpServletRequest;

import org.finra.herd.model.dto.ApplicationUser;

/**
 * A builder that knows how to build an application user.
 */
public interface ApplicationUserBuilder
{
    /**
     * Builds an application user from the headers contained within the HTTP servlet request.
     *
     * @param request the HTTP servlet request.
     *
     * @return the application user.
     */
    public ApplicationUser build(HttpServletRequest request);

    /**
     * Builds a user object without the roles from the headers contained within the HTTP servlet request. This method provides a security filter the ability to
     * compare the current header's username or session Id and session init time with the previous requests version to see if a different user is logged in.
     * Building the entire user would also work, but that could be expensive (e.g. role lookups could come from LDAP).
     *
     * @param request the HTTP servlet request.
     *
     * @return the application user without roles present.
     */
    public ApplicationUser buildNoRoles(HttpServletRequest request);
}
