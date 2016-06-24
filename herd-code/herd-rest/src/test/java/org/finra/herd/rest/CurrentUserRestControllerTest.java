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
package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import org.finra.herd.model.api.xml.NamespaceAuthorization;
import org.finra.herd.model.api.xml.UserAuthorizations;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.SecurityUserWrapper;

/**
 * This class tests various functionality within the current user REST controller.
 */
public class CurrentUserRestControllerTest extends AbstractRestTest
{
    @Test
    public void testGetCurrentUser() throws Exception
    {
        // Create a set of test namespace authorizations.
        Set<NamespaceAuthorization> namespaceAuthorizations = new LinkedHashSet<>();
        namespaceAuthorizations.add(new NamespaceAuthorization(NAMESPACE, SUPPORTED_NAMESPACE_PERMISSIONS));
        namespaceAuthorizations.add(new NamespaceAuthorization(NAMESPACE_2, SUPPORTED_NAMESPACE_PERMISSIONS));

        // Override the security context to return an application user populated with test values.
        Authentication originalAuthentication = SecurityContextHolder.getContext().getAuthentication();
        try
        {
            SecurityContextHolder.getContext().setAuthentication(new Authentication()
            {
                @Override
                public String getName()
                {
                    return null;
                }

                @Override
                public void setAuthenticated(boolean isAuthenticated) throws IllegalArgumentException
                {
                }

                @Override
                public boolean isAuthenticated()
                {
                    return false;
                }

                @Override
                public Object getPrincipal()
                {
                    List<GrantedAuthority> authorities = Collections.emptyList();

                    ApplicationUser applicationUser = new ApplicationUser(this.getClass());
                    applicationUser.setUserId(USER_ID);
                    applicationUser.setNamespaceAuthorizations(namespaceAuthorizations);

                    return new SecurityUserWrapper(USER_ID, STRING_VALUE, true, true, true, true, authorities, applicationUser);
                }

                @Override
                public Object getDetails()
                {
                    return null;
                }

                @Override
                public Object getCredentials()
                {
                    return null;
                }

                @Override
                public Collection<? extends GrantedAuthority> getAuthorities()
                {
                    return null;
                }
            });

            // Get the current user information.
            UserAuthorizations userAuthorizations = currentUserRestController.getCurrentUser();

            // Validate the response object.
            assertEquals(new UserAuthorizations(USER_ID, new ArrayList<>(namespaceAuthorizations)), userAuthorizations);
        }
        finally
        {
            // Restore the original authentication.
            SecurityContextHolder.getContext().setAuthentication(originalAuthentication);
        }
    }
}
