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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import org.finra.herd.model.api.xml.NamespaceAuthorization;
import org.finra.herd.model.api.xml.UserAuthorizations;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.SecurityUserWrapper;
import org.finra.herd.model.jpa.SecurityRoleEntity;

/**
 * This class tests various functionality within the current user service.
 */
public class CurrentUserServiceTest extends AbstractServiceTest
{
    @Test
    public void testGetCurrentUser() throws Exception
    {
        // Create a set of test namespace authorizations.
        Set<NamespaceAuthorization> namespaceAuthorizations = new LinkedHashSet<>();
        namespaceAuthorizations.add(new NamespaceAuthorization(NAMESPACE, SUPPORTED_NAMESPACE_PERMISSIONS));
        namespaceAuthorizations.add(new NamespaceAuthorization(NAMESPACE_2, SUPPORTED_NAMESPACE_PERMISSIONS));

        // Create test roles
        List<SecurityRoleEntity> securityRoleEntities = securityRoleDaoTestHelper.createTestSecurityRoles();

        // Fetch the security role codes to add to the application user.
        Set<String> roles = securityRoleEntities.stream().map(SecurityRoleEntity::getCode).collect(Collectors.toSet());

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
                    List<SimpleGrantedAuthority> authorities =
                        Arrays.asList(new SimpleGrantedAuthority(SECURITY_FUNCTION), new SimpleGrantedAuthority(SECURITY_FUNCTION_2));

                    ApplicationUser applicationUser = new ApplicationUser(this.getClass());
                    applicationUser.setUserId(USER_ID);
                    applicationUser.setRoles(roles);
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
            UserAuthorizations userAuthorizations = currentUserService.getCurrentUser();

            // Validate the response object.
            assertEquals(new UserAuthorizations(USER_ID, new ArrayList<>(namespaceAuthorizations), new ArrayList<>(roles),
                Arrays.asList(SECURITY_FUNCTION, SECURITY_FUNCTION_2)), userAuthorizations);
        }
        finally
        {
            // Restore the original authentication.
            SecurityContextHolder.getContext().setAuthentication(originalAuthentication);
        }
    }

    @Test
    public void testGetCurrentUserNoAuthentication() throws Exception
    {
        // Override the security context to have no authentication.
        Authentication originalAuthentication = SecurityContextHolder.getContext().getAuthentication();
        try
        {
            // Get the current user information.
            UserAuthorizations userAuthorizations = currentUserService.getCurrentUser();

            // Validate the response object.
            assertEquals(new UserAuthorizations(null, null, NO_SECURITY_ROLES, NO_SECURITY_FUNCTIONS), userAuthorizations);
        }
        finally
        {
            // Restore the original authentication.
            SecurityContextHolder.getContext().setAuthentication(originalAuthentication);
        }
    }

    @Test
    public void testGetCurrentUserNoSecurityRolesAndFunctions() throws Exception
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
                    List<SimpleGrantedAuthority> authorities = new ArrayList<>();

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
            UserAuthorizations userAuthorizations = currentUserService.getCurrentUser();

            // Validate the response object.
            assertEquals(new UserAuthorizations(USER_ID, new ArrayList<>(namespaceAuthorizations), NO_SECURITY_ROLES, NO_SECURITY_FUNCTIONS),
                userAuthorizations);
        }
        finally
        {
            // Restore the original authentication.
            SecurityContextHolder.getContext().setAuthentication(originalAuthentication);
        }
    }
}
