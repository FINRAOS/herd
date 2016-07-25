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

import static org.junit.Assert.assertNull;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockFilterConfig;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import org.finra.herd.app.AbstractAppTest;
import org.finra.herd.model.api.xml.NamespaceAuthorization;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.SecurityFunctionEntity;
import org.finra.herd.model.jpa.SecurityRoleEntity;
import org.finra.herd.model.jpa.SecurityRoleFunctionEntity;

/**
 * This class tests the http header authentication filter.
 */
public class HttpHeaderAuthenticationFilterTest extends AbstractAppTest
{
    private final String[] TEST_FUNCTIONS = {"test_function_1", "test_function_2"};

    @Test
    public void testHttpHeaderAuthenticationFilter() throws Exception
    {
        setupTestFunctions("testRole");
        modifyPropertySourceInEnvironment(getDefaultSecurityEnvironmentVariables());

        try
        {
            MockHttpServletRequest request =
                getRequestWithHeaders(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09");

            // Invalidate user session if exists.
            invalidateApplicationUser(request);

            httpHeaderAuthenticationFilter.init(new MockFilterConfig());
            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            validateHttpHeaderApplicationUser(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09", TEST_FUNCTIONS,
                null);

            // retry with same request.
            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            validateHttpHeaderApplicationUser(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09", TEST_FUNCTIONS,
                null);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testHttpHeaderAuthenticationFilterRegularUser() throws Exception
    {
        // Create and persist the relative database entities.
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(USER_ID, namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE_2), SUPPORTED_NAMESPACE_PERMISSIONS);
        userNamespaceAuthorizationDaoTestHelper
            .createUserNamespaceAuthorizationEntity(USER_ID, namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE_3), SUPPORTED_NAMESPACE_PERMISSIONS);

        // Create an ordered set of expected namespace authorizations.
        Set<NamespaceAuthorization> expectedNamespaceAuthorizations = new HashSet<>();
        expectedNamespaceAuthorizations.add(new NamespaceAuthorization(NAMESPACE_2, SUPPORTED_NAMESPACE_PERMISSIONS));
        expectedNamespaceAuthorizations.add(new NamespaceAuthorization(NAMESPACE_3, SUPPORTED_NAMESPACE_PERMISSIONS));

        setupTestFunctions("testRole");
        modifyPropertySourceInEnvironment(getDefaultSecurityEnvironmentVariables());

        try
        {
            MockHttpServletRequest request =
                getRequestWithHeaders(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09");

            // Invalidate user session if exists.
            invalidateApplicationUser(request);

            httpHeaderAuthenticationFilter.init(new MockFilterConfig());
            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            validateHttpHeaderApplicationUser(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09", TEST_FUNCTIONS,
                expectedNamespaceAuthorizations);

            // retry with same request.
            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            validateHttpHeaderApplicationUser(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09", TEST_FUNCTIONS,
                expectedNamespaceAuthorizations);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testHttpHeaderAuthenticationFilterAdminUser() throws Exception
    {
        // Create and persist the relative database entities.
        userDaoTestHelper.createUserEntity(USER_ID, true);
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE_2);

        // Create an ordered set of expected namespace authorizations.
        Set<NamespaceAuthorization> expectedNamespaceAuthorizations = new HashSet<>();
        expectedNamespaceAuthorizations.add(new NamespaceAuthorization(NAMESPACE, SUPPORTED_NAMESPACE_PERMISSIONS));
        expectedNamespaceAuthorizations.add(new NamespaceAuthorization(NAMESPACE_2, SUPPORTED_NAMESPACE_PERMISSIONS));

        setupTestFunctions("testRole");
        modifyPropertySourceInEnvironment(getDefaultSecurityEnvironmentVariables());

        try
        {
            MockHttpServletRequest request =
                getRequestWithHeaders(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09");
            // Invalidate user session if exists.
            invalidateApplicationUser(request);

            httpHeaderAuthenticationFilter.init(new MockFilterConfig());
            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            validateHttpHeaderApplicationUser(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09", TEST_FUNCTIONS,
                expectedNamespaceAuthorizations);

            // retry with same request.
            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            validateHttpHeaderApplicationUser(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09", TEST_FUNCTIONS,
                expectedNamespaceAuthorizations);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testHttpHeaderAuthenticationFilterUserAuthorizationDisabled() throws Exception
    {
        // Create and persist the relative database entities.
        userDaoTestHelper.createUserEntity(USER_ID, true);
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE_2);

        // Create an ordered set of expected namespace authorizations.
        Set<NamespaceAuthorization> expectedNamespaceAuthorizations = new HashSet<>();
        expectedNamespaceAuthorizations.add(new NamespaceAuthorization(NAMESPACE, SUPPORTED_NAMESPACE_PERMISSIONS));
        expectedNamespaceAuthorizations.add(new NamespaceAuthorization(NAMESPACE_2, SUPPORTED_NAMESPACE_PERMISSIONS));

        setupTestFunctions("testRole");
        Map<String, Object> overrideMap = getDefaultSecurityEnvironmentVariables();
        overrideMap.put(ConfigurationValue.USER_NAMESPACE_AUTHORIZATION_ENABLED.getKey(), "false");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            MockHttpServletRequest request =
                getRequestWithHeaders(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09");
            // Invalidate user session if exists.
            invalidateApplicationUser(request);

            httpHeaderAuthenticationFilter.init(new MockFilterConfig());
            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            validateHttpHeaderApplicationUser(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09", TEST_FUNCTIONS,
                expectedNamespaceAuthorizations);

            // retry with same request.
            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            validateHttpHeaderApplicationUser(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09", TEST_FUNCTIONS,
                expectedNamespaceAuthorizations);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testHttpHeaderAuthenticationFilterUserAuthorizationInvalidConfigurationValue() throws Exception
    {
        // Create and persist the relative database entities.
        userDaoTestHelper.createUserEntity(USER_ID, true);
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE_2);

        // Create an ordered set of expected namespace authorizations.
        Set<NamespaceAuthorization> expectedNamespaceAuthorizations = new HashSet<>();
        expectedNamespaceAuthorizations.add(new NamespaceAuthorization(NAMESPACE, SUPPORTED_NAMESPACE_PERMISSIONS));
        expectedNamespaceAuthorizations.add(new NamespaceAuthorization(NAMESPACE_2, SUPPORTED_NAMESPACE_PERMISSIONS));

        setupTestFunctions("testRole");
        Map<String, Object> overrideMap = getDefaultSecurityEnvironmentVariables();
        overrideMap.put(ConfigurationValue.USER_NAMESPACE_AUTHORIZATION_ENABLED.getKey(), "NOT_A_BOOLEAN");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            MockHttpServletRequest request =
                getRequestWithHeaders(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09");

            // Invalidate user session if exists.
            invalidateApplicationUser(request);

            httpHeaderAuthenticationFilter.init(new MockFilterConfig());
            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            // Validate that there is no authentication.
            assertNull(SecurityContextHolder.getContext().getAuthentication());
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testHttpHeaderAuthenticationFilterNoHeaders() throws Exception
    {
        modifyPropertySourceInEnvironment(getDefaultSecurityEnvironmentVariables());

        try
        {
            // Invalidate user session if exists.
            invalidateApplicationUser(null);

            httpHeaderAuthenticationFilter.init(new MockFilterConfig());
            httpHeaderAuthenticationFilter.doFilter(new MockHttpServletRequest(), new MockHttpServletResponse(), new MockFilterChain());

            Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
            assertNull(authentication);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testHttpHeaderAuthenticationFilterUserChangedInHeaders() throws Exception
    {
        modifyPropertySourceInEnvironment(getDefaultSecurityEnvironmentVariables());

        try
        {
            MockHttpServletRequest request =
                getRequestWithHeaders(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09");

            // Invalidate user session if exists.
            invalidateApplicationUser(request);

            httpHeaderAuthenticationFilter.init(new MockFilterConfig());
            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            validateHttpHeaderApplicationUser(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09", null, null);

            // Change the userId in the header.
            request = getRequestWithHeaders(USER_ID_2, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09");

            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            validateHttpHeaderApplicationUser(USER_ID_2, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09", null, null);

            // Change the session init time in the header.
            request = getRequestWithHeaders(USER_ID_2, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 11:24:09");

            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            validateHttpHeaderApplicationUser(USER_ID_2, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 11:24:09", null, null);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testHttpHeaderAuthenticationFilterNoRoles() throws Exception
    {
        modifyPropertySourceInEnvironment(getDefaultSecurityEnvironmentVariables());

        try
        {
            MockHttpServletRequest request = getRequestWithHeaders(USER_ID, "testFirstName", "testLastName", "testEmail", null, "Wed, 11 Mar 2015 10:24:09");

            // Invalidate user session if exists.
            invalidateApplicationUser(request);

            httpHeaderAuthenticationFilter.init(new MockFilterConfig());
            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            validateHttpHeaderApplicationUser(USER_ID, "testFirstName", "testLastName", "testEmail", (String) null, "Wed, 11 Mar 2015 10:24:09", null, null);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testHttpHeaderAuthenticationFilterNoSessionInitTime() throws Exception
    {
        modifyPropertySourceInEnvironment(getDefaultSecurityEnvironmentVariables());

        try
        {
            MockHttpServletRequest request = getRequestWithHeaders(USER_ID, "testFirstName", "testLastName", "testEmail", null, null);

            // Invalidate user session if exists.
            invalidateApplicationUser(request);

            httpHeaderAuthenticationFilter.init(new MockFilterConfig());
            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            validateHttpHeaderApplicationUser(USER_ID, "testFirstName", "testLastName", "testEmail", (String) null, null, null, null);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testHttpHeaderAuthenticationFilterInvalidateSessionOnWrongHeader() throws Exception
    {
        modifyPropertySourceInEnvironment(getDefaultSecurityEnvironmentVariables());

        try
        {
            MockHttpServletRequest request =
                getRequestWithHeaders(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09");
            // Invalidate user session if exists.
            invalidateApplicationUser(request);

            httpHeaderAuthenticationFilter.init(new MockFilterConfig());
            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            validateHttpHeaderApplicationUser(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09", null, null);

            // Try again with no header, user should be invalidated.
            httpHeaderAuthenticationFilter.doFilter(new MockHttpServletRequest(), new MockHttpServletResponse(), new MockFilterChain());

            Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
            assertNull(authentication);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testHttpHeaderAuthenticationFilterMultipleRoles() throws Exception
    {
        modifyPropertySourceInEnvironment(getDefaultSecurityEnvironmentVariables());

        try
        {
            MockHttpServletRequest request =
                getRequestWithHeaders(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole1,testRole2", "Wed, 11 Mar 2015 10:24:09");
            // Invalidate user session if exists.
            invalidateApplicationUser(request);

            httpHeaderAuthenticationFilter.init(new MockFilterConfig());
            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            Set<String> expectedRoles = new HashSet<>();
            expectedRoles.add("testRole1");
            expectedRoles.add("testRole2");
            validateHttpHeaderApplicationUser(USER_ID, "testFirstName", "testLastName", "testEmail", expectedRoles, "Wed, 11 Mar 2015 10:24:09", null, null);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testHttpHeaderAuthenticationFilterEmptyRoleRegex() throws Exception
    {
        Map<String, Object> overrideMap = getDefaultSecurityEnvironmentVariables();
        overrideMap.put(ConfigurationValue.SECURITY_HTTP_HEADER_ROLE_REGEX.getKey(), " ");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            MockHttpServletRequest request =
                getRequestWithHeaders(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole1,testRole2", "Wed, 11 Mar 2015 10:24:09");
            // Invalidate user session if exists.
            invalidateApplicationUser(request);

            httpHeaderAuthenticationFilter.init(new MockFilterConfig());
            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            Set<String> expectedRoles = new HashSet<>();
            validateHttpHeaderApplicationUser(USER_ID, "testFirstName", "testLastName", "testEmail", expectedRoles, "Wed, 11 Mar 2015 10:24:09", null, null);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testHttpHeaderAuthenticationFilterNoRegexGroup() throws Exception
    {
        Map<String, Object> overrideMap = getDefaultSecurityEnvironmentVariables();
        overrideMap.put(ConfigurationValue.SECURITY_HTTP_HEADER_ROLE_REGEX_GROUP.getKey(), " ");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            MockHttpServletRequest request =
                getRequestWithHeaders(USER_ID, "testFirstName", "testLastName", "testEmail", "testRole1,testRole2", "Wed, 11 Mar 2015 10:24:09");
            // Invalidate user session if exists.
            invalidateApplicationUser(request);

            httpHeaderAuthenticationFilter.init(new MockFilterConfig());
            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            Set<String> expectedRoles = new HashSet<>();
            expectedRoles.add("testRole1,");
            expectedRoles.add("testRole2");
            validateHttpHeaderApplicationUser(USER_ID, "testFirstName", "testLastName", "testEmail", expectedRoles, "Wed, 11 Mar 2015 10:24:09", null, null);

        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testHttpHeaderAuthenticationFilterUserIdWithDomainName() throws Exception
    {
        modifyPropertySourceInEnvironment(getDefaultSecurityEnvironmentVariables());

        try
        {
            MockHttpServletRequest request =
                getRequestWithHeaders("testUser@company.com", "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09");
            // Invalidate user session if exists.
            invalidateApplicationUser(request);

            httpHeaderAuthenticationFilter.init(new MockFilterConfig());
            httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

            validateHttpHeaderApplicationUser("testUser@company.com", "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09",
                null, null);
        }
        finally
        {
            restorePropertySourceInEnvironment();
        }
    }

    private void setupTestFunctions(String roleId)
    {
        SecurityRoleEntity securityRoleEntity = new SecurityRoleEntity();
        securityRoleEntity.setCode(roleId);

        herdDao.saveAndRefresh(securityRoleEntity);

        for (String function : TEST_FUNCTIONS)
        {
            SecurityFunctionEntity securityFunctionEntity = new SecurityFunctionEntity();
            securityFunctionEntity.setCode(function);
            herdDao.saveAndRefresh(securityFunctionEntity);

            SecurityRoleFunctionEntity securityRoleFunctionEntity = new SecurityRoleFunctionEntity();
            securityRoleFunctionEntity.setSecurityRole(securityRoleEntity);
            securityRoleFunctionEntity.setSecurityFunction(securityFunctionEntity);

            herdDao.saveAndRefresh(securityRoleFunctionEntity);
        }
    }
}
