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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockFilterConfig;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.context.SecurityContextHolder;

import org.finra.herd.app.AbstractAppTest;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * This class tests the trusted user authentication filter.
 */
public class TrustedUserAuthenticationFilterTest extends AbstractAppTest
{
    @Test
    public void testTrustedUserFilter() throws Exception
    {
        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.SECURITY_ENABLED_SPEL_EXPRESSION.getKey(), "false");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Invalidate user session if exists.
            invalidateApplicationUser(null);

            trustedUserAuthenticationFilter.init(new MockFilterConfig());
            trustedUserAuthenticationFilter.doFilter(new MockHttpServletRequest(), new MockHttpServletResponse(), new MockFilterChain());

            validateTrustedApplicationUser();

            // retry with same request.
            trustedUserAuthenticationFilter.doFilter(new MockHttpServletRequest(), new MockHttpServletResponse(), new MockFilterChain());

            validateTrustedApplicationUser();
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testTrustedUserFilterSecurityEnabled() throws Exception
    {
        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.SECURITY_ENABLED_SPEL_EXPRESSION.getKey(), "true");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Invalidate user session if exists.
            invalidateApplicationUser(null);

            trustedUserAuthenticationFilter.init(new MockFilterConfig());
            trustedUserAuthenticationFilter.doFilter(new MockHttpServletRequest(), new MockHttpServletResponse(), new MockFilterChain());

            assertNoUserInContext();
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testTrustedUserFilterNoSpel() throws Exception
    {
        // Override configuration.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.SECURITY_ENABLED_SPEL_EXPRESSION.getKey(), "");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Invalidate user session if exists.
            invalidateApplicationUser(null);

            trustedUserAuthenticationFilter.init(new MockFilterConfig());
            trustedUserAuthenticationFilter.doFilter(new MockHttpServletRequest(), new MockHttpServletResponse(), new MockFilterChain());

            assertNoUserInContext();
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testTrustedUserFilterSwitchTrustedUser() throws Exception
    {
        // Override configuration.
        Map<String, Object> overrideMap = getDefaultSecurityEnvironmentVariables();
        overrideMap.put(ConfigurationValue.SECURITY_ENABLED_SPEL_EXPRESSION.getKey(), "false");
        modifyPropertySourceInEnvironment(overrideMap);

        // Create HttpHeader user in session.
        MockHttpServletRequest request =
            getRequestWithHeaders("testUser", "testFirstName", "testLastName", "testEmail", "testRole", "Wed, 11 Mar 2015 10:24:09");
        // Invalidate user session if exists.
        invalidateApplicationUser(request);

        httpHeaderAuthenticationFilter.init(new MockFilterConfig());
        httpHeaderAuthenticationFilter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

        assertNoUserInContext();

        // Now apply the trusted user filter to ensure that user is switched to trusted user

        try
        {
            trustedUserAuthenticationFilter.init(new MockFilterConfig());
            trustedUserAuthenticationFilter.doFilter(new MockHttpServletRequest(), new MockHttpServletResponse(), new MockFilterChain());
            validateTrustedApplicationUser();
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    private void assertNoUserInContext()
    {
        assertNull("security context authentication", SecurityContextHolder.getContext().getAuthentication());
    }
}
