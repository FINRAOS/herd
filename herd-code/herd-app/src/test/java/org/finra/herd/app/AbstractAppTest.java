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
package org.finra.herd.app;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.web.WebAppConfiguration;

import org.finra.herd.app.config.AppTestSpringModuleConfig;
import org.finra.herd.app.security.HttpHeaderApplicationUserBuilder;
import org.finra.herd.app.security.HttpHeaderAuthenticationFilter;
import org.finra.herd.app.security.TrustedApplicationUserBuilder;
import org.finra.herd.app.security.TrustedUserAuthenticationFilter;
import org.finra.herd.model.api.xml.NamespaceAuthorization;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.SecurityUserWrapper;
import org.finra.herd.rest.AbstractRestTest;

/**
 * This is an abstract base class that provides useful methods for REST test drivers.
 */
@ContextConfiguration(classes = AppTestSpringModuleConfig.class, inheritLocations = false)
@WebAppConfiguration
public abstract class AbstractAppTest extends AbstractRestTest
{
    @Autowired
    protected TrustedUserAuthenticationFilter trustedUserAuthenticationFilter;

    @Autowired
    protected HttpHeaderAuthenticationFilter httpHeaderAuthenticationFilter;

    /**
     * Invalidated the user in session and also clears the spring security context.
     */
    protected void invalidateApplicationUser(HttpServletRequest request)
    {
        if (request != null)
        {
            HttpSession session = request.getSession(false);
            if (session != null)
            {
                session.invalidate();
            }
        }

        SecurityContextHolder.clearContext();
    }

    protected MockHttpServletRequest getRequestWithHeaders(String userId, String firstName, String lastName, String email, String memberOf,
        String sessionInitTime)
    {
        MockHttpServletRequest request = new MockHttpServletRequest();

        if (StringUtils.isNotBlank(userId))
        {
            request.addHeader("userId", userId);
        }
        if (StringUtils.isNotBlank(firstName))
        {
            request.addHeader("firstName", firstName);
        }
        if (StringUtils.isNotBlank(lastName))
        {
            request.addHeader("lastName", lastName);
        }
        if (StringUtils.isNotBlank(email))
        {
            request.addHeader("email", email);
        }
        if (StringUtils.isNotBlank(memberOf))
        {
            request.addHeader("roles", memberOf);
        }
        if (StringUtils.isNotBlank(sessionInitTime))
        {
            request.addHeader("sessionInitTime", sessionInitTime);
        }

        return request;
    }

    protected Map<String, Object> getDefaultSecurityEnvironmentVariables()
    {
        Map<String, Object> defaultEnvironmentVariables = new HashMap<>();
        defaultEnvironmentVariables.put(ConfigurationValue.SECURITY_HTTP_HEADER_NAMES.getKey(), "useridHeader=userId|firstNameHeader=firstName" +
            "|lastNameHeader=lastName|emailHeader=email|rolesHeader=roles|sessionInitTimeHeader=sessionInitTime");
        defaultEnvironmentVariables.put(ConfigurationValue.SECURITY_HTTP_HEADER_ROLE_REGEX.getKey(), "(?<role>.+?)(,|$)");
        defaultEnvironmentVariables.put(ConfigurationValue.SECURITY_HTTP_HEADER_ROLE_REGEX_GROUP.getKey(), "role");
        defaultEnvironmentVariables.put(ConfigurationValue.USER_NAMESPACE_AUTHORIZATION_ENABLED.getKey(), "true");
        return defaultEnvironmentVariables;
    }

    protected Map<String, Object> getDefaultSecurityEnvironmentVariablesWithMultiHeaderRoles()
    {
        Map<String, Object> defaultEnvironmentVariables = new HashMap<>();
        defaultEnvironmentVariables.put(ConfigurationValue.SECURITY_HTTP_HEADER_NAMES.getKey(), "useridHeader=userId|firstNameHeader=firstName" +
            "|lastNameHeader=lastName|emailHeader=email|sessionInitTimeHeader=sessionInitTime|useridSuffixHeader=useridSuffix");
        defaultEnvironmentVariables.put(ConfigurationValue.SECURITY_HTTP_HEADER_ROLES_REGEX.getKey(), "priv(.+)");
        defaultEnvironmentVariables.put(ConfigurationValue.USER_NAMESPACE_AUTHORIZATION_ENABLED.getKey(), "true");
        return defaultEnvironmentVariables;
    }

    /**
     * Delegates to validateHttpHeaderApplicationUser(String, String, String, String, Set, String) with a single role.
     *
     * @param expectedUserId the expected user Id.
     * @param expectedFirstName the expected first name.
     * @param expectedLastName the expected last name.
     * @param expectedEmail the expected e-mail.
     * @param expectedRole the expected role.
     * @param expectedSessionInitTime the expected session init time.
     * @param expectedFunctions the expected functions.
     *
     * @throws Exception if any errors were encountered.
     */
    protected void validateHttpHeaderApplicationUser(String expectedUserId, String expectedFirstName, String expectedLastName, String expectedEmail,
        String expectedRole, String expectedSessionInitTime, String[] expectedFunctions, Set<NamespaceAuthorization> expectedNamespaceAuthorizations)
        throws Exception
    {
        Set<String> roles = new HashSet<>();
        if (expectedRole != null)
        {
            roles.add(expectedRole);
        }
        validateHttpHeaderApplicationUser(expectedUserId, expectedFirstName, expectedLastName, expectedEmail, roles, expectedSessionInitTime, expectedFunctions,
            expectedNamespaceAuthorizations);
    }

    /**
     * Retrieves the user from the current spring security context and asserts that each of the properties of the user matches the given expected values.
     * Asserts that the principal stored in the current security context user is an instance of {@link SecurityUserWrapper}.
     *
     * @param expectedUserId the expected user Id.
     * @param expectedFirstName the expected first name.
     * @param expectedLastName the expected last name.
     * @param expectedEmail the expected e-mail.
     * @param expectedRoles the expected roles.
     * @param expectedSessionInitTime the expected session init time.
     * @param expectedFunctions the expected functions.
     *
     * @throws Exception if any errors were encountered.
     */
    protected void validateHttpHeaderApplicationUser(String expectedUserId, String expectedFirstName, String expectedLastName, String expectedEmail,
        Set<String> expectedRoles, String expectedSessionInitTime, String[] expectedFunctions, Set<NamespaceAuthorization> expectedNamespaceAuthorizations)
        throws Exception
    {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        assertNotNull(authentication);

        Object principal = authentication.getPrincipal();
        assertNotNull("expected principal to be not null, but was null", principal);
        assertTrue("expected principal to be an instance of " + SecurityUserWrapper.class + ", but was an instance of  " + principal.getClass(),
            principal instanceof SecurityUserWrapper);
        SecurityUserWrapper user = (SecurityUserWrapper) principal;
        ApplicationUser applicationUser = user.getApplicationUser();
        assertEquals(expectedUserId, applicationUser.getUserId());
        assertEquals(expectedFirstName, applicationUser.getFirstName());
        assertEquals(expectedLastName, applicationUser.getLastName());
        assertEquals(expectedEmail, applicationUser.getEmail());

        assertEquals(expectedRoles, applicationUser.getRoles());
        if (StringUtils.isNotBlank(expectedSessionInitTime))
        {
            assertEquals(DateUtils.parseDate(expectedSessionInitTime, HttpHeaderApplicationUserBuilder.CALENDAR_PATTERNS),
                applicationUser.getSessionInitTime());
        }

        assertNotNull(applicationUser.getSessionId());

        assertEquals(HttpHeaderApplicationUserBuilder.class, applicationUser.getGeneratedByClass());

        // Validate functions.
        if (expectedFunctions != null)
        {
            Set<String> functions = new HashSet<>();
            for (GrantedAuthority grantedAuthority : user.getAuthorities())
            {
                functions.add(grantedAuthority.getAuthority());
            }

            for (String expectedFunction : expectedFunctions)
            {
                assertTrue(functions.contains(expectedFunction));
            }
        }

        // Validate namespace authorizations.
        if (expectedNamespaceAuthorizations != null)
        {
            assertEquals(expectedNamespaceAuthorizations, applicationUser.getNamespaceAuthorizations());
        }
    }

    protected void validateTrustedApplicationUser() throws Exception
    {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        assertNotNull(authentication);

        SecurityUserWrapper user = (SecurityUserWrapper) authentication.getPrincipal();
        ApplicationUser applicationUser = user.getApplicationUser();
        assertEquals(TrustedApplicationUserBuilder.TRUSTED_USER_ID, applicationUser.getUserId());
        assertEquals(TrustedApplicationUserBuilder.TRUSTED_USER_FIRST_NAME, applicationUser.getFirstName());
        assertEquals(TrustedApplicationUserBuilder.TRUSTED_USER_LAST_NAME, applicationUser.getLastName());
        assertEquals(TrustedApplicationUserBuilder.TRUSTED_USER_EMAIL, applicationUser.getEmail());

        Set<String> roles = applicationUser.getRoles();
        assertTrue(roles.contains(TrustedApplicationUserBuilder.TRUSTED_USER_ROLE));
        assertNotNull(applicationUser.getSessionId());

        assertEquals(TrustedApplicationUserBuilder.class, applicationUser.getGeneratedByClass());
    }
}
