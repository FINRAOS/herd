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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import org.finra.herd.app.AbstractAppTest;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * Tests cases where the {@link org.finra.herd.app.security.TrustedUserAuthenticationFilter} and {@link org.finra.herd.app.security.HttpHeaderAuthenticationFilter} are used in sequence.
 */
public class SecurityFilterChainTest extends AbstractAppTest
{
    @Before
    public void before()
    {
        SecurityContextHolder.clearContext();
    }

    /**
     * When the filters are executed with security disabled, and the filters are run again with security enabled, the trusted user should no longer be in the
     * context and instead the user should be created based on the headers given in the request.
     * 
     * @throws Exception
     */
    @Test
    public void testFilterAuthenticatedUserOverridesTrustedUser() throws Exception
    {
        String expectedUserId = "testUser";
        HashMap<String, Object> requestHeaders = new HashMap<>();

        // Execute filters with security disabled
        Authentication authentication1 = executeAuthenticationFilters(false, requestHeaders);
        assertAuthenticatedUserId(TrustedApplicationUserBuilder.TRUSTED_USER_ID, TrustedApplicationUserBuilder.TRUSTED_USER_FIRST_NAME, null, authentication1);

        // Execute filters with security disabled
        requestHeaders.put("userId", expectedUserId);
        Authentication authentication2 = executeAuthenticationFilters(true, requestHeaders);
        assertAuthenticatedUserId(expectedUserId, null, null, authentication2);
    }

    /**
     * When the filters are executed twice with security enabled, and the user ID header changes between the requests, the session's user should be recreated
     * with the
     * new headers.
     * 
     * @throws Exception
     */
    @Test
    public void testFilterUserIsReauthenticatedWhenUserIdChanges() throws Exception
    {
        String expectedUserId1 = "testUser1";
        String expectedUserId2 = "testUser2";

        HashMap<String, Object> requestHeaders = new HashMap<>();
        requestHeaders.put("userId", expectedUserId1);

        Authentication authentication1 = executeAuthenticationFilters(true, requestHeaders);
        assertAuthenticatedUserId(expectedUserId1, null, null, authentication1);

        requestHeaders.put("userId", expectedUserId2);

        Authentication authentication2 = executeAuthenticationFilters(true, requestHeaders);
        assertAuthenticatedUserId(expectedUserId2, null, null, authentication2);
    }

    /**
     * When the filters are executed twice with security enabled, and the session init time header changes between the requests, the session should be recreated
     * with
     * the new headers.
     * 
     * @throws Exception
     */
    @Test
    public void testFilterUserIsReauthenticatedWhenSessionInitTimeChanges() throws Exception
    {
        String expectedUserId = "testUser1";
        Date expectedSessionInitTime1 = new Date(1000l);
        Date expectedSessionInitTime2 = new Date(2000l);

        SimpleDateFormat dateFormat = new SimpleDateFormat(HttpHeaderApplicationUserBuilder.CALENDAR_PATTERN_PWD);

        HashMap<String, Object> requestHeaders = new HashMap<>();
        requestHeaders.put("userId", expectedUserId);
        requestHeaders.put("sessionInitTime", dateFormat.format(expectedSessionInitTime1));

        Authentication authentication1 = executeAuthenticationFilters(true, requestHeaders);
        assertAuthenticatedUserId(expectedUserId, null, expectedSessionInitTime1, authentication1);

        requestHeaders.put("sessionInitTime", dateFormat.format(expectedSessionInitTime2));
        Authentication authentication2 = executeAuthenticationFilters(true, requestHeaders);
        assertAuthenticatedUserId(expectedUserId, null, expectedSessionInitTime2, authentication2);
    }

    /**
     * When the filters are executed twice with security enabled, and only the first name header changes between the requests, the session should NOT be
     * recreated.
     * This behavior should also apply for any headers other than userId and sessionInitTime.
     * 
     * @throws Exception
     */
    @Test
    public void testFilterUserIsNotReauthenticatedWhenFirstNameChanges() throws Exception
    {
        String expectedUserId = "testUser1";
        String firstName1 = "firstName1";

        HashMap<String, Object> requestHeaders = new HashMap<>();
        requestHeaders.put("userId", expectedUserId);
        requestHeaders.put("firstName", firstName1);

        Authentication authentication1 = executeAuthenticationFilters(true, requestHeaders);
        assertAuthenticatedUserId(expectedUserId, firstName1, null, authentication1);

        requestHeaders.put("firstName", "differentFirstName");
        Authentication authentication2 = executeAuthenticationFilters(true, requestHeaders);
        assertAuthenticatedUserId(expectedUserId, firstName1, null, authentication2);
    }

    /**
     * Makes the following assertions about the given {@link Authentication}:
     * <ol>
     * <li>is not null</li>
     * <li>principal is not null</li>
     * <li>principal type is {@link org.finra.herd.app.security.SecurityUserWrapper}</li>
     * <li>principal applicationUser is not null</li>
     * <li>principal applicationUser userId equals given userId</li>
     * <li>principal applicationUser firstName equals given firstName</li>
     * <li>principal applicationUser uesrId equals given userId</li>
     * <li>principal applicationUser sessionInitTime equals given sessionInitTime</li>
     * </ol>
     * 
     * @param expectedUserId
     * @param expectedFirstName
     * @param expectedSessionInitTime
     * @param authentication {@link Authentication} to assert
     */
    private void assertAuthenticatedUserId(String expectedUserId, String expectedFirstName, Date expectedSessionInitTime, Authentication authentication)
    {
        Assert.assertNotNull("authentication is null", authentication);
        Assert.assertNotNull("authentication principal is null", authentication.getPrincipal());
        Assert.assertEquals("authentication principal type", SecurityUserWrapper.class, authentication.getPrincipal().getClass());
        SecurityUserWrapper securityUserWrapper = (SecurityUserWrapper) authentication.getPrincipal();
        ApplicationUser applicationUser = securityUserWrapper.getApplicationUser();
        Assert.assertNotNull("securityUserWrapper applicationUser is null", applicationUser);
        Assert.assertEquals("securityUserWrapper applicationUser userId", expectedUserId, applicationUser.getUserId());
        Assert.assertEquals("securityUserWrapper applicationUser firstName", expectedFirstName, applicationUser.getFirstName());
        Assert.assertEquals("securityUserWrapper applicationUser sessionInitTime", expectedSessionInitTime, applicationUser.getSessionInitTime());
    }

    /**
     * Executes {@link org.finra.herd.app.security.TrustedUserAuthenticationFilter} and {@link org.finra.herd.app.security.HttpHeaderAuthenticationFilter} in sequence with security enabled or disabled, with the given
     * request headers. Returns the final {@link Authentication} as the result of the filter executions.
     * 
     * @param isSecurityEnabled true to enable security, false otherwise
     * @param requestHeaders request headers
     * @return {@link Authentication}, may be null if filters did not put any authentication in the context.
     * @throws Exception
     */
    private Authentication executeAuthenticationFilters(Boolean isSecurityEnabled, Map<String, Object> requestHeaders) throws Exception
    {
        // Build a mock http request with the headers
        MockHttpServletRequest request = new MockHttpServletRequest();
        for (Map.Entry<String, Object> header : requestHeaders.entrySet())
        {
            request.addHeader(header.getKey(), header.getValue());
        }

        MockHttpServletResponse response = new MockHttpServletResponse();

        // Override environment with test security settings. Enable or disable security based on parameter.
        Map<String, Object> defaultSecurityEnvironmentVariables = getDefaultSecurityEnvironmentVariables();
        defaultSecurityEnvironmentVariables.put(ConfigurationValue.SECURITY_ENABLED_SPEL_EXPRESSION.getKey(), isSecurityEnabled.toString());
        modifyPropertySourceInEnvironment(defaultSecurityEnvironmentVariables);

        try
        {
            // Execute the filters
            trustedUserAuthenticationFilter.doFilter(request, response, new MockFilterChain());
            httpHeaderAuthenticationFilter.doFilter(request, response, new MockFilterChain());

            // Return the authentication in the context
            return SecurityContextHolder.getContext().getAuthentication();
        }
        finally
        {
            // Restore the environment to default in case of errors
            restorePropertySourceInEnvironment();
        }
    }
}
