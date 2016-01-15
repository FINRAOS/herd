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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockFilterConfig;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;

import org.finra.herd.model.dto.ConfigurationValue;

/**
 * This class tests various functionality within the Log4j Mdc logging Filter.
 */
public class Log4jMdcLoggingFilterTest extends AbstractAppTest
{
    @Test
    public void testLoggingWithUser() throws Exception
    {
        // Perform trusted user login.
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

            // Apply user logging filter.
            Log4jMdcLoggingFilter filterUnderTest = new Log4jMdcLoggingFilter();
            filterUnderTest.init(new MockFilterConfig());
            MockFilterChain mockChain = new MockFilterChain();
            MockHttpServletRequest req = new MockHttpServletRequest();
            MockHttpServletResponse rsp = new MockHttpServletResponse();

            filterUnderTest.doFilter(req, rsp, mockChain);

            filterUnderTest.destroy();
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testLoggingNoUser() throws Exception
    {
        invalidateApplicationUser(null);

        // Apply user logging filter.
        Log4jMdcLoggingFilter filterUnderTest = new Log4jMdcLoggingFilter();
        filterUnderTest.init(new MockFilterConfig());
        MockFilterChain mockChain = new MockFilterChain();
        MockHttpServletRequest req = new MockHttpServletRequest();
        MockHttpServletResponse rsp = new MockHttpServletResponse();

        filterUnderTest.doFilter(req, rsp, mockChain);

        filterUnderTest.destroy();
    }

    @Test
    public void testLoggingAnonymousUser() throws Exception
    {
        invalidateApplicationUser(null);

        // Apply AnonymousAuthenticationFilter
        AnonymousAuthenticationFilter anonymousAuthenticationFilter = new AnonymousAuthenticationFilter("AnonymousFilterKey");
        anonymousAuthenticationFilter.doFilter(new MockHttpServletRequest(), new MockHttpServletResponse(), new MockFilterChain());
        
        // Apply user logging filter.
        Log4jMdcLoggingFilter filterUnderTest = new Log4jMdcLoggingFilter();
        filterUnderTest.init(new MockFilterConfig());
        MockFilterChain mockChain = new MockFilterChain();
        MockHttpServletRequest req = new MockHttpServletRequest();
        MockHttpServletResponse rsp = new MockHttpServletResponse();

        filterUnderTest.doFilter(req, rsp, mockChain);

        filterUnderTest.destroy();
    }
}
