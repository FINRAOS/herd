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
package org.finra.dm.app;

import javax.servlet.ServletContext;

import org.junit.Test;

/**
 * Tests the WarInitializer class.
 */
public class WarInitializerTest
{
    @Test
    public void testOnStartup() throws Exception
    {
        WarInitializer warInitializer = new WarInitializer();

        // Create a mock servlet context.
        ServletContext servletContext = new ExtendedMockServletContext();

        // Call the various init methods of the WAR initializer to make sure they don't throw exceptions.
        // Since everything is mocked that the WAR initializer modifies, there's no real way to validate it so we'll consider the methods not throwing
        // an exception a successful test case.

        // Note that we are NOT calling the initContextLoaderListener method because the application JUnits will set the application context for us.
        // If we re-initialize it here, other JUnits could fail.
        warInitializer.initDispatchServlet(servletContext);
        warInitializer.initDelegatingFilterProxy(servletContext);
        warInitializer.initLog4JMdcLoggingFilter(servletContext);
        warInitializer.initCharacterEncodingFilter(servletContext);
        warInitializer.initRequestLoggingFilter(servletContext);
        warInitializer.initServletMapping(servletContext);
        warInitializer.initActiviti(servletContext);
    }
}
