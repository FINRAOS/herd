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
package org.finra.herd.tools.access.validator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;

import org.junit.Test;
import org.springframework.context.ApplicationContext;

import org.finra.herd.model.api.xml.BuildInformation;
import org.finra.herd.tools.common.ToolsCommonConstants;
import org.finra.herd.tools.common.databridge.DataBridgeApp;

public class AccessValidatorAppTest extends AbstractAccessValidatorTest
{
    private AccessValidatorApp accessValidatorApp = new AccessValidatorApp()
    {
        ApplicationContext createApplicationContext()
        {
            return applicationContext;
        }
    };

    @Test
    public void testGoMissingOptionalParameters() throws Exception
    {
        String[] arguments = {};

        // We are expecting this to fail with a FileNotFoundException.
        runApplicationAndCheckReturnValue(accessValidatorApp, arguments, new FileNotFoundException());
    }

    @Test
    public void testGoSuccess() throws Exception
    {
        String[] arguments = {"--properties", PROPERTIES_FILE_PATH};

        // We are expecting this to fail with a FileNotFoundException.
        runApplicationAndCheckReturnValue(accessValidatorApp, arguments, new FileNotFoundException());
    }

    @Test
    public void testGoUnknownParameter() throws Exception
    {
        String[] arguments = {"--unknown"};

        // We are expecting this to fail.
        runApplicationAndCheckReturnValue(accessValidatorApp, arguments, ToolsCommonConstants.ReturnValue.FAILURE);
    }

    @Test
    public void testParseCommandLineArgumentsHelpOpt()
    {
        String output = runTestGetSystemOut(() -> {
            String[] arguments = {"--help"};
            assertEquals(ToolsCommonConstants.ReturnValue.SUCCESS, accessValidatorApp.parseCommandLineArguments(arguments, applicationContext));
        });

        assertTrue("Incorrect usage information returned.", output.startsWith("usage: " + AccessValidatorApp.APPLICATION_NAME));
    }

    @Test
    public void testParseCommandLineArgumentsVersionOpt()
    {
        String output = runTestGetSystemOut(() -> {
            String[] arguments = {"--version"};
            assertEquals(ToolsCommonConstants.ReturnValue.SUCCESS, accessValidatorApp.parseCommandLineArguments(arguments, applicationContext));
        });

        BuildInformation buildInformation = applicationContext.getBean(BuildInformation.class);

        assertEquals("output", String
            .format(DataBridgeApp.BUILD_INFO_STRING_FORMAT, buildInformation.getBuildDate(), buildInformation.getBuildNumber(), buildInformation.getBuildOs(),
                buildInformation.getBuildUser()), output);
    }
}
