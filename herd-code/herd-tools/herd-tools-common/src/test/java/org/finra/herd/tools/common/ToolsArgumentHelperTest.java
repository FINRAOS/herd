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
package org.finra.herd.tools.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import org.finra.herd.core.AbstractCoreTest;
import org.finra.herd.core.ArgumentParser;
import org.finra.herd.tools.common.databridge.DataBridgeApp;

public class ToolsArgumentHelperTest extends AbstractCoreTest
{
    @Rule
    public EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Test
    public void validateCliEnvArgumentNeitherExist() throws ParseException
    {
        String[] argumentsWithEmptyOption = {};
        validateCliEnvException(argumentsWithEmptyOption);

        String[] argumentsWithEnableVarsOptFalse = {"--enableEnvVariables", "false"};
        validateCliEnvException(argumentsWithEnableVarsOptFalse);

        String[] argumentsWithEnableVarsOptButNoEnvValue = {"--enableEnvVariables", "true"};
        validateCliEnvException(argumentsWithEnableVarsOptButNoEnvValue);

        String[] argumentsWithBlankCliOptionEnableVarsOptButNoEnvValue = {"-w", "", "--enableEnvVariables", "true"};
        validateCliEnvException(argumentsWithBlankCliOptionEnableVarsOptButNoEnvValue);
    }

    private void validateCliEnvException(String[] args) throws ParseException
    {
        ArgumentParser argParser = new ArgumentParser("test");
        Option passwordOpt = argParser.addArgument("w", "password", true, "The password used for HTTPS client authentication.", false);
        Option enableEnvVariablesOpt = argParser.addArgument("E", "enableEnvVariables", true,
            "The enableEnvVariables used for HTTPS client authentication through environment provided variable.", false);
        argParser.parseArguments(args);
        try
        {
            ToolsArgumentHelper.validateCliEnvArgument(argParser, passwordOpt, enableEnvVariablesOpt);
            fail("Excepted Exception not thrown");
        }
        catch (ParseException e)
        {
            assertEquals("Either password or enableEnvVariables with env variable HERD_PASSWORD is required.", e.getMessage());
        }
    }

    @Test
    public void validateCliEnvArgumentSuccess() throws ParseException
    {
        ArgumentParser argParser = new ArgumentParser("test");
        Option passwordOpt = argParser.addArgument("w", "password", true, "The password used for HTTPS client authentication.", false);
        Option enableEnvVariablesOpt = argParser.addArgument("E", "enableEnvVariables", true,
            "The enableEnvVariables used for HTTPS client authentication through environment provided variable.", false);
        // CLI password only
        String[] argumentsWithCliOpt = {"-w", "cliPassword"};
        argParser.parseArguments(argumentsWithCliOpt);
        ToolsArgumentHelper.validateCliEnvArgument(argParser, passwordOpt, enableEnvVariablesOpt);

        // enableEnvVariables true
        environmentVariables.set("HERD_PASSWORD", "envPassword");
        String[] argumentsWithEnableVarsOpt = {"--enableEnvVariables", "true"};
        argParser.parseArguments(argumentsWithEnableVarsOpt);
        ToolsArgumentHelper.validateCliEnvArgument(argParser, passwordOpt, enableEnvVariablesOpt);
        environmentVariables.clear("HERD_PASSWORD");

        // CLI password and enableEnvVariables true
        String[] argumentsWithCliAndEnableVarsOpt = {"-w", "cliPassword", "--enableEnvVariables", "true"};
        argParser.parseArguments(argumentsWithCliAndEnableVarsOpt);
        ToolsArgumentHelper.validateCliEnvArgument(argParser, passwordOpt, enableEnvVariablesOpt);
    }

    @Test
    public void getCliEnvArgumentValueNonBlackCliOption() throws ParseException
    {
        String[] argumentsWithCliOpt = {"-w", "cliPassword"};
        validateCliEnvArgument(argumentsWithCliOpt, "cliPassword");
    }

    @Test
    public void getCliEnvArgumentValueBlackCliOption() throws ParseException
    {
        String[] argumentsWithCliOpt = {"-w", ""};
        validateCliEnvArgument(argumentsWithCliOpt, null);
    }

    @Test
    public void getCliEnvArgumentValueEnableEnvVarTrueNonBlackEnvOption() throws ParseException
    {
        environmentVariables.set("HERD_PASSWORD", "envPassword");
        String[] argumentsWithCliOpt = {"--enableEnvVariables", "true"};
        validateCliEnvArgument(argumentsWithCliOpt, "envPassword");
        environmentVariables.clear("HERD_PASSWORD");
    }

    @Test
    public void getCliEnvArgumentValueEnableEnvVarFalse() throws ParseException
    {
        environmentVariables.set("HERD_PASSWORD", "envPassword");
        String[] argumentsWithCliOpt = {"--enableEnvVariables", "false"};
        validateCliEnvArgument(argumentsWithCliOpt, null);
        environmentVariables.clear("HERD_PASSWORD");
    }

    @Test
    public void getCliEnvArgumentValueEnableEnvVarTrueBlankEnvVar() throws ParseException
    {
        environmentVariables.set("HERD_PASSWORD", "");
        String[] argumentsWithCliOpt = {"--enableEnvVariables", "true"};
        validateCliEnvArgument(argumentsWithCliOpt, null);
        environmentVariables.clear("HERD_PASSWORD");
    }

    private void validateCliEnvArgument(String[] args, String expectedPassword) throws ParseException
    {
        ArgumentParser argParser = new ArgumentParser("test");
        Option passwordOpt = argParser.addArgument("w", "password", true, "The password used for HTTPS client authentication.", false);
        Option enableEnvVariablesOpt = argParser.addArgument("E", "enableEnvVariables", true,
            "The enableEnvVariables used for HTTPS client authentication through environment provided variable.", false);
        // CLI password only
        argParser.parseArguments(args);
        assertEquals(expectedPassword, ToolsArgumentHelper.getCliEnvArgumentValue(argParser, passwordOpt, enableEnvVariablesOpt));
    }
}
