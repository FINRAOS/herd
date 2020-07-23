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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import org.finra.herd.core.ArgumentParser;

public class ToolArgumentHelper
{
    /**
     * Validate cli option and env parameter
     *
     * @param argParser             argument parser instance
     * @param option                cli option
     * @param enableEnvVariablesOpt enableEnvVariables cli option
     * @throws ParseException when neither is provided
     */
    public static void validateCliEnvArgument(ArgumentParser argParser, Option option, Option enableEnvVariablesOpt) throws ParseException
    {
        String optionName = option.getLongOpt();
        String envVarName = getEnvVarName(option);
        //check password cli input, then env vars if enableEnvVariables is true
        boolean blankCliOption = StringUtils.isBlank(argParser.getStringValue(option));
        boolean enableEnvVars = argParser.getStringValueAsBoolean(enableEnvVariablesOpt, false);
        boolean blankEnvVars = StringUtils.isBlank(ToolArgumentHelper.getEnvValue(option));
        if (blankCliOption && !enableEnvVars || blankCliOption && blankEnvVars)
        {
            {
                throw new ParseException(String.format("Either %s or enableEnvVariables with env variable %s is required.", optionName, envVarName));
            }

        }

    }

    /**
     * Retrieves the user provided password, fetch cli provided password first, otherwise fetch environment provided password if enableEnvVariables option is
     * true
     *
     * @param argParser             argument parser instance
     * @param cliOption             cli option
     * @param enableEnvVariablesOpt enableEnvVariables cli option
     * @return String for cli provided parameter value or env provided parameter value
     * @throws ParseException if the value of the argument is an invalid boolean value
     */
    public static String getCliEnvArgumentValue(ArgumentParser argParser, Option cliOption, Option enableEnvVariablesOpt) throws ParseException
    {
        String cliValue = argParser.getStringValue(cliOption);
        String envValue = getEnvValue(cliOption);
        boolean enableEnvVars = argParser.getStringValueAsBoolean(enableEnvVariablesOpt, false);

        // return CLI provided password if exist
        if (StringUtils.isNoneBlank(cliValue))
        {
            return cliValue;

        }
        // return ENV provided password if enableEnvVariables is true and env provided password exist
        if (enableEnvVars && StringUtils.isNoneBlank(envValue))
        {
            return envValue;
        }
        return null;
    }

    private static String getEnvValue(Option option)
    {
        return System.getenv(getEnvVarName(option));
    }

    private static String getEnvVarName(Option option)
    {
        return ToolsCommonConstants.ENV_VAR_PREFIX + option.getLongOpt().toUpperCase();
    }
}
