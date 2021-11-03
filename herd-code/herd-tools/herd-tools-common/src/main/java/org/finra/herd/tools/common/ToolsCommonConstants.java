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

/**
 * Common constants for all tools.
 */
public final class ToolsCommonConstants
{
    /**
     * The format for the build information string for the herd tool applications.
     */
    public static final String BUILD_INFO_STRING_FORMAT = "buildDate: %s\nbuildNumber: %s\nbuildUser: %s";

    /**
     * Default number of threads to be used by the Amazon S3 TransferManager.
     */
    public static final Integer DEFAULT_THREADS = 10;

    /**
     * The location of Log4J configuration.
     */
    public static final String LOG4J_CONFIG_LOCATION = "classpath:herd-console-info-log4j2.xml";

    public static final String ENV_VAR_PREFIX = "HERD_";

    private ToolsCommonConstants()
    {
        // Prevent classes from instantiating.
    }

    /**
     * The list of possible return values for the herd tool applications.
     */
    public enum ReturnValue
    {
        SUCCESS(0),
        FAILURE(1);

        private int returnCode;

        private ReturnValue(int returnCode)
        {
            this.returnCode = returnCode;
        }

        public int getReturnCode()
        {
            return returnCode;
        }
    }
}
