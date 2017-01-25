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
package org.finra.herd.service.credstash;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;


/**
 * Class to get and store the environment configuration for credstash.
 */
public class EnvConfig
{
    public static final String CREDSTASH_ENV_PREFIX = "CRED_";

    public static final String PROXY = "PROXY";

    public static final String PORT = "PORT";

    private Map<String, String> env;

    private void init()
    {
        if (env == null)
        {
            env = getEnvVars();
        }
    }

    /**
     * Method to get the environment variables
     *
     * @return a string value pair map
     */
    protected Map<String, String> getEnvVars()
    {
        Map<String, String> envVars = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, String> sysEnv = System.getenv();

        final String[] envKeys = {CREDSTASH_ENV_PREFIX + PROXY, CREDSTASH_ENV_PREFIX + PORT};

        for (String key : envKeys)
        {
            if (sysEnv.containsKey(key))
            {
                envVars.put(key, sysEnv.get(key));
            }
        }

        return envVars;
    }

    public String getProxy()
    {
        return getProp(CREDSTASH_ENV_PREFIX + PROXY);
    }

    public String getPort()
    {
        return getProp(CREDSTASH_ENV_PREFIX + PORT);
    }

    public boolean hasProxyEnv()
    {
        return StringUtils.isNoneBlank(getProxy(), getPort());
    }

    private String getProp(String key)
    {
        init();
        return StringUtils.trim(env.get(key));
    }
}
