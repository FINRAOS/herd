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
package org.finra.dm.core.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

import org.finra.dm.core.helper.SecurityManagerHelper;
import org.finra.dm.model.api.xml.BuildInformation;

/**
 * Core Spring module configuration.
 */
@Configuration
// Component scan all packages, but exclude the configuration ones since they are explicitly specified.
@ComponentScan(value = "org.finra.dm.core", excludeFilters = @ComponentScan.Filter(type = FilterType.REGEX, pattern = "org\\.finra\\.dm\\.core\\.config\\..*") )
@PropertySource("classpath:dmBuildInfo.properties")
public class CoreSpringModuleConfig
{
    private static final Logger LOGGER = Logger.getLogger(CoreSpringModuleConfig.class);

    @Autowired
    private Environment environment;

    /**
     * Initializer for this class.
     * Logs whether the security manager is enabled or not.
     */
    public CoreSpringModuleConfig()
    {
        LOGGER.debug("Security manager is " + (SecurityManagerHelper.isSecurityManagerEnabled() ? "ENABLED" : "DISABLED"));
    }

    /**
     * Gets the build information.
     *
     * @return the build information.
     */
    @Bean
    public BuildInformation buildInformation()
    {
        // Use the environment to get access to the dmBuildInfo.properties properties configured above using the @PropertySource annotation.
        // Another way to do this would be to use "@Value("${<property>}")" and ensure a static @Bean that returned a PropertySourcesPlaceholderConfigurer
        // was configured. Using the environment is the preferred way due to its extended capabilities.
        BuildInformation buildInformation = new BuildInformation();
        buildInformation.setBuildDate(environment.getProperty("build.date"));
        buildInformation.setBuildNumber(environment.getProperty("build.number"));
        buildInformation.setBuildOs(environment.getProperty("build.os"));
        buildInformation.setBuildUser(environment.getProperty("build.user"));

        // Log useful build and system property information.
        LOGGER.info(String.format("Build Information: {buildNumber=%s, buildDate=%s, buildUser=%s, buildOs=%s}", buildInformation.getBuildNumber(),
            buildInformation.getBuildDate(), buildInformation.getBuildUser(), buildInformation.getBuildOs()));
        LOGGER.info("System Properties: " +
            getSystemPropertyMap("java.version", "java.runtime.version", "java.vm.version", "java.vm.name", "java.vendor", "java.vendor.url", "java.home",
                "java.class.path", "os.name", "os.version", "os.arch", "user.name", "user.dir", "user.home", "file.separator", "path.separator"));

        return buildInformation;
    }

    /**
     * Gets the specified system properties and returns them in a map where the key is the property name and the value is the value of the property.
     *
     * @param properties the list of properties.
     *
     * @return the property map.
     */
    private Map<String, String> getSystemPropertyMap(String... properties)
    {
        Map<String, String> propertyMap = new LinkedHashMap<>();
        for (String property : properties)
        {
            propertyMap.put(property, System.getProperty(property));
        }
        return propertyMap;
    }
}
