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
package org.finra.herd.core.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.aop.interceptor.SimpleAsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.task.TaskExecutor;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.core.helper.SecurityManagerHelper;
import org.finra.herd.model.api.xml.BuildInformation;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * Core Spring module configuration.
 */
@Configuration
// Component scan all packages, but exclude the configuration ones since they are explicitly specified.
@ComponentScan(value = "org.finra.herd.core",
    excludeFilters = @ComponentScan.Filter(type = FilterType.REGEX, pattern = "org\\.finra\\.herd\\.core\\.config\\..*"))
@PropertySource("classpath:herdBuildInfo.properties")
@EnableAsync
@EnableAspectJAutoProxy(proxyTargetClass = true)
public class CoreSpringModuleConfig implements AsyncConfigurer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CoreSpringModuleConfig.class);

    @Autowired
    private Environment environment;

    @Autowired
    private ConfigurationHelper configurationHelper;

    /**
     * Initializer for this class. Logs whether the security manager is enabled or not.
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
        // Use the environment to get access to the herdBuildInfo.properties properties configured above using the @PropertySource annotation.
        // Another way to do this would be to use "@Value("${<property>}")" and ensure a static @Bean that returned a PropertySourcesPlaceholderConfigurer
        // was configured. Using the environment is the preferred way due to its extended capabilities.
        BuildInformation buildInformation = new BuildInformation();
        buildInformation.setBuildDate(environment.getProperty("build.date"));
        buildInformation.setBuildNumber(environment.getProperty("build.number"));
        buildInformation.setBuildUser(environment.getProperty("build.user"));

        // Log useful build and system property information.
        LOGGER.info(String.format("Build Information: {buildNumber=%s, buildDate=%s, buildUser=%s}", buildInformation.getBuildNumber(),
            buildInformation.getBuildDate(), buildInformation.getBuildUser()));
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

    /**
     * Returns an Async "task" executor which is also a normal "executor".It is also being used by the "@EnableAsync" annotation and the fact that this class
     * implements AsyncConfigurer. That way, all methods annotated with "@Async" will be executed asynchronously by this executor.
     *
     * @return the async task executor.
     */
    @Override
    @Bean // This will call the "initialize" method of the ThreadPoolTaskExecutor automatically.
    public TaskExecutor getAsyncExecutor()
    {
        // Create a Spring thread pool "task" executor that is backed by a JDK Thread Pool Executor.
        // Use the environment to make the key thread pool parameters configurable although changing them would require a server restart.
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(configurationHelper.getProperty(ConfigurationValue.THREAD_POOL_CORE_POOL_SIZE, Integer.class));
        executor.setMaxPoolSize(configurationHelper.getProperty(ConfigurationValue.THREAD_POOL_MAX_POOL_SIZE, Integer.class));
        executor.setKeepAliveSeconds(configurationHelper.getProperty(ConfigurationValue.THREAD_POOL_KEEP_ALIVE_SECS, Integer.class));
        executor.setQueueCapacity(configurationHelper.getProperty(ConfigurationValue.THREAD_POOL_QUEUE_CAPACITY, Integer.class));
        return executor;
    }

    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler()
    {
        // In case any @Async methods return "void" and not a "Future", this exception handler will handle those cases since the caller has not way to access
        // the exception without a "Future". Just use an out-of-the-box Spring handler that logs those exceptions.
        return new SimpleAsyncUncaughtExceptionHandler();
    }

    @Bean
    public SpelExpressionParser spelExpressionParser()
    {
        return new SpelExpressionParser();
    }
}
