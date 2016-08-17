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
package org.finra.herd.app.config;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.Filter;
import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.core.env.Environment;
import org.springframework.security.access.AccessDecisionManager;
import org.springframework.security.access.AccessDecisionVoter;
import org.springframework.security.access.vote.AffirmativeBased;
import org.springframework.security.access.vote.RoleVoter;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.method.configuration.GlobalMethodSecurityConfiguration;
import org.springframework.security.web.FilterChainProxy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationProvider;
import org.springframework.security.web.context.SecurityContextPersistenceFilter;

import org.finra.herd.app.security.HerdUserDetailsService;
import org.finra.herd.app.security.HttpHeaderApplicationUserBuilder;
import org.finra.herd.app.security.HttpHeaderAuthenticationFilter;
import org.finra.herd.app.security.TrustedApplicationUserBuilder;
import org.finra.herd.app.security.TrustedUserAuthenticationFilter;
import org.finra.herd.core.ApplicationContextHolder;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.Log4jOverridableConfigurer;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.ConfigurationEntity;

/**
 * This is the Spring root configuration for the web application.
 */
@Configuration
// Component scan all packages, but exclude the configuration ones since they are explicitly specified.
@ComponentScan(value = "org.finra.herd.app",
    excludeFilters = @ComponentScan.Filter(type = FilterType.REGEX, pattern = "org\\.finra\\.herd\\.app\\.config\\..*"))
@EnableGlobalMethodSecurity(securedEnabled = true)
public class AppSpringModuleConfig extends GlobalMethodSecurityConfiguration
{
    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private HerdUserDetailsService herdUserDetailsService;

    /**
     * The Log4J overridable configurer that will handle our Log4J initialization for the herd application.
     * <p/>
     * IMPORTANT: Ensure this method is static since the returned Log4jOverridableConfigurer is a bean factory post processor (BFPP). If it weren't static,
     * autowiring and injection on this @Configuration class won't work due to lifecycle issues. See "Bootstrapping" comment in @Bean annotation for more
     * details.
     *
     * @return the Log4J overridable configurer.
     */
    @Bean
    public static Log4jOverridableConfigurer log4jConfigurer()
    {
        // Access the environment using the application context holder since we're in a static method that doesn't have access to the environment in any
        // other way.
        Environment environment = ApplicationContextHolder.getApplicationContext().getEnvironment();

        // Create the Log4J configurer.
        Log4jOverridableConfigurer log4jConfigurer = new Log4jOverridableConfigurer();

        // This is the primary database override location (if present). An entry needs to be present in the database at application startup for this
        // configuration to be used.
        log4jConfigurer.setDataSource(DaoSpringModuleConfig.getHerdDataSource());
        log4jConfigurer.setTableName(ConfigurationEntity.TABLE_NAME);
        log4jConfigurer.setSelectColumn(ConfigurationEntity.COLUMN_VALUE_CLOB);
        log4jConfigurer.setWhereColumn(ConfigurationEntity.COLUMN_KEY);
        log4jConfigurer.setWhereValue(ConfigurationValue.LOG4J_OVERRIDE_CONFIGURATION.getKey());

        // This is the secondary override location (if present and if the primary location isn't present). This resource needs to be present at application
        // startup for this configuration to be used.
        log4jConfigurer.setOverrideResourceLocation(ConfigurationHelper.getProperty(ConfigurationValue.LOG4J_OVERRIDE_RESOURCE_LOCATION, environment));

        // This is the default fallback location which is bundled in the WAR and should always be present in case the override locations aren't present.
        // Note that the herd-log4j.xml file must be in the herd-war project so it is accessible on the classpath as a file. Placing it within another
        // project's JAR will result in the resource not being found as a "file".
        log4jConfigurer.setDefaultResourceLocation("classpath:herd-log4j.xml");

        // Return the Log4J configurer.
        return log4jConfigurer;
    }

    /**
     * Gets a filter chain proxy.
     *
     * @param trustedUserAuthenticationFilter the trusted user authentication filter.
     * @param httpHeaderAuthenticationFilter the HTTP header authentication filter.
     *
     * @return the filter chain proxy.
     */
    @Bean
    public FilterChainProxy filterChainProxy(final TrustedUserAuthenticationFilter trustedUserAuthenticationFilter,
        final HttpHeaderAuthenticationFilter httpHeaderAuthenticationFilter)
    {
        return new FilterChainProxy(new SecurityFilterChain()
        {
            @Override
            public boolean matches(HttpServletRequest request)
            {
                // Match all URLs.
                return true;
            }

            @Override
            public List<Filter> getFilters()
            {
                List<Filter> filters = new ArrayList<>();

                // Required filter to store session information between HTTP requests.
                filters.add(new SecurityContextPersistenceFilter());

                // Trusted user filter to bypass security based on SpEL expression environment property.
                filters.add(trustedUserAuthenticationFilter);

                // Filter that authenticates based on http headers.
                if (Boolean.valueOf(configurationHelper.getProperty(ConfigurationValue.SECURITY_HTTP_HEADER_ENABLED)))
                {
                    filters.add(httpHeaderAuthenticationFilter);
                }

                // Anonymous user filter.
                filters.add(new AnonymousAuthenticationFilter("AnonymousFilterKey"));

                return filters;
            }
        });
    }

    @Bean
    public TrustedUserAuthenticationFilter trustedUserAuthenticationFilter(AuthenticationManager authenticationManager,
        TrustedApplicationUserBuilder trustedApplicationUserBuilder)
    {
        return new TrustedUserAuthenticationFilter(authenticationManager, trustedApplicationUserBuilder);
    }

    @Bean
    public TrustedApplicationUserBuilder trustedApplicationUserBuilder()
    {
        return new TrustedApplicationUserBuilder();
    }

    @Bean
    public HttpHeaderAuthenticationFilter httpHeaderAuthenticationFilter(AuthenticationManager authenticationManager,
        HttpHeaderApplicationUserBuilder httpHeaderApplicationUserBuilder)
    {
        return new HttpHeaderAuthenticationFilter(authenticationManager, httpHeaderApplicationUserBuilder);
    }

    @Bean
    public HttpHeaderApplicationUserBuilder httpHeaderApplicationUserBuilder()
    {
        return new HttpHeaderApplicationUserBuilder();
    }

    @Bean
    @Override
    public AuthenticationManager authenticationManager()
    {
        PreAuthenticatedAuthenticationProvider authenticationProvider = new PreAuthenticatedAuthenticationProvider();
        authenticationProvider.setPreAuthenticatedUserDetailsService(herdUserDetailsService);
        List<AuthenticationProvider> providers = new ArrayList<>();
        providers.add(authenticationProvider);
        return new ProviderManager(providers);
    }

    /**
     * Overridden to remove role prefix for the role voter. The application does not require any other access decision voters in the default configuration.
     */
    /*
     * rawtypes must be suppressed because AffirmativeBased constructor takes in a raw typed list of AccessDecisionVoters
     */
    @SuppressWarnings("rawtypes")
    @Override
    protected AccessDecisionManager accessDecisionManager()
    {
        List<AccessDecisionVoter<?>> decisionVoters = new ArrayList<>();
        RoleVoter decisionVoter = new RoleVoter();
        decisionVoter.setRolePrefix("");
        decisionVoters.add(decisionVoter);
        return new AffirmativeBased(decisionVoters);
    }
}
