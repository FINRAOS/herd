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

import java.nio.charset.StandardCharsets;

import javax.servlet.FilterRegistration;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;

import org.springframework.web.WebApplicationInitializer;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.filter.CharacterEncodingFilter;
import org.springframework.web.filter.DelegatingFilterProxy;
import org.springframework.web.servlet.DispatcherServlet;

import org.finra.herd.app.config.AppSpringModuleConfig;
import org.finra.herd.app.security.WebSecurityConfig;
import org.finra.herd.core.ApplicationContextHolder;
import org.finra.herd.core.config.CoreSpringModuleConfig;
import org.finra.herd.dao.config.DaoEnvSpringModuleConfig;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.rest.config.RestSpringModuleConfig;
import org.finra.herd.service.config.ServiceEnvSpringModuleConfig;
import org.finra.herd.service.config.ServiceSpringModuleConfig;
import org.finra.herd.ui.RequestLoggingFilter;
import org.finra.herd.ui.config.UiEnvSpringModuleConfig;
import org.finra.herd.ui.config.UiSpringModuleConfig;

/**
 * The WAR initializer. This replaces the majority of the web.xml configuration so things can be done programmatically for compile time syntax checking and Java
 * debugging capabilities.
 */
public class WarInitializer implements WebApplicationInitializer
{
    @Override
    public void onStartup(ServletContext servletContext) throws ServletException
    {
        // Initialize all the parts individually so an extending class can override individual methods as needed.
        initContextLoaderListener(servletContext);
        initDispatchServlet(servletContext);
        initDelegatingFilterProxy(servletContext);
        initLog4JMdcLoggingFilter(servletContext);
        initCharacterEncodingFilter(servletContext);
        initRequestLoggingFilter(servletContext);
        initServletMapping(servletContext);
    }

    /**
     * Initializes the context loader listener which bootstraps Spring and provides access to the application context.
     *
     * @param servletContext the servlet context.
     */
    protected void initContextLoaderListener(ServletContext servletContext)
    {
        // Add the context loader listener for the base (i.e. root) Spring configuration.
        // We register all our @Configuration annotated classes with the context so Spring will load all the @Bean's via these classes.
        // We also set the application context in an application context holder before "registering" so static @Bean's
        // (e.g. PropertySourcesPlaceholderConfigurer) will have access to it since they can't take advantage of autowiring or having a class be
        // ApplicationContextAware to get it.
        AnnotationConfigWebApplicationContext contextLoaderListenerContext = new AnnotationConfigWebApplicationContext();
        ApplicationContextHolder.setApplicationContext(contextLoaderListenerContext);
        contextLoaderListenerContext
            .register(CoreSpringModuleConfig.class, DaoSpringModuleConfig.class, DaoEnvSpringModuleConfig.class, ServiceSpringModuleConfig.class,
                ServiceEnvSpringModuleConfig.class, UiSpringModuleConfig.class, UiEnvSpringModuleConfig.class, RestSpringModuleConfig.class,
                AppSpringModuleConfig.class, WebSecurityConfig.class);
        servletContext.addListener(new ContextLoaderListener(contextLoaderListenerContext));
    }

    /**
     * Initializes the dispatch servlet which is used for Spring MVC REST and UI controllers.
     *
     * @param servletContext the servlet context.
     */
    protected void initDispatchServlet(ServletContext servletContext)
    {
        // Add the dispatch servlet to handle user URL's and REST URL's.
        AnnotationConfigWebApplicationContext dispatchServletContext = new AnnotationConfigWebApplicationContext();
        ServletRegistration.Dynamic servlet = servletContext.addServlet("springMvcServlet", new DispatcherServlet(dispatchServletContext));
        servlet.addMapping("/");
        servlet.setLoadOnStartup(1);
    }

    /**
     * Initializes the delegating filter proxy which is used for Spring Security.
     *
     * @param servletContext the servlet context.
     */
    protected void initDelegatingFilterProxy(ServletContext servletContext)
    {
        // Initialize and add the Spring security's delegating filter proxy to the servlet context.
        DelegatingFilterProxy delegatingFilterProxy = new DelegatingFilterProxy();
        FilterRegistration.Dynamic filterChainProxyFilterRegistration = servletContext.addFilter("filterChainProxy", delegatingFilterProxy);
        filterChainProxyFilterRegistration.addMappingForUrlPatterns(null, true, "/*");
    }

    /**
     * Initializes the Log4J MDC logging filter which provides extra diagnostic information that can be used for logging.
     *
     * @param servletContext the servlet context.
     */
    protected void initLog4JMdcLoggingFilter(ServletContext servletContext)
    {
        // Add log4j MDC logging filter.
        FilterRegistration.Dynamic log4jMdcLoggingFilter = servletContext.addFilter("log4jMdcLoggingFilter", Log4jMdcLoggingFilter.class);
        log4jMdcLoggingFilter.addMappingForUrlPatterns(null, true, "/*");
    }

    /**
     * Initializes the character encoding filter which provides UTF-8 encoding.
     *
     * @param servletContext the servlet context.
     */
    protected void initCharacterEncodingFilter(ServletContext servletContext)
    {
        // Add a filter that encodes incoming requests and outgoing responses with UTF-8 encoding.
        FilterRegistration.Dynamic filterRegistration = servletContext.addFilter("characterEncodingFilter", new CharacterEncodingFilter());
        filterRegistration.setInitParameter("encoding", StandardCharsets.UTF_8.name());
        filterRegistration.setInitParameter("forceEncoding", "true");
        filterRegistration.addMappingForUrlPatterns(null, true, "/*");
    }

    /**
     * Initializes the request logging filter that logs all incoming REST requests.
     *
     * @param servletContext the servlet context.
     */
    protected void initRequestLoggingFilter(ServletContext servletContext)
    {
        // Add a filter that logs incoming HTTP request and configure flags to enable more detailed logging.
        FilterRegistration.Dynamic filterRegistration = servletContext.addFilter("requestLoggingFilter", new RequestLoggingFilter());
        filterRegistration.addMappingForUrlPatterns(null, true, "/rest/*");
    }

    /**
     * Initializes the servlet mapping that allows the application server to serve up various static content.
     *
     * @param servletContext the servlet context.
     */
    protected void initServletMapping(ServletContext servletContext)
    {
        // The "default" Tomcat servlet will directly serve up URL matches for static assets so none of the other ones will (e.g. DispatcherServlet).
        // Activiti uses ".svg" resources.
        servletContext.getServletRegistration("default")
            .addMapping("*.html", "*.jpg", "*.png", "*.gif", "*.css", "*.js", "*.svg", "*.map", "*.yaml", "*.woff", "*.woff2", "*.ttf");
    }
}
