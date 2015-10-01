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
package org.finra.dm.rest.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.Marshaller;

import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.xml.MappingJackson2XmlHttpMessageConverter;
import org.springframework.http.converter.xml.MarshallingHttpMessageConverter;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.util.PathMatcher;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;
import org.springframework.web.servlet.resource.ResourceUrlProvider;
import org.springframework.web.util.UrlPathHelper;

import org.finra.dm.core.helper.ConfigurationHelper;
import org.finra.dm.dao.helper.DmCharacterEscapeHandler;
import org.finra.dm.model.dto.ConfigurationValue;

/**
 * REST Spring module configuration. This configuration doesn't use the @EnableWebMvc annotation and instead extends WebMvcConfigurationSupport so we have the
 * ability to override configuration methods that we need. One example is that we need to configure our own message converter that can perform XSD validation.
 * Note that to override non-bean defined base class methods, we need to add the "@Bean" configuration that calls those methods in this configuration for them
 * to get used. In this case, just define the bean and call it's super method to invoke the default processing.
 */
@Configuration
// Component scan all packages, but exclude the configuration ones since they are explicitly specified.
@ComponentScan(value = "org.finra.dm.rest",
    excludeFilters = @ComponentScan.Filter(type = FilterType.REGEX, pattern = "org\\.finra\\.dm\\.rest\\.config\\..*"))
@Import(RestAopSpringModuleConfig.class)
public class RestSpringModuleConfig extends WebMvcConfigurationSupport
{
    @Autowired
    private ResourcePatternResolver resourceResolver;

    @Autowired
    private DmCharacterEscapeHandler dmCharacterEscapeHandler;

    @Autowired
    private ConfigurationHelper configurationHelper;

    /**
     * We need to override the base method so this "@Bean" will get invoked and ultimately call configureMessageConverters. Otherwise, it doesn't get called.
     * This implementation doesn't do anything except call the super method.
     *
     * @return the RequestMappingHandlerAdapter.
     */
    @Bean
    @Override
    public HandlerExceptionResolver handlerExceptionResolver()
    {
        return super.handlerExceptionResolver();
    }

    /**
     * We need to override the base method so this "@Bean" will get invoked and ultimately call configureMessageConverters. Otherwise, it doesn't get called.
     * This implementation doesn't do anything except call the super method.
     *
     * @return the RequestMappingHandlerAdapter.
     */
    @Bean
    @Override
    public RequestMappingHandlerAdapter requestMappingHandlerAdapter()
    {
        return super.requestMappingHandlerAdapter();
    }

    /**
     * This is called from requestMappingHandlerAdapter to configure the message converters. We override it to configure our own converter in addition to the
     * default converters.
     *
     * @param converters the converter list we configure.
     */
    @Override
    @SuppressWarnings("rawtypes")
    protected void configureMessageConverters(List<HttpMessageConverter<?>> converters)
    {
        // Add in our custom converter first.
        converters.add(marshallingMessageConverter());

        // Add in the default converters (e.g. standard JAXB, Jackson, etc.).
        addDefaultHttpMessageConverters(converters);

        // Remove the Jackson2Xml converter since we want to use JAXB instead when we encounter "application/xml". Otherwise, the XSD auto-generated
        // classes with JAXB annotations won't get used.
        for (HttpMessageConverter httpMessageConverter : converters)
        {
            if (httpMessageConverter instanceof MappingJackson2XmlHttpMessageConverter)
            {
                converters.remove(httpMessageConverter);
                break;
            }
        }
    }

    /**
     * Gets a new marshalling HTTP message converter that is aware of our custom JAXB marshaller.
     *
     * @return the newly created message converter.
     */
    @Bean
    public MarshallingHttpMessageConverter marshallingMessageConverter()
    {
        // Return a new marshalling HTTP message converter with our custom JAXB marshaller.
        return new MarshallingHttpMessageConverter(jaxb2Marshaller(), jaxb2Marshaller());
    }

    /**
     * Gets a new JAXB marshaller that is aware of our XSD and can perform schema validation. It is also aware of all our auto-generated classes that are in the
     * org.finra.dm.model.api.xml package. Note that REST endpoints that use Java objects which are not in this package will not use this marshaller and will
     * not get schema validated which is good since they don't have an XSD.
     *
     * @return the newly created JAXB marshaller.
     */
    @Bean
    public Jaxb2Marshaller jaxb2Marshaller()
    {
        try
        {
            // Create the marshaller that is aware of our Java XSD and it's auto-generated classes.
            Jaxb2Marshaller marshaller = new Jaxb2Marshaller();
            marshaller.setPackagesToScan("org.finra.dm.model.api.xml");
            marshaller.setSchemas(resourceResolver.getResources("classpath:dm.xsd"));

            // Get the JAXB XML headers from the environment.
            String xmlHeaders = configurationHelper.getProperty(ConfigurationValue.JAXB_XML_HEADERS);

            // We need to set marshaller properties to reconfigure the XML header.
            Map<String, Object> marshallerProperties = new HashMap<>();
            marshaller.setMarshallerProperties(marshallerProperties);

            // Remove the header that JAXB will generate.
            marshallerProperties.put(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);

            // Specify the new XML headers.
            marshallerProperties.put(ConfigurationValue.JAXB_XML_HEADERS.getKey(), xmlHeaders);

            // Specify a custom character escape handler to escape XML 1.1 restricted characters.
            marshallerProperties.put(MarshallerProperties.CHARACTER_ESCAPE_HANDLER, dmCharacterEscapeHandler);

            // Return the marshaller.
            return marshaller;
        }
        catch (Exception ex)
        {
            // Throw a runtime exception instead of a checked IOException since the XSD file should be contained within our application.
            throw new IllegalArgumentException("Unable to create marshaller.", ex);
        }
    }

    @Bean
    @Override
    public RequestMappingHandlerMapping requestMappingHandlerMapping()
    {
        return super.requestMappingHandlerMapping();
    }

    @Bean
    @Override
    public ResourceUrlProvider mvcResourceUrlProvider()
    {
        return super.mvcResourceUrlProvider();
    }

    @Bean
    @Override
    public PathMatcher mvcPathMatcher()
    {
        return super.mvcPathMatcher();
    }

    @Bean
    @Override
    public UrlPathHelper mvcUrlPathHelper()
    {
        return super.mvcUrlPathHelper();
    }

    /**
     * Configure the path match by disabling suffix pattern matching.
     *
     * @param configurer the path match configurer.
     */
    @Override
    public void configurePathMatch(PathMatchConfigurer configurer)
    {
        // Turn off suffix pattern matching which will ensure REST URL's that end with periods and some other text get matched in full and not without
        // the period and the following text suffix. This is due to Spring's extension suffix matching logic that we don't need and don't want
        // (e.g. .txt could be parsed by a specific handler).
        configurer.setUseSuffixPatternMatch(false);
    }
}