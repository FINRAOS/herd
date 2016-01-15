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

import java.util.Collection;
import java.util.EnumSet;
import java.util.EventListener;
import java.util.Map;
import java.util.Set;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterRegistration;
import javax.servlet.MultipartConfigElement;
import javax.servlet.Servlet;
import javax.servlet.ServletRegistration;
import javax.servlet.ServletSecurityElement;

import org.springframework.mock.web.MockServletContext;

/**
 * A mock servlet context that extends Spring's mock that doesn't throw unsupported operation exceptions.
 */
public class ExtendedMockServletContext extends MockServletContext
{
    @Override
    public <T extends EventListener> void addListener(T t)
    {
    }

    @Override
    public ServletRegistration getServletRegistration(String servletName)
    {
        return new ServletRegistration()
        {
            @Override
            public Set<String> addMapping(String... urlPatterns)
            {
                return null;
            }

            @Override
            public Collection<String> getMappings()
            {
                return null;
            }

            @Override
            public String getRunAsRole()
            {
                return null;
            }

            @Override
            public String getName()
            {
                return null;
            }

            @Override
            public String getClassName()
            {
                return null;
            }

            @Override
            public boolean setInitParameter(String name, String value)
            {
                return false;
            }

            @Override
            public String getInitParameter(String name)
            {
                return null;
            }

            @Override
            public Set<String> setInitParameters(Map<String, String> initParameters)
            {
                return null;
            }

            @Override
            public Map<String, String> getInitParameters()
            {
                return null;
            }
        };
    }

    @Override
    public FilterRegistration.Dynamic addFilter(String filterName, Filter filter)
    {
        return new FilterRegistration.Dynamic()
        {

            @Override
            public void setAsyncSupported(boolean isAsyncSupported)
            {
            }

            @Override
            public void addMappingForServletNames(EnumSet<DispatcherType> dispatcherTypes, boolean isMatchAfter, String... servletNames)
            {
            }

            @Override
            public Collection<String> getServletNameMappings()
            {
                return null;
            }

            @Override
            public void addMappingForUrlPatterns(EnumSet<DispatcherType> dispatcherTypes, boolean isMatchAfter, String... urlPatterns)
            {
            }

            @Override
            public Collection<String> getUrlPatternMappings()
            {
                return null;
            }

            @Override
            public String getName()
            {
                return null;
            }

            @Override
            public String getClassName()
            {
                return null;
            }

            @Override
            public boolean setInitParameter(String name, String value)
            {
                return false;
            }

            @Override
            public String getInitParameter(String name)
            {
                return null;
            }

            @Override
            public Set<String> setInitParameters(Map<String, String> initParameters)
            {
                return null;
            }

            @Override
            public Map<String, String> getInitParameters()
            {
                return null;
            }
        };
    }

    @Override
    public FilterRegistration.Dynamic addFilter(String filterName, Class<? extends Filter> filterClass) {
        return new FilterRegistration.Dynamic()
        {

            @Override
            public void setAsyncSupported(boolean isAsyncSupported)
            {
            }

            @Override
            public void addMappingForServletNames(EnumSet<DispatcherType> dispatcherTypes, boolean isMatchAfter, String... servletNames)
            {
            }

            @Override
            public Collection<String> getServletNameMappings()
            {
                return null;
            }

            @Override
            public void addMappingForUrlPatterns(EnumSet<DispatcherType> dispatcherTypes, boolean isMatchAfter, String... urlPatterns)
            {
            }

            @Override
            public Collection<String> getUrlPatternMappings()
            {
                return null;
            }

            @Override
            public String getName()
            {
                return null;
            }

            @Override
            public String getClassName()
            {
                return null;
            }

            @Override
            public boolean setInitParameter(String name, String value)
            {
                return false;
            }

            @Override
            public String getInitParameter(String name)
            {
                return null;
            }

            @Override
            public Set<String> setInitParameters(Map<String, String> initParameters)
            {
                return null;
            }

            @Override
            public Map<String, String> getInitParameters()
            {
                return null;
            }
        };
    }
    
    @Override
    public ServletRegistration.Dynamic addServlet(String servletName, Servlet servlet)
    {
        return new ServletRegistration.Dynamic()
        {
            @Override
            public void setLoadOnStartup(int loadOnStartup)
            {
            }

            @Override
            public Set<String> setServletSecurity(ServletSecurityElement constraint)
            {
                return null;
            }

            @Override
            public void setMultipartConfig(MultipartConfigElement multipartConfig)
            {
            }

            @Override
            public void setRunAsRole(String roleName)
            {
            }

            @Override
            public void setAsyncSupported(boolean isAsyncSupported)
            {
            }

            @Override
            public Set<String> addMapping(String... urlPatterns)
            {
                return null;
            }

            @Override
            public Collection<String> getMappings()
            {
                return null;
            }

            @Override
            public String getRunAsRole()
            {
                return null;
            }

            @Override
            public String getName()
            {
                return null;
            }

            @Override
            public String getClassName()
            {
                return null;
            }

            @Override
            public boolean setInitParameter(String name, String value)
            {
                return false;
            }

            @Override
            public String getInitParameter(String name)
            {
                return null;
            }

            @Override
            public Set<String> setInitParameters(Map<String, String> initParameters)
            {
                return null;
            }

            @Override
            public Map<String, String> getInitParameters()
            {
                return null;
            }
        };
    }
}
