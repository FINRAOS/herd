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
package org.finra.herd.ui.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.ViewResolver;
import org.springframework.web.servlet.view.InternalResourceViewResolver;

/**
 * UI environment specific Spring module configuration.
 */
@Configuration
public class UiEnvSpringModuleConfig
{
    /**
     * Gets a JSP view resolver.
     *
     * @return the JSP view resolver.
     */
    @Bean
    public ViewResolver jspViewResolver()
    {
        // The JSP view resolver which means the "view" in our UI controller's "ModelAndView" return object will map to a JSP file
        // (e.g. a view name of "test" would map to "test.jsp" in the "/WEB-INF/jsp" directory.
        InternalResourceViewResolver resolver = new InternalResourceViewResolver();
        resolver.setPrefix("/WEB-INF/jsp/");
        resolver.setSuffix(".jsp");
        return resolver;
    }
}
