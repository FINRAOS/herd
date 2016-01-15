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
package org.finra.herd.core;

import org.springframework.context.ApplicationContext;

/**
 * A class that can statically hold a Spring application context for later easy retrieval. It is recommended that you store the context in this holder once the
 * context is created in a WebApplicationInitializer. It can then be easily retrieved from a JavaConfig @Bean annotated static method that returns a Bean
 * Factory Post Processor (BFPP) such as a property source place holder configurer.
 * <p/>
 * Note that this class should only be used when there is no easy way to get a hold of the context using normal means such as autowiring and making a class
 * ApplicationContextAware.
 */
public class ApplicationContextHolder
{
    private static ApplicationContext applicationContext;

    public static ApplicationContext getApplicationContext()
    {
        return applicationContext;
    }

    public static void setApplicationContext(ApplicationContext applicationContext)
    {
        ApplicationContextHolder.applicationContext = applicationContext;
    }
}
