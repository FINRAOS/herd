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
package org.finra.dm.core;

import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

/**
 * A Spring annotation config context loader that stores the application context in an application context holder. This can be used for test cases that have
 * static @Configuration @Bean methods which require access to the application context.
 */
public class ContextHolderContextLoader extends AnnotationConfigContextLoader
{
    @Override
    protected void prepareContext(GenericApplicationContext context)
    {
        // Set the application context in the context holder for access by static @Bean methods.
        ApplicationContextHolder.setApplicationContext(context);

        // Perform standard functionality.
        super.prepareContext(context);
    }
}
