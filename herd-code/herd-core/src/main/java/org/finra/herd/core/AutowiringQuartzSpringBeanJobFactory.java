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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.quartz.spi.TriggerFiredBundle;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;

/**
 * This SpringBeanJobFactory is used to automatically autowire Quartz objects using Spring. Please note that the SpringBeanJobFactory allows you to inject
 * properties from the scheduler context, job data map and trigger data entries into the Quartz job bean. But there is no way out of the box to inject beans
 * from the application context. Thus, we need the factory bean below that extends the SpringBeanJobFactory to add auto-wiring support.
 */
public final class AutowiringQuartzSpringBeanJobFactory extends SpringBeanJobFactory implements ApplicationContextAware
{
    private transient AutowireCapableBeanFactory beanFactory;

    @Override
    public void setApplicationContext(final ApplicationContext context)
    {
        beanFactory = context.getAutowireCapableBeanFactory();
    }

    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR",
        justification = "This is a false positive. setApplicationContext is called before this method which will ensure beanFactory is not null.")
    @Override
    protected Object createJobInstance(final TriggerFiredBundle bundle) throws Exception
    {
        final Object job = super.createJobInstance(bundle);
        beanFactory.autowireBean(job);
        return job;
    }
}
