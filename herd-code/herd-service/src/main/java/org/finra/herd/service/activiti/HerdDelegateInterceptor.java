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
package org.finra.herd.service.activiti;

import org.activiti.engine.impl.delegate.DelegateInvocation;
import org.activiti.engine.impl.interceptor.DelegateInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.stereotype.Component;

import org.finra.herd.service.activiti.task.BaseJavaDelegate;

/**
 * A custom herd Activiti delegate interceptor that allows us to autowire Spring beans onto Java delegate tasks.
 */
@Component
public class HerdDelegateInterceptor implements DelegateInterceptor
{
    @Autowired
    private AutowireCapableBeanFactory autowireCapableBeanFactory;

    @Override
    public void handleInvocation(DelegateInvocation invocation) throws Exception
    {
        // The delegate tasks are not spring managed, but we need Spring beans wired in.
        // So if the invocation target is a sub-class of BaseJavaDelegate, autowire the Spring beans.
        if (invocation.getTarget() instanceof BaseJavaDelegate)
        {
            BaseJavaDelegate javaDelegate = (BaseJavaDelegate) invocation.getTarget();
            if (!javaDelegate.isSpringInitialized())
            {
                autowireCapableBeanFactory.autowireBean(javaDelegate);
                javaDelegate.setSpringInitialized(true);
            }
        }

        invocation.proceed();
    }
}
