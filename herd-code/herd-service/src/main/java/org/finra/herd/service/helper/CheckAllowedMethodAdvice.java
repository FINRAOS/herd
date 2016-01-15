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
package org.finra.herd.service.helper;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Advice that checks and blocks the methods to be blocked in configuration.  
 */
@Component
public class CheckAllowedMethodAdvice
{
    @Autowired
    private HerdDaoHelper herdDaoHelper;

    /**
     * Checks whether the requested operation is permitted.
     *
     * @param pjp the join point.
     *
     * @return the return value of the method at the join point.
     * @throws Throwable if any errors were encountered.
     */
    @SuppressWarnings("rawtypes")
    public Object checkNotAllowedMethods(ProceedingJoinPoint pjp) throws Throwable
    {
        // Get the method name being invoked.
        Class targetClass = pjp.getTarget().getClass();
        MethodSignature targetMethodSignature = (MethodSignature) pjp.getSignature();
        String methodName = targetClass.getName() + "." + targetMethodSignature.getName();

        herdDaoHelper.checkNotAllowedMethod(methodName);
        return pjp.proceed();
    }
}