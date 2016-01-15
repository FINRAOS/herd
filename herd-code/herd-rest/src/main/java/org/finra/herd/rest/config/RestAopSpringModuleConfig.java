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
package org.finra.herd.rest.config;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import org.finra.herd.service.helper.CheckAllowedMethodAdvice;

/**
 * Rest AOP Spring module configuration. This class defines specific configuration related to aspects.
 */
@Configuration
@EnableAspectJAutoProxy
@Aspect
public class RestAopSpringModuleConfig
{
    @Autowired
    private CheckAllowedMethodAdvice checkAllowedMethodAdvice;

    /**
     * A pointcut for all herd rest methods.
     */
    @Pointcut("execution(* org.finra.herd.rest.*Controller.*(..))")
    public void restMethods()
    {
        // Pointcut methods are defined by their annotation and don't have an implementation.
    }

    /**
     * Around advice that blocks the methods configured to not be available.
     *
     * @param pjp the proceeding join point.
     *
     * @return the return value of the method we are advising.
     * @throws Throwable if there were any problems executing the method.
     */
    @Around("restMethods()")
    public Object blockRestMethods(ProceedingJoinPoint pjp) throws Throwable
    {
        return checkAllowedMethodAdvice.checkNotAllowedMethods(pjp);
    }
}
