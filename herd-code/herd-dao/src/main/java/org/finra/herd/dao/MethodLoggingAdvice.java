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
package org.finra.herd.dao;

import java.lang.reflect.Method;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;

import org.finra.herd.core.SuppressLogging;

@Component
@Aspect
public class MethodLoggingAdvice
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodLoggingAdvice.class);

    /**
     * A pointcut for all herd DAO operations methods.
     */
    @Pointcut("execution(* org.finra.herd.dao.*Operations.*(..))")
    protected void operationsMethods()
    {
        // Pointcut methods are defined by their annotation and don't have an implementation.
    }

    /**
     * Around advice that logs methods being invoked for all DAO operations methods.
     *
     * @param pjp the proceeding join point.
     *
     * @return the return value of the method we are advising.
     * @throws Throwable if there were any problems executing the method.
     */
    @Around("operationsMethods()")
    public Object logMethodBeingInvoked(ProceedingJoinPoint pjp) throws Throwable
    {
        // Get the target class being called.
        Class<?> targetClass = pjp.getTarget().getClass();

        // Get the target method being called.
        MethodSignature targetMethodSignature = (MethodSignature) pjp.getSignature();
        Method targetMethod = targetMethodSignature.getMethod();
        if (targetMethod.getDeclaringClass().isInterface())
        {
            // Get the underlying implementation if we are given an interface.
            targetMethod = pjp.getTarget().getClass().getMethod(pjp.getSignature().getName(), targetMethod.getParameterTypes());
        }

        // Only log the method if the class and method aren't suppressing logging and the log level is debug.
        if ((AnnotationUtils.findAnnotation(targetClass, SuppressLogging.class) == null) &&
            (AnnotationUtils.findAnnotation(targetMethod, SuppressLogging.class) == null) && (LOGGER.isDebugEnabled()))
        {
            LOGGER.debug("javaMethod=\"{}.{}\"", targetClass.getName(), targetMethodSignature.getName());
        }

        // Proceed to the join point (i.e. call the method and let it return).
        return pjp.proceed();
    }
}