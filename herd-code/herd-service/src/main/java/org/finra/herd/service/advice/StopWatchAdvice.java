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
package org.finra.herd.service.advice;

import java.lang.reflect.Method;

import org.apache.commons.lang3.time.StopWatch;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.core.SuppressLogging;

@Component
@Aspect
public class StopWatchAdvice extends AbstractServiceAdvice
{
    // TODO: Change logger to use the class instead of string.
    // The logger name is purposefully using the legacy name of "StopWatchAdvice" due to external monitoring depending on the legacy name.
    private static final Logger LOGGER = LoggerFactory.getLogger("org.finra.herd.core.StopWatchAdvice");

    /**
     * Around advice that logs methods times for all service methods.
     *
     * @param pjp the proceeding join point.
     *
     * @return the return value of the method we are advising.
     * @throws Throwable if there were any problems executing the method.
     */
    @Around("serviceMethods()")
    public Object logMethodTime(ProceedingJoinPoint pjp) throws Throwable
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

        // Only keep a stop watch if the class and method aren't suppressing logging and the log level is info.
        if ((AnnotationUtils.findAnnotation(targetClass, SuppressLogging.class) == null) &&
            (AnnotationUtils.findAnnotation(targetMethod, SuppressLogging.class) == null) && (LOGGER.isInfoEnabled()))
        {
            // Start the stop watch.
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            // Proceed to the join point (i.e. call the method and let it return).
            Object returnValue = pjp.proceed();

            // Log the duration.
            long durationMilliseconds = stopWatch.getTime();
            LOGGER.info("javaMethod=\"{}.{}\" javaMethodDurationTimeInMilliseconds={} javaMethodDurationTimeFormatted=\"{}\"", targetClass.getName(),
                targetMethodSignature.getName(), durationMilliseconds, HerdDateUtils.formatDuration(durationMilliseconds));

            // Return the method return value.
            return returnValue;
        }
        else
        {
            // Invoke the method normally.
            return pjp.proceed();
        }
    }
}
