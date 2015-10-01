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

import java.lang.reflect.Method;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.annotation.AnnotationUtils;

/**
 * Advice that logs how long a method takes to run.
 */
public class StopWatchAdvice
{
    private static final Logger LOGGER = Logger.getLogger(StopWatchAdvice.class);

    /**
     * Logs the time it takes to execute the method at the join point if the class or method isn't annotated with SuppressLogging and if the log level is set to
     * info.
     *
     * @param pjp the join point.
     *
     * @return the return value of the method at the join point.
     * @throws Throwable if any errors were encountered.
     */
    @SuppressWarnings("rawtypes")
    public static Object logMethodTime(ProceedingJoinPoint pjp) throws Throwable
    {
        // Get the target class being called.
        Class targetClass = pjp.getTarget().getClass();

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
            (AnnotationUtils.findAnnotation(targetMethod, SuppressLogging.class) == null) &&
            (LOGGER.isInfoEnabled()))
        {
            // Start the stop watch.
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            // Proceed to the join point (i.e. call the method and let it return).
            Object returnValue = pjp.proceed();

            // Log the duration.
            LOGGER.info(
                "Method " + targetClass.getName() + "." + targetMethodSignature.getName() + " took " + DmDateUtils.formatDuration(stopWatch.getTime(), true) +
                    ".");

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