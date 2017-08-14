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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.Command;
import org.finra.herd.core.MockProceedingJoinPoint;
import org.finra.herd.core.SuppressLogging;
import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.service.AbstractServiceTest;

public class StopWatchAdviceTest extends AbstractServiceTest
{
    @InjectMocks
    private StopWatchAdvice stopWatchAdvice;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testLogMethodTime() throws Throwable
    {
        // Mock a join point of the method call.
        ProceedingJoinPoint joinPoint = getMockedProceedingJoinPoint(new StopWatchAdviceTest(), StopWatchAdviceTest.class.getDeclaredMethod("mockMethod"));

        // Call the method under test.
        stopWatchAdvice.logMethodTime(joinPoint);
    }

    @Test
    public void testLogMethodTimeTargetMethodIsInterface() throws Throwable
    {
        // Mock a join point of the method call.
        ProceedingJoinPoint joinPoint = getMockedProceedingJoinPoint(new Command()
        {
            @Override
            public void execute() throws Exception
            {
                return;
            }
        }, Command.class.getDeclaredMethod("execute"));

        // Call the method under test.
        stopWatchAdvice.logMethodTime(joinPoint);
    }

    @Test
    public void testLogMethodTimeTargetMethodTargetClassSuppressLogging() throws Throwable
    {
        // Mock a join point of the method call.
        ProceedingJoinPoint joinPoint =
            getMockedProceedingJoinPoint(new MockClassThatSuppressLogging(), MockClassThatSuppressLogging.class.getDeclaredMethod("mockMethod"));

        // Call the method under test.
        stopWatchAdvice.logMethodTime(joinPoint);
    }

    @Test
    public void testLogMethodTimeTargetMethodTargetMethodSuppressLogging() throws Throwable
    {
        // Mock a join point of the method call.
        ProceedingJoinPoint joinPoint =
            getMockedProceedingJoinPoint(new StopWatchAdviceTest(), StopWatchAdviceTest.class.getDeclaredMethod("mockMethodThatSuppressLogging"));

        // Call the method under test.
        stopWatchAdvice.logMethodTime(joinPoint);
    }

    @Test
    public void testLogMethodTimeWithInfoLoggingDisabled() throws Throwable
    {
        // Mock a join point of the method call.
        ProceedingJoinPoint joinPoint = getMockedProceedingJoinPoint(new StopWatchAdviceTest(), StopWatchAdviceTest.class.getDeclaredMethod("mockMethod"));

        // Get the logger and the current logger level.
        LogLevel origLogLevel = getLogLevel("org.finra.herd.core.StopWatchAdvice");

        // Set logging level to OFF.
        setLogLevel("org.finra.herd.core.StopWatchAdvice", LogLevel.OFF);

        // Run the test and reset the logging level back to the original value.
        try
        {
            // Call the method under test.
            stopWatchAdvice.logMethodTime(joinPoint);
        }
        finally
        {
            setLogLevel("org.finra.herd.core.StopWatchAdvice", origLogLevel);
        }
    }

    @Test
    public void testLogMethodTimeWithInfoLoggingEnabled() throws Throwable
    {
        // Mock a join point of the method call.
        ProceedingJoinPoint joinPoint = getMockedProceedingJoinPoint(new StopWatchAdviceTest(), StopWatchAdviceTest.class.getDeclaredMethod("mockMethod"));

        // Get the logger and the current logger level.
        LogLevel origLogLevel = getLogLevel("org.finra.herd.core.StopWatchAdvice");

        // Set logging level to INFO.
        setLogLevel("org.finra.herd.core.StopWatchAdvice", LogLevel.INFO);

        // Run the test and reset the logging level back to the original value.
        try
        {
            // Call the method under test.
            stopWatchAdvice.logMethodTime(joinPoint);
        }
        finally
        {
            setLogLevel("org.finra.herd.core.StopWatchAdvice", origLogLevel);
        }
    }

    /**
     * Creates and returns a mocked join point of the method call.
     *
     * @param targetObject the target object
     * @param method the method
     *
     * @return the mocked ProceedingJoinPoint
     */
    private ProceedingJoinPoint getMockedProceedingJoinPoint(Object targetObject, Method method) throws Exception
    {
        ProceedingJoinPoint joinPoint = mock(ProceedingJoinPoint.class);
        MethodSignature methodSignature = mock(MethodSignature.class);
        when(joinPoint.getTarget()).thenReturn(targetObject);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(methodSignature.getName()).thenReturn(method.getName());

        return joinPoint;
    }

    /**
     * Do not invoke this method. This method is a test input for reflection related tests.
     */
    private void mockMethod()
    {
    }

    /**
     * Do not invoke this method. This method is a test input for reflection related tests.
     */
    @SuppressLogging
    private void mockMethodThatSuppressLogging()
    {
    }

    /**
     * Do not invoke this class. This class is a test input for reflection related tests.
     */
    @SuppressLogging
    private class MockClassThatSuppressLogging extends MockProceedingJoinPoint
    {
        private void mockMethod()
        {
            return;
        }
    }
}
