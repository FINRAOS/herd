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

import org.aspectj.lang.Signature;
import org.junit.Test;

/**
 * This class tests functionality within the stop watch advice.
 */
public class StopWatchAdviceTest extends AbstractCoreTest
{
    @Test
    public void testLogMethodTime() throws Throwable
    {
        // Normal flow should log the method time.
        StopWatchAdvice.logMethodTime(new MockProceedingJoinPoint());
    }

    @Test
    public void testLogMethodTimeClassSuppressLogging() throws Throwable
    {
        // Invoke the advice which shouldn't log the method time because the class is annotated with SuppressLogging.
        StopWatchAdvice.logMethodTime(new MockProceedingJoinPointClassSuppressLogging());
    }

    @Test
    public void testLogMethodTimeMethodSuppressLogging() throws Throwable
    {
        // Invoke the advice which shouldn't log the method time because the method is annotated with SuppressLogging.
        StopWatchAdvice.logMethodTime(new MockProceedingJoinPointMethodSuppressLogging());
    }

    /**
     * This is a mock proceeding join point that has the suppress logging annotation present.
     */
    @SuppressLogging
    public class MockProceedingJoinPointClassSuppressLogging extends org.finra.dm.core.MockProceedingJoinPoint
    {
    }

    public class MockProceedingJoinPointMethodSuppressLogging extends org.finra.dm.core.MockProceedingJoinPoint
    {
        /**
         * Return our own mock method signature that can return a signature that knows how to return a method that suppresses logging.
         *
         * @return a mock method signature.
         */
        @Override
        public Signature getSignature()
        {
            return new MockMethodSignatureSuppressLogging();
        }
    }

    public class MockMethodSignatureSuppressLogging extends MockMethodSignature
    {
        /**
         * This method returns a handle to itself. This method also happens to be annotated with the suppress logging annotation.
         *
         * @return a handle to this method.
         */
        @Override
        @SuppressLogging
        public Method getMethod()
        {
            try
            {
                // Return a handle to this method.
                return this.getClass().getMethod("getMethod");
            }
            catch (Exception ex)
            {
                // We shouldn't get here since the method we're returning is this method.
                return null;
            }
        }
    }
}
