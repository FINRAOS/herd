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
package org.finra.herd.model;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.finra.herd.model.AlreadyExistsException;

/**
 * Tests the AlreadyExistsException class.
 */
public class AlreadyExistsExceptionTest
{
    private static final String TEST_MESSAGE_1 = "message1";
    private static final String TEST_MESSAGE_2 = "message2";

    @Test
    public void testExceptionNoArgConstructor() throws Exception
    {
        AlreadyExistsException exception = new AlreadyExistsException();
        assertTrue(exception.getMessage() == null);
    }

    @Test
    public void testExceptionMessageConstructor() throws Exception
    {
        AlreadyExistsException exception = new AlreadyExistsException(TEST_MESSAGE_1);
        assertTrue(exception.getMessage().equals(TEST_MESSAGE_1));
    }

    @Test
    public void testExceptionMessageAndThrowableConstructor() throws Exception
    {
        Exception exception = new Exception(TEST_MESSAGE_2);
        AlreadyExistsException alreadyExistsException = new AlreadyExistsException(TEST_MESSAGE_1, exception);
        assertTrue(alreadyExistsException.getMessage().equals(TEST_MESSAGE_1));
        assertTrue(alreadyExistsException.getCause().getMessage().equals(TEST_MESSAGE_2));
    }

    @Test
    public void testExceptionThrowableConstructor() throws Exception
    {
        Exception exception = new Exception(TEST_MESSAGE_2);
        AlreadyExistsException alreadyExistsException = new AlreadyExistsException(exception);
        assertTrue(alreadyExistsException.getCause().getMessage().equals(TEST_MESSAGE_2));
    }
}
