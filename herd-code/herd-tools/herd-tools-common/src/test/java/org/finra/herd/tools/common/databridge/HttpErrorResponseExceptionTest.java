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
package org.finra.herd.tools.common.databridge;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit tests for UploaderApp class.
 */
public class HttpErrorResponseExceptionTest extends AbstractDataBridgeTest
{
    final int testStatusCode = Integer.MAX_VALUE;
    final String testStatusDescription = "Test Status Description";
    final String testResponseMessage = "Test Response Message";
    final String testMessage = "Test Message";
    final Throwable testCause = new Throwable();

    @Test
    public void testHttpErrorResponseException()
    {
        HttpErrorResponseException testHttpErrorResponseException;

        testHttpErrorResponseException = new HttpErrorResponseException(testStatusCode, testStatusDescription, testResponseMessage);
        validateHttpErrorResponseException(null, null, testStatusCode, testStatusDescription, testResponseMessage, testHttpErrorResponseException);

        testHttpErrorResponseException = new HttpErrorResponseException(testMessage, testStatusCode, testStatusDescription, testResponseMessage);
        validateHttpErrorResponseException(testMessage, null, testStatusCode, testStatusDescription, testResponseMessage, testHttpErrorResponseException);

        testHttpErrorResponseException = new HttpErrorResponseException(testMessage, testCause, testStatusCode, testStatusDescription, testResponseMessage);
        validateHttpErrorResponseException(testMessage, testCause, testStatusCode, testStatusDescription, testResponseMessage, testHttpErrorResponseException);

        testHttpErrorResponseException = new HttpErrorResponseException(testCause, testStatusCode, testStatusDescription, testResponseMessage);
        validateHttpErrorResponseException(testCause.toString(), testCause, testStatusCode, testStatusDescription, testResponseMessage,
            testHttpErrorResponseException);

        testHttpErrorResponseException =
            new HttpErrorResponseException(testMessage, testCause, false, false, testStatusCode, testStatusDescription, testResponseMessage);
        validateHttpErrorResponseException(testMessage, testCause, testStatusCode, testStatusDescription, testResponseMessage, testHttpErrorResponseException);
    }

    @Test
    public void testToString()
    {
        HttpErrorResponseException testHttpErrorResponseException =
            new HttpErrorResponseException(testMessage, testStatusCode, testStatusDescription, testResponseMessage);

        String expectedHttpErrorResponseExceptionString = String
            .format("Message: %s, Status Code: %d, Status Description: %s, Response Message: %s", testMessage, testStatusCode, testStatusDescription,
                testResponseMessage);

        assertEquals(expectedHttpErrorResponseExceptionString, testHttpErrorResponseException.toString());
    }

    @Test
    public void testEquals()
    {
        HttpErrorResponseException testHttpErrorResponseException =
            new HttpErrorResponseException(testMessage, testStatusCode, testStatusDescription, testResponseMessage);

        Object nullObject = null;

        assertTrue(testHttpErrorResponseException.equals(testHttpErrorResponseException));
        assertFalse(testHttpErrorResponseException.equals(nullObject));
        assertFalse(testHttpErrorResponseException.equals(""));
        assertTrue(testHttpErrorResponseException.equals(new HttpErrorResponseException(testStatusCode, null, null)));
    }

    @Test
    public void testHashCode()
    {
        HttpErrorResponseException testHttpErrorResponseException =
            new HttpErrorResponseException(testMessage, testStatusCode, testStatusDescription, testResponseMessage);

        assertEquals(((Integer) testStatusCode).hashCode(), testHttpErrorResponseException.hashCode());
    }

    private void validateHttpErrorResponseException(String expectedMessage, Throwable expectedCause, int expectedStatusCode, String expectedStatusDescription,
        String expectedResponseMessage, HttpErrorResponseException httpErrorResponseException)
    {
        assertEquals(expectedMessage, httpErrorResponseException.getMessage());
        assertEquals(expectedCause, httpErrorResponseException.getCause());
        assertEquals(expectedStatusCode, httpErrorResponseException.getStatusCode());
        assertEquals(expectedStatusDescription, httpErrorResponseException.getStatusDescription());
        assertEquals(expectedResponseMessage, httpErrorResponseException.getResponseMessage());
    }
}
