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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.amazonaws.AmazonServiceException;
import org.apache.http.HttpStatus;
import org.junit.Test;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the AwsServiceHelper class.
 */
public class AwsServiceHelperTest extends AbstractServiceTest
{
    @Test
    public void testHandleAmazonException()
    {
        try
        {
            AmazonServiceException amazonServiceException = new AmazonServiceException(ERROR_MESSAGE);
            awsServiceHelper.handleAmazonException(amazonServiceException, MESSAGE_TEXT);
            fail();
        }
        catch (AmazonServiceException amazonServiceException)
        {
            assertThat("Error message not correct.", amazonServiceException.getMessage().contains(ERROR_MESSAGE), is(true));
        }
    }

    @Test
    public void testHandleAmazonExceptionWithBadRequestException()
    {
        try
        {
            AmazonServiceException amazonServiceException = new AmazonServiceException(ERROR_MESSAGE);
            amazonServiceException.setStatusCode(HttpStatus.SC_BAD_REQUEST);
            awsServiceHelper.handleAmazonException(amazonServiceException, MESSAGE_TEXT);
            fail();
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            assertThat("Error message not correct.", illegalArgumentException.getMessage().contains(MESSAGE_TEXT), is(true));
        }
    }

    @Test
    public void testHandleAmazonExceptionWithNotFoundException()
    {
        try
        {
            AmazonServiceException amazonServiceException = new AmazonServiceException(ERROR_MESSAGE);
            amazonServiceException.setStatusCode(HttpStatus.SC_NOT_FOUND);
            awsServiceHelper.handleAmazonException(amazonServiceException, MESSAGE_TEXT);
            fail();
        }
        catch (ObjectNotFoundException objectNotFoundException)
        {
            assertThat("Error message not correct.", objectNotFoundException.getMessage().contains(MESSAGE_TEXT), is(true));
        }
    }
}
