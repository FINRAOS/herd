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

import com.amazonaws.AmazonServiceException;
import org.apache.http.HttpStatus;
import org.springframework.stereotype.Component;

import org.finra.herd.model.ObjectNotFoundException;

/**
 * A helper class for AWS related code.
 */
@Component
public class AwsServiceHelper
{
    /**
     * Handles the AmazonServiceException, throws corresponding exception based on status code in amazon exception.
     *
     * @param amazonServiceException the AWS exception that will be handled by this method.
     * @param message the message to include with this exception.
     *
     * @throws IllegalArgumentException the exception to be thrown with a HttpStatus.SC_BAD_REQUEST
     * @throws ObjectNotFoundException the exception to be thrown with a HttpStatus.SC_NOT_FOUND
     */
    public void handleAmazonException(AmazonServiceException amazonServiceException, String message) throws IllegalArgumentException, ObjectNotFoundException
    {
        if (amazonServiceException.getStatusCode() == HttpStatus.SC_BAD_REQUEST)
        {
            throw new IllegalArgumentException(message + " Reason: " + amazonServiceException.getMessage(), amazonServiceException);
        }
        else if (amazonServiceException.getStatusCode() == HttpStatus.SC_NOT_FOUND)
        {
            throw new ObjectNotFoundException(message + " Reason: " + amazonServiceException.getMessage(), amazonServiceException);
        }

        throw amazonServiceException;
    }
}
