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
     */
    public void handleAmazonException(AmazonServiceException ex, String message) throws IllegalArgumentException, ObjectNotFoundException
    {
        if (ex.getStatusCode() == HttpStatus.SC_BAD_REQUEST)
        {
            throw new IllegalArgumentException(message + " Reason: " + ex.getMessage(), ex);
        }
        else if (ex.getStatusCode() == HttpStatus.SC_NOT_FOUND)
        {
            throw new ObjectNotFoundException(message + " Reason: " + ex.getMessage(), ex);
        }
        throw ex;
    }
}
