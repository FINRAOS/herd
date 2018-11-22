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
package org.finra.herd.dao.helper;

import java.io.IOException;

import io.searchbox.action.Action;
import io.searchbox.client.JestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;

import org.finra.herd.dao.JestClientFactory;

/**
 * JestClientHelper
 */
@Component
public class JestClientHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JestClientHelper.class);

    @Autowired
    private JestClientFactory jestClientFactory;

    /**
     * Method to use the JEST client to execute an elastic action
     *
     * @param action action
     *
     * @return a jest search result
     */
    @Retryable(maxAttempts = 3, value = RestClientException.class, backoff = @Backoff(delay = 5000, multiplier = 2))
    public <T extends JestResult> T execute(Action<T> action)
    {
        T actionResult = null;
        try
        {
            actionResult = jestClientFactory.getJestClient().execute(action);

            // log the error if the action failed but no exception is thrown
            if (actionResult == null || !actionResult.isSucceeded())
            {
                LOGGER.error("Failed to execute JEST client action. action={}, actionResult={}", action, actionResult);
            }
        }
        catch (final IOException ioException)
        {
            LOGGER.error("Failed to execute JEST client action.", ioException);
            //throw a runtime exception so that the client needs not catch
            throw new RestClientException(ioException.getMessage()); //NOPMD
        }

        return actionResult;
    }

}
