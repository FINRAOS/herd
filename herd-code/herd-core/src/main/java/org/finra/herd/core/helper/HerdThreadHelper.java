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
package org.finra.herd.core.helper;

import org.apache.log4j.Logger;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * A helper class for thread operations.
 */
@Component
public class HerdThreadHelper
{
    private static final Logger LOGGER = Logger.getLogger(HerdThreadHelper.class);

    /**
     * Sleeps specified number of milliseconds.
     *
     * @param millis the number of milliseconds to sleep
     */
    public void sleep(Long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (Exception e)
        {
            // Don't do anything since this is just a sleep.
            LOGGER.warn("Couldn't sleep for " + millis + " milliseconds.", e);
        }
    }

    @Async
    public void executeAsync(Runnable runnable)
    {
        runnable.run();
    }
}
