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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.core.AbstractCoreTest;
import org.finra.herd.core.Command;

/**
 * This class tests functionality within the HerdThreadHelper class.
 */
public class HerdThreadHelperTest extends AbstractCoreTest
{
    @Autowired
    private HerdThreadHelper herdThreadHelper;

    /*
     * This test is to get the clover coverage for sleep() method on HerdHelper
     */
    @Test
    public void testSleep() throws Exception
    {
        // Sleep for 1 second
        herdThreadHelper.sleep(1 * 1000L);

        // Passing null should result in Exception that is eaten and logged

        executeWithoutLogging(HerdThreadHelper.class, new Command()
        {
            @Override
            public void execute() throws Exception
            {
                (new HerdThreadHelper()).sleep(null);
            }
        });
    }

    /**
     * Asserts that calling executeAsync actually runs the given runnable and runs it asynchronously.
     * We test it by having a "token" queue which is a blocking queue.
     * The async task will push a token into the queue while the thread which scheduled the task will wait for a token to be available in the queue.
     * The main thread will be blocked, and the lock only released when a asynchronous thread pushes a token into the queue.
     */
    @Test
    public void testExecuteAsync() throws Exception
    {
        // The "token" queue
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>(1);

        // Schedule an asynchronous task
        herdThreadHelper.executeAsync(new Runnable()
        {
            public void run()
            {
                try
                {
                    Thread.sleep(500);
                    queue.offer("testToken");
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }
        });

        /*
         * Poll the queue, which blocks this thread.
         * For safety, a short timeout of 1 second is added, so in case of deadlock or other threading nightmares, this test doesn't block the rest of test
         * execution.
         * The token will be null if the timeout is reached, otherwise
         */
        String token = queue.poll(1000, TimeUnit.MILLISECONDS);

        Assert.assertNotNull("Expected the async task to have put a token in the queue, but none was found after a timeout. Ensure that the async task "
            + "actually ran and pushed a token into queue.", token);
    }
}
