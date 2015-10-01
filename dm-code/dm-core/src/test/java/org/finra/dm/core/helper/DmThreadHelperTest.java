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
package org.finra.dm.core.helper;

import org.springframework.beans.factory.annotation.Autowired;

import org.junit.Test;

import org.finra.dm.core.AbstractCoreTest;
import org.finra.dm.core.Command;

/**
 * This class tests functionality within the DmThreadHelper class.
 */
public class DmThreadHelperTest extends AbstractCoreTest
{
    @Autowired
    DmThreadHelper dmThreadHelper;
    /*
     * This test is to get the clover coverage for sleep() method on DmHelper 
     */
    @Test
    public void testSleep() throws Exception
    {
        // Sleep for 1 second
        dmThreadHelper.sleep(1 * 1000L);

        // Passing null should result in Exception that is eaten and logged 

        executeWithoutLogging(DmThreadHelper.class, new Command()
        {
            @Override
            public void execute() throws Exception
            {
                (new DmThreadHelper()).sleep(null);
            }
        });
    }
}
