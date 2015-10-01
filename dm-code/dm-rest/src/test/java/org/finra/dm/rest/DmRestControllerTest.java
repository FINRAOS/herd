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
package org.finra.dm.rest;

import static org.junit.Assert.assertNotNull;

import org.apache.log4j.Logger;
import org.junit.Test;

import org.finra.dm.model.api.xml.BuildInformation;

/**
 * This class tests various functionality within the DM REST controller.
 */
public class DmRestControllerTest extends AbstractRestTest
{
    private static Logger logger = Logger.getLogger(DmRestControllerTest.class);

    @Test
    public void testGetBuildInfo() throws Exception
    {
        // Get the build information and ensure it is valid.
        BuildInformation buildInformation = dmRestController.getBuildInfo();
        assertNotNull(buildInformation);
        assertNotNull(buildInformation.getBuildDate());
        logger.info(buildInformation);
    }
}
