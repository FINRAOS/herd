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
package org.finra.dm.tools.uploader;

import java.io.IOException;

import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.dm.model.dto.DmRegServerAccessParamsDto;
import org.finra.dm.tools.common.databridge.AbstractDataBridgeTest;

/**
 * This is an abstract base class that provides useful methods for uploader test drivers.
 */
public abstract class AbstractUploaderTest extends AbstractDataBridgeTest
{
    protected static final Integer TEST_RETRY_ATTEMPTS = 2;
    protected static final Integer TEST_RETRY_DELAY_SECS = 5;

    /**
     * Provide easy access to the UploaderManifestReader instance for all test methods.
     */
    @Autowired
    protected UploaderManifestReader uploaderManifestReader;

    /**
     * Provide easy access to the UploaderController for all test methods.
     */
    @Autowired
    protected UploaderController uploaderController;

    /**
     * Provide easy access to the uploader web client for all test methods.
     */
    @Autowired
    protected UploaderWebClient uploaderWebClient;

    /**
     * Sets up the test environment.
     */
    @Before
    public void setupEnv() throws IOException
    {
        super.setupEnv();

        // Initialize the uploader web client instance.
        DmRegServerAccessParamsDto dmRegServerAccessParamsDto =
            DmRegServerAccessParamsDto.builder().dmRegServerHost(WEB_SERVICE_HOSTNAME).dmRegServerPort(WEB_SERVICE_HTTPS_PORT).useSsl(true)
                .username(WEB_SERVICE_HTTPS_USERNAME).password(WEB_SERVICE_HTTPS_PASSWORD).build();
        uploaderWebClient.setDmRegServerAccessParamsDto(dmRegServerAccessParamsDto);
    }
}
