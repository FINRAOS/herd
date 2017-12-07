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
package org.finra.herd.tools.retention.exporter;

import java.io.IOException;

import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.tools.common.databridge.AbstractDataBridgeTest;

/**
 * This is an abstract base class that provides useful methods for herd retention expiration tool test drivers.
 */
public abstract class AbstractExporterTest extends AbstractDataBridgeTest
{
    /**
     * Provide easy access to the UploaderController for all test methods.
     */
    @Autowired
    protected ExporterController exporterController;

    /**
     * Provide easy access to the uploader web client for all test methods.
     */
    @Autowired
    protected ExporterWebClient exporterWebClient;

    /**
     * Sets up the test environment.
     */
    @Before
    public void setupEnv() throws IOException
    {
        super.setupEnv();

        // Initialize the uploader web client instance.
        RegServerAccessParamsDto regServerAccessParamsDto =
            RegServerAccessParamsDto.builder().withRegServerHost(WEB_SERVICE_HOSTNAME).withRegServerPort(WEB_SERVICE_HTTPS_PORT).withUseSsl(true)
                .withUsername(WEB_SERVICE_HTTPS_USERNAME).withPassword(WEB_SERVICE_HTTPS_PASSWORD).build();
        exporterWebClient.setRegServerAccessParamsDto(regServerAccessParamsDto);
    }
}
