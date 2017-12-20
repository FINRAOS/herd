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
package org.finra.herd.tools.retention.destroyer;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import org.finra.herd.core.helper.LogLevel;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.tools.common.databridge.DataBridgeWebClient;

public class RetentionExpirationDestroyerControllerTest extends AbstractRetentionExpirationDestroyerTest
{
    @Before
    @Override
    public void setup() throws Exception
    {
        super.setup();

        // Set the web client logger to warn level so we don't get unnecessary info level logging on the output.
        setLogLevel(DataBridgeWebClient.class, LogLevel.WARN);
        setLogLevel(RetentionExpirationDestroyerWebClient.class, LogLevel.WARN);
    }

    @Test
    public void testPerformRetentionExpirationDestruction() throws Exception
    {
        // Build an input CSV file.
        File inputFile = new File(LOCAL_INPUT_FILE);

        // Create business object definition URI.
        String expectedUri = String.format("https://%s/data-entities/%s/%s", UDC_SERVICE_HOSTNAME, NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME);

        // Build the input file content.
        StringBuilder stringBuilder = new StringBuilder();

        // Add a CSV header.
        stringBuilder.append(
            "\"Namespace\",\"Business Object Definition Name\",\"Business Object Format Usage\",\"Business Object Format File Type\",\"Business Object Format Version\",\"Primary Partition Value\",\"Sub-Partition Value 1\",\"Sub-Partition Value 2\",\"Sub-Partition Value 3\",\"Sub-Partition Value 4\",\"Business Object Data Version\",\"Business Object Definition URI\"")
            .append(System.lineSeparator());

        // Add business object data with sub-partitions.
        stringBuilder.append(String
            .format("\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%s\"%n", NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME,
                BUSINESS_OBJECT_FORMAT_USAGE, BUSINESS_OBJECT_FORMAT_FILE_TYPE, BUSINESS_OBJECT_FORMAT_VERSION, "primaryPartitionValue", "subPartitionValue1",
                "subPartitionValue2", "subPartitionValue3", "subPartitionValue4", BUSINESS_OBJECT_DATA_VERSION, expectedUri));

        // Add a business object data without sub-partitions.
        stringBuilder.append(String
            .format("\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%s\"%n", NAMESPACE, BUSINESS_OBJECT_DEFINITION_NAME,
                BUSINESS_OBJECT_FORMAT_USAGE, BUSINESS_OBJECT_FORMAT_FILE_TYPE, BUSINESS_OBJECT_FORMAT_VERSION, "primaryPartitionValue", "", "", "", "",
                BUSINESS_OBJECT_DATA_VERSION, expectedUri));

        // Write to the input file.
        FileUtils.writeStringToFile(inputFile, stringBuilder.toString(), StandardCharsets.UTF_8);

        // Create and initialize the registration server DTO.
        RegServerAccessParamsDto regServerAccessParamsDto =
            RegServerAccessParamsDto.builder().withRegServerHost(WEB_SERVICE_HOSTNAME).withRegServerPort(WEB_SERVICE_HTTPS_PORT).withUseSsl(true)
                .withUsername(WEB_SERVICE_HTTPS_USERNAME).withPassword(WEB_SERVICE_HTTPS_PASSWORD).build();

        // Perform the retention expiration destruction.
        retentionExpirationDestroyerController.performRetentionExpirationDestruction(inputFile, regServerAccessParamsDto);
    }
}
