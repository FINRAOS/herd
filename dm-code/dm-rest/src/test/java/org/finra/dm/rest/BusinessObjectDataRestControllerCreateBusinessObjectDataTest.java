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

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.finra.dm.model.dto.S3FileTransferRequestParamsDto;
import org.finra.dm.model.api.xml.BusinessObjectData;
import org.finra.dm.model.api.xml.BusinessObjectDataCreateRequest;

/**
 * This class tests the createBusinessObjectData functionality within the business object data REST controller.
 */
public class BusinessObjectDataRestControllerCreateBusinessObjectDataTest extends AbstractRestTest
{
    protected static Logger logger = Logger.getLogger(BusinessObjectDataRestControllerCreateBusinessObjectDataTest.class);

    /**
     * Initialize the environment. This method is run once before any of the test methods in the class.
     */
    @BeforeClass
    public static void initEnv() throws IOException
    {
        localTempPath = Paths.get(System.getProperty("java.io.tmpdir"), "dm-bod-service-create-test-local-folder");
    }

    /**
     * Sets up the test environment.
     */
    @Before
    public void setupEnv() throws IOException
    {
        // Create local temp directory.
        localTempPath.toFile().mkdir();
    }

    /**
     * Cleans up the local temp directory and S3 test path that we are using.
     */
    @After
    public void cleanEnv() throws IOException
    {
        try
        {
            // Clean up the local directory.
            FileUtils.deleteDirectory(localTempPath.toFile());

            // Clean up the destination S3 folder.
            S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = getTestS3FileTransferRequestParamsDto();
            s3FileTransferRequestParamsDto.setS3KeyPrefix(testS3KeyPrefix);
            s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);
        }
        catch (Exception ex)
        {
            // If an exception is thrown by one of the @Test methods, some cleanup operations could also fail. This is why we are just logging a warning here.
            logger.warn("Unable to cleanup environment.", ex);
        }
    }

    @Test
    public void testCreateBusinessObjectData()
    {
        // Create an initial version of the business object data.
        BusinessObjectDataCreateRequest businessObjectDataCreateRequest = getNewBusinessObjectDataCreateRequest();
        BusinessObjectData resultBusinessObjectData = businessObjectDataRestController.createBusinessObjectData(businessObjectDataCreateRequest);

        // Verify the results.
        validateBusinessObjectData(businessObjectDataCreateRequest, INITIAL_DATA_VERSION, true, resultBusinessObjectData);
    }
}
