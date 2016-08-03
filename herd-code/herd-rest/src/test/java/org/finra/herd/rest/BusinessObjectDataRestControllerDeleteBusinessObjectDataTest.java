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
package org.finra.herd.rest;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;

/**
 * This class tests deleteBusinessObjectData functionality within the business object data REST controller.
 */
public class BusinessObjectDataRestControllerDeleteBusinessObjectDataTest extends AbstractRestTest
{
    /**
     * Sets up the test environment.
     */
    @Before
    public void setupEnv() throws IOException
    {
        // Create a local temp directory.
        localTempPath = Files.createTempDirectory(null);
    }

    /**
     * Cleans up the local temp directory and S3 test path that we are using.
     */
    @After
    public void cleanEnv() throws IOException
    {
        // Clean up the local directory.
        FileUtils.deleteDirectory(localTempPath.toFile());

        // Clean up the destination S3 folders.
        S3FileTransferRequestParamsDto s3FileTransferRequestParamsDto = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        for (String keyPrefix : Arrays.asList(testS3KeyPrefix, TEST_S3_KEY_PREFIX))
        {
            // Since the key prefix represents a directory, we add a trailing '/' character to it.
            s3FileTransferRequestParamsDto.setS3KeyPrefix(keyPrefix + "/");
            s3Dao.deleteDirectory(s3FileTransferRequestParamsDto);
        }
    }

    @Test
    public void testDeleteBusinessObjectData() throws Exception
    {
        // Create an initial version of a business object data.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS);

        // Validate that this business object data exists.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                INITIAL_DATA_VERSION);
        assertNotNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));

        // Delete the business object data.
        BusinessObjectData deletedBusinessObjectData = businessObjectDataRestController
            .deleteBusinessObjectData(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), INITIAL_DATA_VERSION, false);

        // Validate the returned object.
        validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, deletedBusinessObjectData);

        // Ensure that this business object data is no longer there.
        assertNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));
    }

    @Test
    public void testDeleteBusinessObjectDataMissingOptionalParameters() throws Exception
    {
        // Test if we can delete a business object data with any allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Create a business object data with the relative number of subpartition values.
            BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, true, BDATA_STATUS);

            // Validate that this business object data exists.
            BusinessObjectDataKey businessObjectDataKey =
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subPartitionValues,
                    DATA_VERSION);
            assertNotNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));

            // Delete the business object data using the relative endpoint.
            BusinessObjectData deletedBusinessObjectData = null;
            switch (i)
            {
                case 0:
                    deletedBusinessObjectData = businessObjectDataRestController
                        .deleteBusinessObjectData(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION,
                            false);
                    break;
                case 1:
                    deletedBusinessObjectData = businessObjectDataRestController
                        .deleteBusinessObjectData(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), DATA_VERSION, false);
                    break;
                case 2:
                    deletedBusinessObjectData = businessObjectDataRestController
                        .deleteBusinessObjectData(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), DATA_VERSION, false);
                    break;
                case 3:
                    deletedBusinessObjectData = businessObjectDataRestController
                        .deleteBusinessObjectData(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), DATA_VERSION, false);
                    break;
                case 4:
                    deletedBusinessObjectData = businessObjectDataRestController
                        .deleteBusinessObjectData(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), subPartitionValues.get(3), DATA_VERSION, false);
                    break;
            }

            // Validate the returned object.
            validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, subPartitionValues, DATA_VERSION, true, BDATA_STATUS, deletedBusinessObjectData);

            // Ensure that this business object data is no longer there.
            assertNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));
        }
    }
}
