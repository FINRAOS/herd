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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;

/**
 * This class tests deleteBusinessObjectData functionality within the business object data REST controller.
 */
public class BusinessObjectDataServiceDeleteBusinessObjectDataTest extends AbstractServiceTest
{
    private static final String testS3KeyPrefix =
        getExpectedS3KeyPrefix(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_KEY,
            PARTITION_VALUE, null, null, INITIAL_DATA_VERSION);

    private Path localTempPath;

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
        BusinessObjectData deletedBusinessObjectData = businessObjectDataService.deleteBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                INITIAL_DATA_VERSION), false);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, deletedBusinessObjectData);

        // Ensure that this business object data is no longer there.
        assertNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));
    }

    @Test
    public void testDeleteBusinessObjectDataMissingRequiredParameters()
    {
        // Try to perform a delete without specifying business object definition name.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), false);
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to perform a delete without specifying business object format usage.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), false);
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to perform a delete without specifying business object format file type.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), false);
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to perform a delete without specifying business object format version.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), false);
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to perform a delete without specifying partition value.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT, SUBPARTITION_VALUES,
                    DATA_VERSION), false);
            fail("Should throw an IllegalArgumentException when partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }

        // Try to perform a delete without specifying 1st subpartition value.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(BLANK_TEXT, SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION), false);
            fail("Should throw an IllegalArgumentException when 1st subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to perform a delete without specifying 2nd subpartition value.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), BLANK_TEXT, SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION), false);
            fail("Should throw an IllegalArgumentException when 2nd subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to perform a delete without specifying 3rd subpartition value.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), BLANK_TEXT, SUBPARTITION_VALUES.get(3)), DATA_VERSION), false);
            fail("Should throw an IllegalArgumentException when 3rd subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to perform a delete without specifying 4th subpartition value.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), BLANK_TEXT), DATA_VERSION), false);
            fail("Should throw an IllegalArgumentException when 4th subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to perform a delete without specifying business object data version.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    null), false);
            fail("Should throw an IllegalArgumentException when business object data version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data version must be specified.", e.getMessage());
        }

        // Try to perform a delete without specifying delete files flag.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), null);
            fail("Should throw an IllegalArgumentException when delete files flag is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A delete files flag must be specified.", e.getMessage());
        }
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
                    deletedBusinessObjectData = businessObjectDataService.deleteBusinessObjectData(
                        new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            NO_SUBPARTITION_VALUES, DATA_VERSION), false);
                    break;
                case 1:
                    deletedBusinessObjectData = businessObjectDataService.deleteBusinessObjectData(
                        new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0)), DATA_VERSION), false);
                    break;
                case 2:
                    deletedBusinessObjectData = businessObjectDataService.deleteBusinessObjectData(
                        new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1)), DATA_VERSION), false);
                    break;
                case 3:
                    deletedBusinessObjectData = businessObjectDataService.deleteBusinessObjectData(
                        new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2)), DATA_VERSION), false);
                    break;
                case 4:
                    deletedBusinessObjectData = businessObjectDataService.deleteBusinessObjectData(
                        new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            SUBPARTITION_VALUES, DATA_VERSION), false);
                    break;
            }

            // Validate the returned object.
            businessObjectDataServiceTestHelper
                .validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, subPartitionValues, DATA_VERSION, true, BDATA_STATUS, deletedBusinessObjectData);

            // Ensure that this business object data is no longer there.
            assertNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));
        }
    }

    @Test
    public void testDeleteBusinessObjectDataTrimParameters()
    {
        // Create and persist a business object data.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

        // Validate that this business object data exists.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);
        assertNotNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));

        // Delete the business object data using input parameters with leading and trailing empty spaces.
        BusinessObjectData deletedBusinessObjectData = businessObjectDataService.deleteBusinessObjectData(
            new BusinessObjectDataKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUBPARTITION_VALUES), DATA_VERSION), false);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS, deletedBusinessObjectData);

        // Ensure that this business object data is no longer there.
        assertNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));
    }

    @Test
    public void testDeleteBusinessObjectDataUpperCaseParameters()
    {
        // Create and persist a business object data using lower case values.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION,
                true, BDATA_STATUS.toLowerCase());

        // Validate that this business object data exists.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION);
        assertNotNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));

        // Delete the business object data using upper case input parameters (except for case-sensitive partition values).
        BusinessObjectData deletedBusinessObjectData = businessObjectDataService.deleteBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION), false);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION,
                true, BDATA_STATUS.toLowerCase(), deletedBusinessObjectData);

        // Ensure that this business object format is no longer there.
        assertNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));
    }

    @Test
    public void testDeleteBusinessObjectDataLowerCaseParameters()
    {
        // Create and persist a business object data using upper case values.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION,
                true, BDATA_STATUS.toUpperCase());

        // Validate that this business object data exists.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION);
        assertNotNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));

        // Delete the business object data using lower case input parameters (except for case-sensitive partition values).
        BusinessObjectData deletedBusinessObjectData = businessObjectDataService.deleteBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION), false);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION,
                true, BDATA_STATUS.toUpperCase(), deletedBusinessObjectData);

        // Ensure that this business object format is no longer there.
        assertNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));
    }

    @Test
    public void testDeleteBusinessObjectDataInvalidParameters()
    {
        // Create and persist a valid business object data.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

        // Validate that this business object data exists.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);
        assertNotNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));

        // Try to perform a delete using invalid namespace.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), false);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage("I_DO_NOT_EXIST", BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }

        // Try to perform a delete using invalid business object definition name.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), false);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }

        // Try to perform a delete using invalid format usage.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), false);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }

        // Try to perform a delete using invalid format file type.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), false);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }

        // Try to perform a delete using invalid business object format version.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), false);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }

        // Try to perform a delete using invalid partition value.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, "I_DO_NOT_EXIST", SUBPARTITION_VALUES,
                    DATA_VERSION), false);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    "I_DO_NOT_EXIST", SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }

        // Try to perform a delete using invalid subpartition value.
        for (int i = 0; i < SUBPARTITION_VALUES.size(); i++)
        {
            List<String> testSubPartitionValues = new ArrayList<>(SUBPARTITION_VALUES);
            testSubPartitionValues.set(i, "I_DO_NOT_EXIST");
            try
            {
                businessObjectDataService.deleteBusinessObjectData(
                    new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        testSubPartitionValues, DATA_VERSION), false);
                fail("Should throw an ObjectNotFoundException when not able to find business object data.");
            }
            catch (ObjectNotFoundException e)
            {
                assertEquals(businessObjectDataServiceTestHelper
                    .getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                        PARTITION_VALUE, testSubPartitionValues, DATA_VERSION, null), e.getMessage());
            }
        }

        // Try to perform a delete using invalid business object data version.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    INVALID_DATA_VERSION), false);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, INVALID_DATA_VERSION, null), e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectDataBusinessObjectDataNoExists()
    {
        // Try to delete a non-existing business object data passing subpartition values.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), false);
            fail("Should throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectDataChildrenExist()
    {
        // Create an initial version of a business object data.
        BusinessObjectDataEntity businessObjectDataParentEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

        // Create a child business object data entity.
        BusinessObjectDataEntity businessObjectDataChildEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE_2,
                SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

        // Associate with each other child and parent business object data entities.
        businessObjectDataParentEntity.getBusinessObjectDataChildren().add(businessObjectDataChildEntity);
        businessObjectDataChildEntity.getBusinessObjectDataParents().add(businessObjectDataParentEntity);

        // Try to delete the parent business object data.
        try
        {
            businessObjectDataService.deleteBusinessObjectData(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), false);
            fail("Should throw an IllegalArgumentException when trying to delete a business object data that has children associated with it.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Can not delete a business object data that has children associated with it. " +
                "Business object data: {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                "businessObjectFormatFileType: \"%s\", " +
                "businessObjectFormatVersion: %d, businessObjectDataPartitionValue: \"%s\", businessObjectDataSubPartitionValues: \"%s,%s,%s,%s\", " +
                "businessObjectDataVersion: %d}", NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION), e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectDataNotLatestVersion()
    {
        // Create and persist a business object data which is not marked as the latest version.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, false, BDATA_STATUS);

        // Validate that this business object data exists.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION);
        assertNotNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));

        // Delete the business object data.
        BusinessObjectData deletedBusinessObjectData = businessObjectDataService.deleteBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), false);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, false, BDATA_STATUS, deletedBusinessObjectData);

        // Ensure that this business object data is no longer there.
        assertNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));
    }

    @Test
    public void testDeleteBusinessObjectDataLatestVersionWhenPreviousVersionExists()
    {
        // Create and persist two versions of the business object data.
        BusinessObjectDataEntity initialVersionBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, INITIAL_DATA_VERSION, false, BDATA_STATUS);
        BusinessObjectDataEntity latestVersionBusinessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, SECOND_DATA_VERSION, true, BDATA_STATUS);

        // Validate that the initial version does not have the latest version flag set and that second version has it set.
        assertFalse(initialVersionBusinessObjectDataEntity.getLatestVersion());
        assertTrue(latestVersionBusinessObjectDataEntity.getLatestVersion());

        // Delete the latest (second) version of the business object format.
        BusinessObjectData deletedBusinessObjectData = businessObjectDataService.deleteBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                SECOND_DATA_VERSION), false);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(latestVersionBusinessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, SECOND_DATA_VERSION, true, BDATA_STATUS, deletedBusinessObjectData);

        // Ensure that this business object format is no longer there.
        assertNull(businessObjectDataDao.getBusinessObjectDataByAltKey(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                SECOND_DATA_VERSION)));

        // Validate that the initial version now has the latest version flag set.
        initialVersionBusinessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                INITIAL_DATA_VERSION));
        assertNotNull(initialVersionBusinessObjectDataEntity);
        assertTrue(initialVersionBusinessObjectDataEntity.getLatestVersion());

        // Delete the initial (first) version of the business object format.
        deletedBusinessObjectData = businessObjectDataService.deleteBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                INITIAL_DATA_VERSION), false);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(initialVersionBusinessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, deletedBusinessObjectData);

        // Ensure that this business object format is no longer there.
        assertNull(businessObjectDataDao.getBusinessObjectDataByAltKey(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                INITIAL_DATA_VERSION)));
    }

    @Test
    public void testDeleteBusinessObjectDataS3ManagedBucket() throws Exception
    {
        // Create test database entities.
        createTestDatabaseEntities(StorageEntity.MANAGED_STORAGE, StoragePlatformEntity.S3, testS3KeyPrefix, LOCAL_FILES);

        // Create and upload to the test S3 storage a set of files.
        businessObjectDataServiceTestHelper.prepareTestS3Files(testS3KeyPrefix, localTempPath, LOCAL_FILES);

        // Validate that this business object data exists.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                new ArrayList<String>(), INITIAL_DATA_VERSION);
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey);
        assertNotNull(businessObjectDataEntity);

        // Delete the business object data with delete files flag set to true.
        BusinessObjectData deletedBusinessObjectData = businessObjectDataService.deleteBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION), true);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, deletedBusinessObjectData);

        // Validate that data files got deleted from S3.
        S3FileTransferRequestParamsDto params = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        params.setS3KeyPrefix(testS3KeyPrefix);
        assertTrue(s3Dao.listDirectory(params).isEmpty());

        // Ensure that this business object data is no longer there.
        assertNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));
    }

    @Test
    public void testDeleteBusinessObjectDataS3NonManagedBucketDeletingDirectory() throws Exception
    {
        // Create test database entities.
        createTestDatabaseEntities(STORAGE_NAME, StoragePlatformEntity.S3, TEST_S3_KEY_PREFIX, new ArrayList<String>());

        // Create and upload to the test S3 storage a set of files.
        businessObjectDataServiceTestHelper.prepareTestS3Files(TEST_S3_KEY_PREFIX, localTempPath, LOCAL_FILES);

        // Validate that this business object data exists.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION);
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey);
        assertNotNull(businessObjectDataEntity);

        // Delete the business object data with delete files flag set to true.
        BusinessObjectData deletedBusinessObjectData = businessObjectDataService.deleteBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION), true);
        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, deletedBusinessObjectData);

        // Validate that data files got deleted from S3.
        S3FileTransferRequestParamsDto params = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        params.setS3KeyPrefix(TEST_S3_KEY_PREFIX);
        assertTrue(s3Dao.listDirectory(params).isEmpty());

        // Ensure that this business object data is no longer there.
        assertNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));
    }

    @Test
    public void testDeleteBusinessObjectDataS3NonManagedBucketDeletingListOfFiles() throws Exception
    {
        // Create test database entities.
        createTestDatabaseEntities(STORAGE_NAME, StoragePlatformEntity.S3, TEST_S3_KEY_PREFIX, LOCAL_FILES);

        // Create and upload to the S3 storage a subset of test files.
        // Please note that we are only uploading a subset of test files to make sure we do not fail the deletion because of missing (non-existing) S3 files.
        businessObjectDataServiceTestHelper.prepareTestS3Files(TEST_S3_KEY_PREFIX, localTempPath, LOCAL_FILES_SUBSET);

        // Validate that this business object data exists.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION);
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey);
        assertNotNull(businessObjectDataEntity);

        // Delete the business object data with delete files flag set to true.
        BusinessObjectData deletedBusinessObjectData = businessObjectDataService.deleteBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION), true);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, deletedBusinessObjectData);

        // Validate that data files got deleted from S3 - please note that
        S3FileTransferRequestParamsDto params = s3DaoTestHelper.getTestS3FileTransferRequestParamsDto();
        params.setS3KeyPrefix(TEST_S3_KEY_PREFIX);
        assertTrue(s3Dao.listDirectory(params).isEmpty());

        // Ensure that this business object data is no longer there.
        assertNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));
    }

    @Test
    public void testDeleteBusinessObjectDataNonS3StoragePlatform() throws Exception
    {
        // Create test database entities including the relative non-S3 storage entities.
        createTestDatabaseEntities(STORAGE_NAME, STORAGE_PLATFORM_CODE, testS3KeyPrefix, LOCAL_FILES);

        // Validate that this business object data exists.
        BusinessObjectDataKey businessObjectDataKey =
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION);
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey);
        assertNotNull(businessObjectDataEntity);

        // Delete the business object data with delete files flag set to true.
        BusinessObjectData deletedBusinessObjectData = businessObjectDataService.deleteBusinessObjectData(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION), true);

        // Validate the returned object.
        businessObjectDataServiceTestHelper
            .validateBusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS, deletedBusinessObjectData);

        // Ensure that this business object data is no longer there.
        assertNull(businessObjectDataDao.getBusinessObjectDataByAltKey(businessObjectDataKey));
    }

    /**
     * Create and persist a business object data entity along with the relative storage related entities.
     *
     * @param storageName the storage name
     * @param storagePlatform the storage platform
     * @param directoryPath the directory path for the storage unit entity
     * @param localFiles the list of local files to create relative storage file entities for
     */
    private void createTestDatabaseEntities(String storageName, String storagePlatform, String directoryPath, List<String> localFiles) throws Exception
    {
        // Create and persist a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, INITIAL_DATA_VERSION, true, BDATA_STATUS);

        // Create an S3 storage entity if it does not exist.
        StorageEntity storageEntity = storageDao.getStorageByName(storageName);
        if (storageEntity == null)
        {
            storageEntity = storageDaoTestHelper
                .createStorageEntity(storageName, storagePlatform, configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME),
                    storageDaoTestHelper.getS3ManagedBucketName());
        }

        // Create a storage unit entity.
        StorageUnitEntity storageUnitEntity =
            storageUnitDaoTestHelper.createStorageUnitEntity(storageEntity, businessObjectDataEntity, StorageUnitStatusEntity.ENABLED, directoryPath);

        // Create relative storage file entities.
        for (String fileLocalPath : localFiles)
        {
            storageFileDaoTestHelper
                .createStorageFileEntity(storageUnitEntity, String.format("%s/%s", directoryPath, fileLocalPath), FILE_SIZE_1_KB, ROW_COUNT_1000);
        }

        herdDao.saveAndRefresh(businessObjectDataEntity);
    }
}
