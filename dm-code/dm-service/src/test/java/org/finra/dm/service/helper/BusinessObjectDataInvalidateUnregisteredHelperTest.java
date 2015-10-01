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
package org.finra.dm.service.helper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.finra.dm.dao.S3Operations;
import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.dm.model.jpa.BusinessObjectFormatEntity;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.api.xml.BusinessObjectData;
import org.finra.dm.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest;
import org.finra.dm.model.api.xml.BusinessObjectDataInvalidateUnregisteredResponse;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.model.api.xml.StorageUnit;
import org.finra.dm.service.AbstractServiceTest;
import org.fusesource.hawtbuf.ByteArrayInputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class BusinessObjectDataInvalidateUnregisteredHelperTest extends AbstractServiceTest
{
    private static final Logger LOGGER = Logger.getLogger(BusinessObjectDataInvalidateUnregisteredHelperTest.class);

    @Autowired
    private S3Operations s3Operations;

    @After
    public void after()
    {
        s3Operations.rollback();
    }

    /**
     * Test case where S3 and DM are in sync because there are no data in either S3 or DM.
     * Expects no new registrations.
     * This is a happy path where common response values are asserted.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataS30DM0()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();

        // Given a business object format
        try
        {
            createBusinessObjectFormat(request);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Call the API
        try
        {
            BusinessObjectDataInvalidateUnregisteredResponse actualResponse =
                businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);

            // Make assertions
            Assert.assertNotNull("response is null", actualResponse);
            Assert.assertEquals("response namespace", request.getNamespace(), actualResponse.getNamespace());
            Assert.assertEquals("response business object definition name", request.getBusinessObjectDefinitionName(), actualResponse
                .getBusinessObjectDefinitionName());
            Assert.assertEquals("response business object format usage", request.getBusinessObjectFormatUsage(), actualResponse.getBusinessObjectFormatUsage());
            Assert.assertEquals("response business object format file type", request.getBusinessObjectFormatFileType(), actualResponse
                .getBusinessObjectFormatFileType());
            Assert.assertEquals("response business object format version", request.getBusinessObjectFormatVersion(), actualResponse
                .getBusinessObjectFormatVersion());
            Assert.assertEquals("response partition value", request.getPartitionValue(), actualResponse.getPartitionValue());
            Assert.assertEquals("response sub-partition values", request.getSubPartitionValues(), actualResponse.getSubPartitionValues());
            Assert.assertEquals("response storage name", request.getStorageName(), actualResponse.getStorageName());
            Assert.assertNotNull("response business object datas is null", actualResponse.getRegisteredBusinessObjectDataList());
            Assert.assertEquals("response business object datas size", 0, actualResponse.getRegisteredBusinessObjectDataList().size());
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail("unexpected exception thrown: " + e);
        }
    }

    /**
     * Test case where DM and S3 are in sync because both have 1 object registered.
     * Expects no new data registration.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataS31DM1()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();

        // Given a business object format
        try
        {
            BusinessObjectFormatEntity businessObjectFormatEntity = createBusinessObjectFormat(request);
            createS3Object(businessObjectFormatEntity, request, 0);
            createBusinessObjectData(businessObjectFormatEntity, request, 0, true);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Call the API
        try
        {
            BusinessObjectDataInvalidateUnregisteredResponse actualResponse =
                businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);

            // Make assertions
            Assert.assertNotNull("response business object datas is null", actualResponse.getRegisteredBusinessObjectDataList());
            Assert.assertEquals("response business object datas size", 0, actualResponse.getRegisteredBusinessObjectDataList().size());
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail("unexpected exception thrown: " + e);
        }
    }

    /**
     * Test case where S3 has 1 object, and DM has no object registered.
     * Expects one new registration in INVALID status.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataS31DM0()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();

        // Given a business object format
        // Given an object in S3
        BusinessObjectFormatEntity businessObjectFormatEntity;
        try
        {
            businessObjectFormatEntity = createBusinessObjectFormat(request);

            createS3Object(businessObjectFormatEntity, request, 0);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Call API
        try
        {
            BusinessObjectDataInvalidateUnregisteredResponse actualResponse =
                businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);

            // Make assertions
            Assert.assertNotNull("response business object datas is null", actualResponse.getRegisteredBusinessObjectDataList());
            Assert.assertEquals("response business object datas size", 1, actualResponse.getRegisteredBusinessObjectDataList().size());
            {
                BusinessObjectData businessObjectData = actualResponse.getRegisteredBusinessObjectDataList().get(0);
                Assert.assertEquals("response business object data[0] version", 0, businessObjectData.getVersion());
                Assert.assertEquals("response business object data[0] status", BusinessObjectDataInvalidateUnregisteredHelper.UNREGISTERED_STATUS,
                    businessObjectData.getStatus());
                Assert.assertNotNull("response business object data[0] storage units is null", businessObjectData.getStorageUnits());
                Assert.assertEquals("response business object data[0] storage units size", 1, businessObjectData.getStorageUnits().size());
                {
                    String expectedS3KeyPrefix =
                        businessObjectDataHelper.buildS3KeyPrefix(businessObjectFormatEntity, businessObjectDataHelper
                            .createBusinessObjectDataKey(businessObjectData));
                    StorageUnit storageUnit = businessObjectData.getStorageUnits().get(0);
                    Assert.assertNotNull("response business object data[0] storage unit[0] storage directory is null", storageUnit.getStorageDirectory());
                    Assert.assertEquals("response business object data[0] storage unit[0] storage directory path", expectedS3KeyPrefix, storageUnit
                        .getStorageDirectory().getDirectoryPath());
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail("unexpected exception thrown: " + e);
        }
    }

    /**
     * Test case where S3 has 1 object, and DM has no object registered. The data has sub-partitions.
     * Expects one new registration in INVALID status.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataS31DM0WithSubPartitions()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();
        request.setSubPartitionValues(SUBPARTITION_VALUES);

        // Given a business object format
        // Given an object in S3
        BusinessObjectFormatEntity businessObjectFormatEntity;
        try
        {
            businessObjectFormatEntity = createBusinessObjectFormat(request);

            createS3Object(businessObjectFormatEntity, request, 0);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Call API
        try
        {
            BusinessObjectDataInvalidateUnregisteredResponse actualResponse =
                businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);

            // Make assertions
            Assert.assertNotNull("response sub-partition values is null", actualResponse.getSubPartitionValues());
            Assert.assertEquals("response sub-partition values", request.getSubPartitionValues(), actualResponse.getSubPartitionValues());
            Assert.assertNotNull("response business object datas is null", actualResponse.getRegisteredBusinessObjectDataList());
            Assert.assertEquals("response business object datas size", 1, actualResponse.getRegisteredBusinessObjectDataList().size());
            {
                BusinessObjectData businessObjectData = actualResponse.getRegisteredBusinessObjectDataList().get(0);
                Assert.assertEquals("response business object data[0] version", 0, businessObjectData.getVersion());
                Assert.assertEquals("response business object data[0] status", BusinessObjectDataInvalidateUnregisteredHelper.UNREGISTERED_STATUS,
                    businessObjectData.getStatus());
                Assert.assertNotNull("response business object data[0] storage units is null", businessObjectData.getStorageUnits());
                Assert.assertEquals("response business object data[0] storage units size", 1, businessObjectData.getStorageUnits().size());
                {
                    String expectedS3KeyPrefix =
                        businessObjectDataHelper.buildS3KeyPrefix(businessObjectFormatEntity, businessObjectDataHelper
                            .createBusinessObjectDataKey(businessObjectData));
                    StorageUnit storageUnit = businessObjectData.getStorageUnits().get(0);
                    Assert.assertNotNull("response business object data[0] storage unit[0] storage directory is null", storageUnit.getStorageDirectory());
                    Assert.assertEquals("response business object data[0] storage unit[0] storage directory path", expectedS3KeyPrefix, storageUnit
                        .getStorageDirectory().getDirectoryPath());
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail("unexpected exception thrown: " + e);
        }
    }

    /**
     * Test case where S3 has 2 objects, and DM has 1 object registered.
     * Expects one new registration in INVALID status.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataS32DM1()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();

        // Given a business object format
        // Given 1 business object data registered
        // Given 2 S3 objects
        BusinessObjectFormatEntity businessObjectFormatEntity;
        try
        {
            businessObjectFormatEntity = createBusinessObjectFormat(request);

            createBusinessObjectData(businessObjectFormatEntity, request, 0, true);
            createS3Object(businessObjectFormatEntity, request, 0);
            createS3Object(businessObjectFormatEntity, request, 1);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Call API
        try
        {
            BusinessObjectDataInvalidateUnregisteredResponse actualResponse =
                businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);

            // Make assertions
            Assert.assertNotNull("response business object datas is null", actualResponse.getRegisteredBusinessObjectDataList());
            Assert.assertEquals("response business object datas size", 1, actualResponse.getRegisteredBusinessObjectDataList().size());
            {
                BusinessObjectData businessObjectData = actualResponse.getRegisteredBusinessObjectDataList().get(0);
                Assert.assertEquals("response business object data[0] version", 1, businessObjectData.getVersion());
                Assert.assertEquals("response business object data[0] status", BusinessObjectDataInvalidateUnregisteredHelper.UNREGISTERED_STATUS,
                    businessObjectData.getStatus());
                Assert.assertNotNull("response business object data[0] storage units is null", businessObjectData.getStorageUnits());
                Assert.assertEquals("response business object data[0] storage units size", 1, businessObjectData.getStorageUnits().size());
                {
                    String expectedS3KeyPrefix =
                        businessObjectDataHelper.buildS3KeyPrefix(businessObjectFormatEntity, businessObjectDataHelper
                            .createBusinessObjectDataKey(businessObjectData));
                    StorageUnit storageUnit = businessObjectData.getStorageUnits().get(0);
                    Assert.assertNotNull("response business object data[0] storage unit[0] storage directory is null", storageUnit.getStorageDirectory());
                    Assert.assertEquals("response business object data[0] storage unit[0] storage directory path", expectedS3KeyPrefix, storageUnit
                        .getStorageDirectory().getDirectoryPath());
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail("unexpected exception thrown: " + e);
        }
    }

    /**
     * Test case where S3 has 2 objects, but DM has no object registered.
     * Expects 2 new registrations in INVALID status.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataS32DM0()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();

        // Given a business object format
        // Given 1 business object data registered
        // Given 2 S3 objects
        BusinessObjectFormatEntity businessObjectFormatEntity;
        try
        {
            businessObjectFormatEntity = createBusinessObjectFormat(request);

            createS3Object(businessObjectFormatEntity, request, 0);
            createS3Object(businessObjectFormatEntity, request, 1);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Call API
        try
        {
            BusinessObjectDataInvalidateUnregisteredResponse actualResponse =
                businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);

            // Make assertions
            Assert.assertNotNull("response business object datas is null", actualResponse.getRegisteredBusinessObjectDataList());
            Assert.assertEquals("response business object datas size", 2, actualResponse.getRegisteredBusinessObjectDataList().size());
            // Assert first data registered
            {
                BusinessObjectData businessObjectData = actualResponse.getRegisteredBusinessObjectDataList().get(0);
                Assert.assertEquals("response business object data[0] version", 0, businessObjectData.getVersion());
                Assert.assertEquals("response business object data[0] status", BusinessObjectDataInvalidateUnregisteredHelper.UNREGISTERED_STATUS,
                    businessObjectData.getStatus());
                Assert.assertNotNull("response business object data[0] storage units is null", businessObjectData.getStorageUnits());
                Assert.assertEquals("response business object data[0] storage units size", 1, businessObjectData.getStorageUnits().size());
                {
                    String expectedS3KeyPrefix =
                        businessObjectDataHelper.buildS3KeyPrefix(businessObjectFormatEntity, businessObjectDataHelper
                            .createBusinessObjectDataKey(businessObjectData));
                    StorageUnit storageUnit = businessObjectData.getStorageUnits().get(0);
                    Assert.assertNotNull("response business object data[0] storage unit[0] storage directory is null", storageUnit.getStorageDirectory());
                    Assert.assertEquals("response business object data[0] storage unit[0] storage directory path", expectedS3KeyPrefix, storageUnit
                        .getStorageDirectory().getDirectoryPath());
                }
            }
            // Assert second data registered
            {
                BusinessObjectData businessObjectData = actualResponse.getRegisteredBusinessObjectDataList().get(1);
                Assert.assertEquals("response business object data[1] version", 1, businessObjectData.getVersion());
                Assert.assertEquals("response business object data[1] status", BusinessObjectDataInvalidateUnregisteredHelper.UNREGISTERED_STATUS,
                    businessObjectData.getStatus());
                Assert.assertNotNull("response business object data[1] storage units is null", businessObjectData.getStorageUnits());
                Assert.assertEquals("response business object data[1] storage units size", 1, businessObjectData.getStorageUnits().size());
                {
                    String expectedS3KeyPrefix =
                        businessObjectDataHelper.buildS3KeyPrefix(businessObjectFormatEntity, businessObjectDataHelper
                            .createBusinessObjectDataKey(businessObjectData));
                    StorageUnit storageUnit = businessObjectData.getStorageUnits().get(0);
                    Assert.assertNotNull("response business object data[1] storage unit[0] storage directory is null", storageUnit.getStorageDirectory());
                    Assert.assertEquals("response business object data[1] storage unit[0] storage directory path", expectedS3KeyPrefix, storageUnit
                        .getStorageDirectory().getDirectoryPath());
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail("unexpected exception thrown: " + e);
        }
    }

    /**
     * Test case where S3 has 1 object, and DM has no object registered.
     * The S3 object is registered under version 1 so there is a gap for version 0 of registration.
     * Expects no new registrations since the API does not consider the S3 objects after a gap.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataS31DM0WithGap()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();
        request.setSubPartitionValues(SUBPARTITION_VALUES);

        // Given a business object format
        // Given an object in S3
        BusinessObjectFormatEntity businessObjectFormatEntity;
        try
        {
            businessObjectFormatEntity = createBusinessObjectFormat(request);

            createS3Object(businessObjectFormatEntity, request, 1);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Call API
        try
        {
            BusinessObjectDataInvalidateUnregisteredResponse actualResponse =
                businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);

            // Make assertions
            Assert.assertNotNull("response business object datas is null", actualResponse.getRegisteredBusinessObjectDataList());
            Assert.assertEquals("response business object datas size", 0, actualResponse.getRegisteredBusinessObjectDataList().size());
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail("unexpected exception thrown: " + e);
        }
    }

    /**
     * The prefix search for S3 object should match prefixed directories, not sub-strings.
     * For example:
     * - If a S3 object exists with key "c/b/aa/test.txt"
     * - If a search for prefix "c/b/a" is executed
     * - The S3 object should NOT match, since it is a prefix, but not a prefix directory.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataS3PrefixWithSlash()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();

        // Given a business object format
        // Given an object in S3
        BusinessObjectFormatEntity businessObjectFormatEntity;
        try
        {
            businessObjectFormatEntity = createBusinessObjectFormat(request);

            request.setPartitionValue("AA"); // Create S3 object which is contains the partition value as substring
            createS3Object(businessObjectFormatEntity, request, 0);

            request.setPartitionValue("A"); // Send request with substring
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Call API
        try
        {
            BusinessObjectDataInvalidateUnregisteredResponse actualResponse =
                businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);

            // Make assertions, expect no data updates since nothing should match
            Assert.assertNotNull("response business object datas is null", actualResponse.getRegisteredBusinessObjectDataList());
            Assert.assertEquals("response business object datas size", 0, actualResponse.getRegisteredBusinessObjectDataList().size());
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail("unexpected exception thrown: " + e);
        }
    }

    /**
     * Asserts that namespace requiredness validation is working.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataValidationNamespaceRequired()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();
        request.setNamespace(BLANK_TEXT);

        // Given a business object format
        try
        {
            createBusinessObjectFormat(request);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Call the API
        try
        {
            businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);
            Assert.fail("expected a IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "The namespace is required", e.getMessage());
        }
    }

    /**
     * Asserts that business object definition name requiredness validation is working.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataValidationBusinessObjectDefinitionNameRequired()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();
        request.setBusinessObjectDefinitionName(BLANK_TEXT);

        // Given a business object format
        try
        {
            createBusinessObjectFormat(request);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Call the API
        try
        {
            businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);
            Assert.fail("expected a IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "The business object definition name is required", e.getMessage());
        }
    }

    /**
     * Asserts that business object format usage requiredness validation is working.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataValidationBusinessObjectFormatUsageRequired()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();
        request.setBusinessObjectFormatUsage(BLANK_TEXT);

        // Given a business object format
        try
        {
            createBusinessObjectFormat(request);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Call the API
        try
        {
            businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);
            Assert.fail("expected a IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "The business object format usage is required", e.getMessage());
        }
    }

    /**
     * Business object format must exist for this API to work
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataValidationBusinessObjectFormatMustExist()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();

        // Given a business object format
        try
        {
            createBusinessObjectFormat(request);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Modify a parameter specific to a format to reference a format that does not exist
        request.setBusinessObjectFormatFileType("DOES_NOT_EXIST");

        // Call the API
        try
        {
            businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);
            Assert.fail("expected a ObjectNotFoundException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", ObjectNotFoundException.class, e.getClass());
            Assert
                .assertEquals(
                    "thrown exception message", "Business object format with namespace \"" + request.getNamespace() + "\", business object definition name \""
                        + request.getBusinessObjectDefinitionName() + "\", format usage \"" + request.getBusinessObjectFormatUsage()
                        + "\", format file type \"" + request.getBusinessObjectFormatFileType() + "\", and format version \""
                        + request.getBusinessObjectFormatVersion() + "\" doesn't exist.", e.getMessage());
        }
    }

    /**
     * Asserts that business object format file type requiredness validation is working.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataValidationBusinessObjectFormatFileTypeRequired()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();
        request.setBusinessObjectFormatFileType(BLANK_TEXT);

        // Given a business object format
        try
        {
            createBusinessObjectFormat(request);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Call the API
        try
        {
            businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);
            Assert.fail("expected a IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "The business object format file type is required", e.getMessage());
        }
    }

    /**
     * Asserts that business object format version requiredness validation is working.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataValidationBusinessObjectFormatVersionRequired()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();

        // Given a business object format
        try
        {
            createBusinessObjectFormat(request);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // nullify version after format is created so that the format is created correctly.
        request.setBusinessObjectFormatVersion(null);

        // Call the API
        try
        {
            businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);
            Assert.fail("expected a IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "The business object format version is required", e.getMessage());
        }
    }

    /**
     * Asserts that business object format version positive validation is working.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataValidationBusinessObjectFormatVersionNegative()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();
        request.setBusinessObjectFormatVersion(-1);

        // Given a business object format
        try
        {
            createBusinessObjectFormat(request);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Call the API
        try
        {
            businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);
            Assert.fail("expected a IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "The business object format version must be greater than or equal to 0", e.getMessage());
        }
    }

    /**
     * Asserts that partition value requiredness validation is working.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataValidationPartitionValueRequired()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();
        request.setPartitionValue(BLANK_TEXT);

        // Given a business object format
        try
        {
            createBusinessObjectFormat(request);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Call the API
        try
        {
            businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);
            Assert.fail("expected a IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "The partition value is required", e.getMessage());
        }
    }

    /**
     * Asserts that storage name requiredness validation is working.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataValidationStorageNameRequired()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();
        request.setStorageName(BLANK_TEXT);

        // Given a business object format
        try
        {
            createBusinessObjectFormat(request);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Call the API
        try
        {
            businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);
            Assert.fail("expected a IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "The storage name is required", e.getMessage());
        }
    }

    /**
     * Storage must exist for this API to work.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataValidationStorageMustExist()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();
        request.setStorageName("DOES_NOT_EXIST");

        // Given a business object format
        try
        {
            createBusinessObjectFormat(request);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Call the API
        try
        {
            businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);
            Assert.fail("expected a ObjectNotFoundException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", ObjectNotFoundException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "Storage with name \"" + request.getStorageName() + "\" doesn't exist.", e.getMessage());
        }
    }

    /**
     * Storage is found, but the storage platform is not S3.
     * This API only works for S3 platforms since it requires S3 key prefix.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataValidationStoragePlatformMustBeS3()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();
        request.setStorageName(STORAGE_NAME);

        // Given a business object format
        try
        {
            createStorageEntity(request.getStorageName(), "NOT_S3");
            createBusinessObjectFormat(request);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Call the API
        try
        {
            businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);
            Assert.fail("expected a IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "The specified storage '" + request.getStorageName() + "' is not a S3 storage platform.", e
                .getMessage());
        }
    }

    /**
     * If sub-partition values are given, they must not be blank.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataValidationSubPartitionValueNotBlank()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();
        request.setSubPartitionValues(Arrays.asList(BLANK_TEXT));

        // Given a business object format
        try
        {
            createBusinessObjectFormat(request);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // Call the API
        try
        {
            businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);
            Assert.fail("expected a IllegalArgumentException, but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", IllegalArgumentException.class, e.getClass());
            Assert.assertEquals("thrown exception message", "The sub-partition value [0] must not be blank", e.getMessage());
        }
    }

    /**
     * Asserts that values are trimmed before the request is processed.
     */
    @Test
    public void testInvalidateUnregisteredBusinessObjectDataTrim()
    {
        LOGGER.debug("start");

        BusinessObjectDataInvalidateUnregisteredRequest request = getDefaultBusinessObjectDataInvalidateUnregisteredRequest();
        request.setSubPartitionValues(SUBPARTITION_VALUES);

        // Given a business object format
        try
        {
            createBusinessObjectFormat(request);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Test failed during setup. Most likely setup or developer error.", e);
        }

        // pad string values with white spaces
        request.setNamespace(BLANK_TEXT + request.getNamespace() + BLANK_TEXT);
        request.setBusinessObjectDefinitionName(BLANK_TEXT + request.getBusinessObjectDefinitionName() + BLANK_TEXT);
        request.setBusinessObjectFormatFileType(BLANK_TEXT + request.getBusinessObjectFormatFileType() + BLANK_TEXT);
        request.setBusinessObjectFormatUsage(BLANK_TEXT + request.getBusinessObjectFormatUsage() + BLANK_TEXT);
        request.setPartitionValue(BLANK_TEXT + request.getPartitionValue() + BLANK_TEXT);
        List<String> paddedSubPartitionValues = new ArrayList<>();
        for (String subPartitionValue : request.getSubPartitionValues())
        {
            paddedSubPartitionValues.add(BLANK_TEXT + subPartitionValue + BLANK_TEXT);
        }
        request.setSubPartitionValues(paddedSubPartitionValues);

        // Call the API
        try
        {
            BusinessObjectDataInvalidateUnregisteredResponse actualResponse =
                businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(request);

            // Make assertions
            /*
             * Note: The API will modify the request to now contain the trimmed value.
             */
            Assert.assertNotNull("response is null", actualResponse);
            Assert.assertEquals("response namespace", request.getNamespace(), actualResponse.getNamespace());
            Assert.assertEquals("response business object definition name", request.getBusinessObjectDefinitionName(), actualResponse
                .getBusinessObjectDefinitionName());
            Assert.assertEquals("response business object format usage", request.getBusinessObjectFormatUsage(), actualResponse.getBusinessObjectFormatUsage());
            Assert.assertEquals("response business object format file type", request.getBusinessObjectFormatFileType(), actualResponse
                .getBusinessObjectFormatFileType());
            Assert.assertEquals("response business object format version", request.getBusinessObjectFormatVersion(), actualResponse
                .getBusinessObjectFormatVersion());
            Assert.assertEquals("response partition value", request.getPartitionValue(), actualResponse.getPartitionValue());
            Assert.assertEquals("response sub-partition values", request.getSubPartitionValues(), actualResponse.getSubPartitionValues());
            Assert.assertEquals("response storage name", request.getStorageName(), actualResponse.getStorageName());
            Assert.assertNotNull("response business object datas is null", actualResponse.getRegisteredBusinessObjectDataList());
            Assert.assertEquals("response business object datas size", 0, actualResponse.getRegisteredBusinessObjectDataList().size());
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail("unexpected exception thrown: " + e);
        }
    }

    /**
     * Creates an object in S3 with the prefix constructed from the given parameters.
     * The object's full path will be {prefix}/{UUID}
     * 
     * @param businessObjectFormatEntity business object format
     * @param request request with partition values and storage
     * @param businessObjectDataVersion business object data version to put
     */
    private void createS3Object(BusinessObjectFormatEntity businessObjectFormatEntity, BusinessObjectDataInvalidateUnregisteredRequest request,
        int businessObjectDataVersion)
    {
        StorageEntity storageEntity = dmDao.getStorageByName(request.getStorageName());
        String s3BucketName = dmDaoHelper.getS3BucketAccessParams(storageEntity).getS3BucketName();

        BusinessObjectDataKey businessObjectDataKey = getBusinessObjectDataKey(request);
        businessObjectDataKey.setBusinessObjectDataVersion(businessObjectDataVersion);

        String s3KeyPrefix = businessObjectDataHelper.buildS3KeyPrefix(businessObjectFormatEntity, businessObjectDataKey);
        String s3ObjectKey = s3KeyPrefix + "/test";
        PutObjectRequest putObjectRequest = new PutObjectRequest(s3BucketName, s3ObjectKey, new ByteArrayInputStream(new byte[1]), new ObjectMetadata());
        s3Operations.putObject(putObjectRequest, null);
    }

    /**
     * Gets the {@link BusinessObjectDataKey} from the given request.
     * 
     * @param request {@link BusinessObjectDataInvalidateUnregisteredRequest}
     * @return {@link BusinessObjectDataKey} minus the version
     */
    private BusinessObjectDataKey getBusinessObjectDataKey(BusinessObjectDataInvalidateUnregisteredRequest request)
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();
        businessObjectDataKey.setNamespace(request.getNamespace());
        businessObjectDataKey.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName());
        businessObjectDataKey.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage());
        businessObjectDataKey.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType());
        businessObjectDataKey.setBusinessObjectFormatVersion(request.getBusinessObjectFormatVersion());
        businessObjectDataKey.setPartitionValue(request.getPartitionValue());
        businessObjectDataKey.setSubPartitionValues(request.getSubPartitionValues());
        return businessObjectDataKey;
    }

    /**
     * Creates and persists a {@link BusinessObjectDataEntity} with the specified parameters.
     * 
     * @param businessObjectFormatEntity {@link BusinessObjectFormatEntity}
     * @param request {@link BusinessObjectDataInvalidateUnregisteredRequest} with bdata alt key
     * @param businessObjectDataVersion bdata version
     * @param latestVersion is this data the latest version?
     * @return the created {@link BusinessObjectDataEntity}
     */
    private BusinessObjectDataEntity createBusinessObjectData(BusinessObjectFormatEntity businessObjectFormatEntity,
        BusinessObjectDataInvalidateUnregisteredRequest request, int businessObjectDataVersion, boolean latestVersion)
    {
        BusinessObjectDataEntity businessObjectDataEntity = new BusinessObjectDataEntity();
        businessObjectDataEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataEntity.setPartitionValue(request.getPartitionValue());
        businessObjectDataEntity.setPartitionValue2(dmCollectionHelper.safeGet(request.getSubPartitionValues(), 0));
        businessObjectDataEntity.setPartitionValue3(dmCollectionHelper.safeGet(request.getSubPartitionValues(), 1));
        businessObjectDataEntity.setPartitionValue4(dmCollectionHelper.safeGet(request.getSubPartitionValues(), 2));
        businessObjectDataEntity.setPartitionValue5(dmCollectionHelper.safeGet(request.getSubPartitionValues(), 3));
        businessObjectDataEntity.setVersion(businessObjectDataVersion);
        businessObjectDataEntity.setLatestVersion(latestVersion);
        businessObjectDataEntity.setStatus(dmDao.getBusinessObjectDataStatusByCode(BusinessObjectDataStatusEntity.VALID));
        dmDao.saveAndRefresh(businessObjectDataEntity);
        return businessObjectDataEntity;
    }
}
