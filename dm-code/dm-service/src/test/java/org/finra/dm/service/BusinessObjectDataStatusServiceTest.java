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
package org.finra.dm.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.model.api.xml.BusinessObjectDataStatusInformation;
import org.finra.dm.model.api.xml.BusinessObjectDataStatusUpdateResponse;

/**
 * This class tests various functionality within the business object data status REST controller.
 */
public class BusinessObjectDataStatusServiceTest extends AbstractServiceTest
{
    @Test
    public void testGetBusinessObjectDataStatus()
    {
        // Create and persist database entities required for testing.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Get the business object data status information.
        BusinessObjectDataStatusInformation resultBusinessObjectDataStatusInformation = businessObjectDataService.getBusinessObjectDataStatus(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), PARTITION_KEY);

        // Validate the returned object.
        validateBusinessObjectDataStatusInformation(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS, resultBusinessObjectDataStatusInformation);
    }

    @Test
    public void testGetBusinessObjectDataStatusMissingRequiredParameters()
    {
        // Create and persist database entities required for testing.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Try to get the business object data status when namespace is not specified.
        try
        {
            businessObjectDataService.getBusinessObjectDataStatus(
                new BusinessObjectDataKey(BLANK_TEXT, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY);
            fail("Should throw an IllegalArgumentException when namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to get the business object data status information when business object definition name is not specified.
        try
        {
            businessObjectDataService.getBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), PARTITION_KEY);
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to get the business object data status information when business object format usage is not specified.
        try
        {
            businessObjectDataService.getBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY);
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to update the business object data status information when business object format file type is not specified.
        try
        {
            businessObjectDataService.getBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY);
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to update the business object data status information when partition value is not specified.
        try
        {
            businessObjectDataService.getBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY);
            fail("Should throw an IllegalArgumentException when partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }

        // Try to update the business object data status information when subpartition value is not specified.
        try
        {
            businessObjectDataService.getBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(BLANK_TEXT), DATA_VERSION), PARTITION_KEY);
            fail("Should throw an IllegalArgumentException when subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataStatusMissingOptionalParameters()
    {
        // Test if we can get status for the business object data without specifying optional parameters
        // and with any allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Create and persist a business object data entity.
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                subPartitionValues, DATA_VERSION, true, BDATA_STATUS);

            // Get the business object data status information without specifying optional parameters.
            BusinessObjectDataStatusInformation resultBusinessObjectDataStatusInformation = businessObjectDataService.getBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, subPartitionValues, null),
                BLANK_TEXT);

            // Validate the returned object.
            validateBusinessObjectDataStatusInformation(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subPartitionValues,
                    DATA_VERSION), BDATA_STATUS, resultBusinessObjectDataStatusInformation);
        }
    }

    @Test
    public void testGetBusinessObjectDataStatusMissingOptionalParametersPassedAsNulls()
    {
        // Create and persist a business object data entity without sub-partition values.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            NO_SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

        // Get the business object data status by passing null value for the partition key.
        BusinessObjectDataStatusInformation resultBusinessObjectDataStatusInformation = businessObjectDataService.getBusinessObjectDataStatus(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, NO_SUBPARTITION_VALUES, null),
            null);

        // Validate the returned object.
        validateBusinessObjectDataStatusInformation(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS, resultBusinessObjectDataStatusInformation);
    }

    @Test
    public void testGetBusinessObjectDataStatusTrimParameters()
    {
        // Create and persist database entities required for testing.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Get the business object data status information using input parameters with leading and trailing empty spaces.
        BusinessObjectDataStatusInformation resultBusinessObjectDataStatusInformation = businessObjectDataService.getBusinessObjectDataStatus(
            new BusinessObjectDataKey(addWhitespace(NAMESPACE_CD), addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUBPARTITION_VALUES), DATA_VERSION),
            addWhitespace(PARTITION_KEY));

        // Validate the returned object.
        validateBusinessObjectDataStatusInformation(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS, resultBusinessObjectDataStatusInformation);
    }

    @Test
    public void testGetBusinessObjectDataStatusUpperCaseParameters()
    {
        // Create and persist database entities required for testing.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Get the business object data status using upper case input parameters (except for case-sensitive partition values).
        BusinessObjectDataStatusInformation resultBusinessObjectDataStatusInformation = businessObjectDataService.getBusinessObjectDataStatus(
            new BusinessObjectDataKey(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION), PARTITION_KEY.toUpperCase());

        // Validate the returned object.
        validateBusinessObjectDataStatusInformation(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS, resultBusinessObjectDataStatusInformation);
    }

    @Test
    public void testGetBusinessObjectDataStatusLowerCaseParameters()
    {
        // Create and persist database entities required for testing.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Get the business object data status using lower case input parameters (except for case-sensitive partition values).
        BusinessObjectDataStatusInformation resultBusinessObjectDataStatusInformation = businessObjectDataService.getBusinessObjectDataStatus(
            new BusinessObjectDataKey(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION), PARTITION_KEY.toLowerCase());

        // Validate the returned object.
        validateBusinessObjectDataStatusInformation(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS, resultBusinessObjectDataStatusInformation);
    }

    @Test
    public void testGetBusinessObjectDataStatusInvalidParameters()
    {
        // Create and persist a valid business object data.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Get the business object data status information.
        BusinessObjectDataStatusInformation resultBusinessObjectDataStatusInformation = businessObjectDataService.getBusinessObjectDataStatus(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), PARTITION_KEY);

        // Validate the returned object.
        validateBusinessObjectDataStatusInformation(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS, resultBusinessObjectDataStatusInformation);

        // Try to perform a get business object data status using invalid business object definition name.
        try
        {
            businessObjectDataService.getBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), PARTITION_KEY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, "I_DO_NOT_EXIST", FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }

        // Try to perform a get business object data status using invalid format usage.
        try
        {
            businessObjectDataService.getBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, "I_DO_NOT_EXIST", FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }

        // Try to perform a get business object data status using invalid format file type.
        try
        {
            businessObjectDataService.getBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), PARTITION_KEY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, "I_DO_NOT_EXIST", FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }

        // Try to perform a get business object data status using invalid partition key.
        try
        {
            businessObjectDataService.getBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), "I_DO_NOT_EXIST");
            fail("Should throw an IllegalArgumentException when using an invalid partition key.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(
                String.format("Partition key \"%s\" doesn't match configured business object format partition key \"%s\".", "I_DO_NOT_EXIST", PARTITION_KEY),
                e.getMessage());
        }

        // Try to perform a get business object data status using invalid partition value.
        try
        {
            businessObjectDataService.getBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, "I_DO_NOT_EXIST",
                    SUBPARTITION_VALUES, DATA_VERSION), PARTITION_KEY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                "I_DO_NOT_EXIST", SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }

        // Try to perform a get business object data status using invalid subpartition value.
        for (int i = 0; i < SUBPARTITION_VALUES.size(); i++)
        {
            List<String> testSubPartitionValues = new ArrayList<>();
            try
            {
                testSubPartitionValues = new ArrayList<>(SUBPARTITION_VALUES);
                testSubPartitionValues.set(i, "I_DO_NOT_EXIST");
                businessObjectDataService.getBusinessObjectDataStatus(
                    new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        testSubPartitionValues, DATA_VERSION), PARTITION_KEY);
                fail("Should throw an ObjectNotFoundException when not able to find business object data.");
            }
            catch (ObjectNotFoundException e)
            {
                assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, testSubPartitionValues, DATA_VERSION, null), e.getMessage());
            }
        }

        // Try to perform a get business object data status using too many subpartition values.
        try
        {
            List<String> testSubPartitionValues = new ArrayList<>(SUBPARTITION_VALUES);
            testSubPartitionValues.add("EXTRA_SUBPARTITION_VALUE");
            businessObjectDataService.getBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    testSubPartitionValues, DATA_VERSION), PARTITION_KEY);
            fail("Should throw an IllegalArgumentException when passing too many subpartition values.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Exceeded maximum number of allowed subpartitions: %d.", BusinessObjectDataEntity.MAX_SUBPARTITIONS), e.getMessage());
        }

        // Try to perform a get business object data status using invalid business object format version.
        try
        {
            businessObjectDataService.getBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), PARTITION_KEY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INVALID_FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }

        // Try to perform a get business object data status using invalid business object data version.
        try
        {
            businessObjectDataService.getBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, INVALID_DATA_VERSION), PARTITION_KEY);
            fail("Should throw an ObjectNotFoundException when not able to find business object data.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, INVALID_DATA_VERSION, null), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataStatusBusinessObjectDataNoExists()
    {
        // Try to get status for a non-existing business object data.
        try
        {
            businessObjectDataService.getBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), PARTITION_KEY);

            fail("Should throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDataStatus()
    {
        // Create and persist relative test entities.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);
        createBusinessObjectDataStatusEntity(BDATA_STATUS_2);

        // Update the business object data status.
        BusinessObjectDataStatusUpdateResponse response = businessObjectDataService.updateBusinessObjectDataStatus(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));

        // Validate the returned object.
        validateBusinessObjectDataStatusUpdateResponse(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS_2, BDATA_STATUS, response);
    }

    @Test
    public void testUpdateBusinessObjectDataStatusLegacy()
    {
        // Create and persist a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create and persist relative test entities.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);
        createBusinessObjectDataStatusEntity(BDATA_STATUS_2);

        // Update the business object data status by calling a legacy endpoint.
        BusinessObjectDataStatusUpdateResponse response = businessObjectDataService.updateBusinessObjectDataStatus(
            new BusinessObjectDataKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));

        // Validate the returned object.
        validateBusinessObjectDataStatusUpdateResponse(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS_2, BDATA_STATUS, response);
    }

    @Test
    public void testUpdateBusinessObjectDataStatusMissingRequiredParameters()
    {
        // Try to update the business object data status when business object definition name is not specified.
        try
        {
            businessObjectDataService.updateBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to update the business object data status instance when business object format usage is not specified.
        try
        {
            businessObjectDataService.updateBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS));
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to update the business object data status when business object format file type is not specified.
        try
        {
            businessObjectDataService.updateBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS));
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to update the business object data status when business object format version is not specified.
        try
        {
            businessObjectDataService.updateBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION), createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS));
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to update the business object data status when partition value is not specified.
        try
        {
            businessObjectDataService.updateBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT, SUBPARTITION_VALUES,
                    DATA_VERSION), createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS));
            fail("Should throw an IllegalArgumentException when partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }

        // Try to update the business object data status without specifying 1st subpartition value.
        try
        {
            businessObjectDataService.updateBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(BLANK_TEXT, SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION),
                createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS));
            fail("Should throw an IllegalArgumentException when 1st subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to update the business object data status without specifying 2nd subpartition value.
        try
        {
            businessObjectDataService.updateBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), BLANK_TEXT, SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION),
                createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS));
            fail("Should throw an IllegalArgumentException when 2nd subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to update the business object data status without specifying 3rd subpartition value.
        try
        {
            businessObjectDataService.updateBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), BLANK_TEXT, SUBPARTITION_VALUES.get(3)), DATA_VERSION),
                createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS));
            fail("Should throw an IllegalArgumentException when 3rd subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to update the business object data status without specifying 4th subpartition value.
        try
        {
            businessObjectDataService.updateBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), BLANK_TEXT), DATA_VERSION),
                createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS));
            fail("Should throw an IllegalArgumentException when 4th subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to update the business object data status when status is not specified.
        try
        {
            businessObjectDataService.updateBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), createBusinessObjectDataStatusUpdateRequest(BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when business object status is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data status must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDataStatusMissingOptionalParametersLegacy()
    {
        // Create and persist a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create and persist a business object data status entity.
        createBusinessObjectDataStatusEntity(BDATA_STATUS_2);

        // Test if we can retrieve an attribute for the business object data with any allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Create and persist a business object data entity.
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                subPartitionValues, DATA_VERSION, true, BDATA_STATUS);

            // Update the business object data status using the relative legacy endpoint.
            BusinessObjectDataStatusUpdateResponse response = null;

            switch (i)
            {
                case 0:
                    response = businessObjectDataService.updateBusinessObjectDataStatus(
                        new BusinessObjectDataKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            NO_SUBPARTITION_VALUES, DATA_VERSION), createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
                case 1:
                    response = businessObjectDataService.updateBusinessObjectDataStatus(
                        new BusinessObjectDataKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0)), DATA_VERSION), createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
                case 2:
                    response = businessObjectDataService.updateBusinessObjectDataStatus(
                        new BusinessObjectDataKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1)), DATA_VERSION),
                        createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
                case 3:
                    response = businessObjectDataService.updateBusinessObjectDataStatus(
                        new BusinessObjectDataKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2)), DATA_VERSION),
                        createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
                case 4:
                    response = businessObjectDataService.updateBusinessObjectDataStatus(
                        new BusinessObjectDataKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            SUBPARTITION_VALUES, DATA_VERSION), createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
            }

            // Validate the returned object.
            validateBusinessObjectDataStatusUpdateResponse(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subPartitionValues,
                    DATA_VERSION), BDATA_STATUS_2, BDATA_STATUS, response);
        }
    }

    @Test
    public void testUpdateBusinessObjectDataStatusMissingOptionalParameters()
    {
        // Create and persist a business object data status entity.
        createBusinessObjectDataStatusEntity(BDATA_STATUS_2);

        // Test if we can retrieve an attribute for the business object data with any allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Create and persist a business object data entity.
            createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                subPartitionValues, DATA_VERSION, true, BDATA_STATUS);

            // Update the business object data status using the relative endpoint.
            BusinessObjectDataStatusUpdateResponse response = null;

            switch (i)
            {
                case 0:
                    response = businessObjectDataService.updateBusinessObjectDataStatus(
                        new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            NO_SUBPARTITION_VALUES, DATA_VERSION), createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
                case 1:
                    response = businessObjectDataService.updateBusinessObjectDataStatus(
                        new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0)), DATA_VERSION), createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
                case 2:
                    response = businessObjectDataService.updateBusinessObjectDataStatus(
                        new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1)), DATA_VERSION),
                        createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
                case 3:
                    response = businessObjectDataService.updateBusinessObjectDataStatus(
                        new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2)), DATA_VERSION),
                        createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
                case 4:
                    response = businessObjectDataService.updateBusinessObjectDataStatus(
                        new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            SUBPARTITION_VALUES, DATA_VERSION), createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
            }

            // Validate the returned object.
            validateBusinessObjectDataStatusUpdateResponse(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subPartitionValues,
                    DATA_VERSION), BDATA_STATUS_2, BDATA_STATUS, response);
        }
    }

    @Test
    public void testUpdateBusinessObjectDataStatusTrimParameters()
    {
        // Create and persist relative test entities.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);
        createBusinessObjectDataStatusEntity(BDATA_STATUS_2);

        // Update the business object data status.
        BusinessObjectDataStatusUpdateResponse response = businessObjectDataService.updateBusinessObjectDataStatus(
            new BusinessObjectDataKey(addWhitespace(NAMESPACE_CD), addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUBPARTITION_VALUES), DATA_VERSION),
            createBusinessObjectDataStatusUpdateRequest(addWhitespace(BDATA_STATUS_2)));

        // Validate the returned object.
        validateBusinessObjectDataStatusUpdateResponse(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS_2, BDATA_STATUS, response);
    }

    @Test
    public void testUpdateBusinessObjectDataStatusUpperCaseParameters()
    {
        // Create and persist relative test entities.
        createBusinessObjectDataEntity(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
            FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS.toLowerCase());
        createBusinessObjectDataStatusEntity(BDATA_STATUS_2.toLowerCase());

        // Update the business object data status using upper case input parameters (except for case-sensitive partition values).
        BusinessObjectDataStatusUpdateResponse response = businessObjectDataService.updateBusinessObjectDataStatus(
            new BusinessObjectDataKey(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION), createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2.toUpperCase()));

        // Validate the returned object.
        validateBusinessObjectDataStatusUpdateResponse(
            new BusinessObjectDataKey(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION), BDATA_STATUS_2.toLowerCase(), BDATA_STATUS.toLowerCase(), response);
    }

    @Test
    public void testUpdateBusinessObjectDataStatusLowerCaseParameters()
    {
        // Create and persist relative test entities.
        createBusinessObjectDataEntity(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
            FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS.toUpperCase());
        createBusinessObjectDataStatusEntity(BDATA_STATUS_2.toUpperCase());

        // Update the business object data status using lower case input parameters (except for case-sensitive partition values).
        BusinessObjectDataStatusUpdateResponse response = businessObjectDataService.updateBusinessObjectDataStatus(
            new BusinessObjectDataKey(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION), createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2.toLowerCase()));

        // Validate the returned object.
        validateBusinessObjectDataStatusUpdateResponse(
            new BusinessObjectDataKey(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION), BDATA_STATUS_2.toUpperCase(), BDATA_STATUS.toUpperCase(), response);
    }

    @Test
    public void testUpdateBusinessObjectDataStatusBusinessObjectDataNoExists()
    {
        // Try to update a business object data status using non-existing business object data.
        createBusinessObjectDataStatusEntity(BDATA_STATUS);
        try
        {
            businessObjectDataService.updateBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS));

            fail("Should throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDataStatusNoStatusChange()
    {
        // Create and persist relative test entities.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Update the business object data status.
        BusinessObjectDataStatusUpdateResponse response = businessObjectDataService.updateBusinessObjectDataStatus(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS));

        // Validate the returned object.
        validateBusinessObjectDataStatusUpdateResponse(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS, BDATA_STATUS, response);
    }

    @Test
    public void testUpdateBusinessObjectDataStatusStatusNoExists()
    {
        // Create a business object data entity.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Try to update a business object data status using non-existing business status.
        try
        {
            businessObjectDataService.updateBusinessObjectDataStatus(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION), createBusinessObjectDataStatusUpdateRequest("I_DO_NOT_EXIST"));

            fail("Should throw an ObjectNotFoundException when business object data status does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals("Business object data status \"I_DO_NOT_EXIST\" doesn't exist.", e.getMessage());
        }
    }
}
