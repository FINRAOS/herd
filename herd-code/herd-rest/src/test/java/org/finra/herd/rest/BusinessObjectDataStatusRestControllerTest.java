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

import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusInformation;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateResponse;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;

/**
 * This class tests various functionality within the business object data status REST controller.
 */
public class BusinessObjectDataStatusRestControllerTest extends AbstractRestTest
{
    @Test
    public void testGetBusinessObjectDataStatus()
    {
        // Create and persist database entities required for testing.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

        // Get the business object data status information.
        BusinessObjectDataStatusInformation resultBusinessObjectDataStatusInformation = businessObjectDataStatusRestController
            .getBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, PARTITION_VALUE,
                getDelimitedFieldValues(SUBPARTITION_VALUES), FORMAT_VERSION, DATA_VERSION);

        // Validate the returned object.
        validateBusinessObjectDataStatusInformation(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS, resultBusinessObjectDataStatusInformation);
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
            businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, true, BDATA_STATUS);

            // Get the business object data status information without specifying optional parameters.
            BusinessObjectDataStatusInformation resultBusinessObjectDataStatusInformation = businessObjectDataStatusRestController
                .getBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, BLANK_TEXT, PARTITION_VALUE,
                    getDelimitedFieldValues(subPartitionValues), null, null);

            // Validate the returned object.
            validateBusinessObjectDataStatusInformation(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subPartitionValues,
                    DATA_VERSION), BDATA_STATUS, resultBusinessObjectDataStatusInformation);
        }
    }

    @Test
    public void testGetBusinessObjectDataStatusMissingOptionalParametersPassedAsNulls()
    {
        // Create and persist a business object data entity without sub-partition values.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

        // Get the business object data status by passing null values for the partition key and the list of sub-partition values.
        BusinessObjectDataStatusInformation resultBusinessObjectDataStatusInformation = businessObjectDataStatusRestController
            .getBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, null, null, null);

        // Validate the returned object.
        validateBusinessObjectDataStatusInformation(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, NO_SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS, resultBusinessObjectDataStatusInformation);
    }

    @Test
    public void testUpdateBusinessObjectDataStatus()
    {
        // Create and persist relative test entities.
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS_2);

        // Update the business object data status.
        BusinessObjectDataStatusUpdateResponse response = businessObjectDataStatusRestController
            .updateBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION,
                createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));

        // Validate the returned object.
        validateBusinessObjectDataStatusUpdateResponse(
            new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION), BDATA_STATUS_2, BDATA_STATUS, response);
    }

    @Test
    public void testUpdateBusinessObjectDataStatusMissingOptionalParameters()
    {
        // Create and persist a business object data status entity.
        businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS_2);

        // Test if we can retrieve an attribute for the business object data with any allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Create and persist a business object data entity.
            businessObjectDataDaoTestHelper
                .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, true, BDATA_STATUS);

            // Update the business object data status using the relative endpoint.
            BusinessObjectDataStatusUpdateResponse response = null;

            switch (i)
            {
                case 0:
                    response = businessObjectDataStatusRestController
                        .updateBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            DATA_VERSION, createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
                case 1:
                    response = businessObjectDataStatusRestController
                        .updateBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), DATA_VERSION, createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
                case 2:
                    response = businessObjectDataStatusRestController
                        .updateBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), DATA_VERSION, createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
                case 3:
                    response = businessObjectDataStatusRestController
                        .updateBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), DATA_VERSION,
                            createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
                case 4:
                    response = businessObjectDataStatusRestController
                        .updateBusinessObjectDataStatus(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), subPartitionValues.get(3), DATA_VERSION,
                            createBusinessObjectDataStatusUpdateRequest(BDATA_STATUS_2));
                    break;
            }

            // Validate the returned object.
            validateBusinessObjectDataStatusUpdateResponse(
                new BusinessObjectDataKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, subPartitionValues,
                    DATA_VERSION), BDATA_STATUS_2, BDATA_STATUS, response);
        }
    }
}
