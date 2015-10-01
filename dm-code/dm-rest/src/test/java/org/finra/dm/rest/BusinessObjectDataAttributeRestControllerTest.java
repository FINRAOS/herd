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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataAttribute;
import org.finra.dm.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.dm.model.api.xml.BusinessObjectDataAttributeKeys;
import org.finra.dm.service.helper.DmDaoHelper;

/**
 * This class tests various functionality within the business object data attribute REST controller.
 */
public class BusinessObjectDataAttributeRestControllerTest extends AbstractRestTest
{
    @Autowired
    protected DmDaoHelper dmDaoHelper;

    @Test
    public void testCreateBusinessObjectDataAttribute()
    {
        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Create a business object data attribute.
        BusinessObjectDataAttribute resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController.createBusinessObjectDataAttribute(
            createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1, resultBusinessObjectDataAttribute);
    }

    @Test
    public void testGetBusinessObjectDataAttribute()
    {
        // Create and persist a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        // Retrieve the business object data attribute.
        BusinessObjectDataAttribute resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
            .getBusinessObjectDataAttribute(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE);

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1,
            resultBusinessObjectDataAttribute);
    }

    @Test
    public void testGetBusinessObjectDataAttributeLegacy()
    {
        // Create a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create and persist a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        // Retrieve the business object data attribute.
        BusinessObjectDataAttribute resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
            .getBusinessObjectDataAttribute(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES.get(0),
                SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1,
            resultBusinessObjectDataAttribute);
    }

    @Test
    public void testGetBusinessObjectDataAttributeMissingOptionalParameters()
    {
        // Test if we can retrieve an attribute for the business object data with any allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Create and persist an attribute for the business object data with the relative number of subpartition values.
            BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
                createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

            // Retrieve the attribute of the business object data using the relative endpoint.
            BusinessObjectDataAttribute resultBusinessObjectDataAttribute = null;
            switch (i)
            {
                case 0:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttribute(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
                case 1:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttribute(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
                case 2:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttribute(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
                case 3:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttribute(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
                case 4:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttribute(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), subPartitionValues.get(3), DATA_VERSION,
                            ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
            }

            // Validate the returned object.
            validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, PARTITION_VALUE, subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1,
                resultBusinessObjectDataAttribute);
        }
    }

    @Test
    public void testGetBusinessObjectDataAttributeMissingOptionalParametersLegacy()
    {
        // Create a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Test if we can retrieve an attribute for the business object data with any allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Create and persist an attribute for the business object data with the relative number of subpartition values.
            BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
                createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

            // Retrieve the attribute of the business object data using the relative endpoint.
            BusinessObjectDataAttribute resultBusinessObjectDataAttribute = null;
            switch (i)
            {
                case 0:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttribute(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION,
                            ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
                case 1:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttribute(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
                case 2:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttribute(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
                case 3:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttribute(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
                case 4:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttribute(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), subPartitionValues.get(3), DATA_VERSION,
                            ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
            }

            // Validate the returned object.
            validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, PARTITION_VALUE, subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1,
                resultBusinessObjectDataAttribute);
        }
    }

    @Test
    public void testGetBusinessObjectDataAttributes()
    {
        // List of test business object data attribute names.
        List<String> testBusinessObjectDataAttributeNames = Arrays.asList(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_NAME_2_MIXED_CASE);

        // Create and persist a business object data attribute entities.
        for (String businessObjectDataAttributeName : testBusinessObjectDataAttributeNames)
        {
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, businessObjectDataAttributeName, ATTRIBUTE_VALUE_1);
        }

        // Retrieve a list of business object data attribute keys.
        BusinessObjectDataAttributeKeys resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeRestController
            .getBusinessObjectDataAttributes(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION);

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataAttributeKeys);
        assertEquals(testBusinessObjectDataAttributeNames.size(), resultBusinessObjectDataAttributeKeys.getBusinessObjectDataAttributeKeys().size());
        for (int i = 0; i < testBusinessObjectDataAttributeNames.size(); i++)
        {
            validateBusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, testBusinessObjectDataAttributeNames.get(i),
                resultBusinessObjectDataAttributeKeys.getBusinessObjectDataAttributeKeys().get(i));
        }
    }

    @Test
    public void testGetBusinessObjectDataAttributesLegacy()
    {
        // List of test business object data attribute names.
        List<String> testBusinessObjectDataAttributeNames = Arrays.asList(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_NAME_2_MIXED_CASE);

        // Create a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create and persist a business object data attribute entities.
        for (String businessObjectDataAttributeName : testBusinessObjectDataAttributeNames)
        {
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, businessObjectDataAttributeName, ATTRIBUTE_VALUE_1);
        }

        // Retrieve a list of business object data attribute keys.
        BusinessObjectDataAttributeKeys resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeRestController
            .getBusinessObjectDataAttributes(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES.get(0),
                SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION);

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataAttributeKeys);
        assertEquals(testBusinessObjectDataAttributeNames.size(), resultBusinessObjectDataAttributeKeys.getBusinessObjectDataAttributeKeys().size());
        for (int i = 0; i < testBusinessObjectDataAttributeNames.size(); i++)
        {
            validateBusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, testBusinessObjectDataAttributeNames.get(i),
                resultBusinessObjectDataAttributeKeys.getBusinessObjectDataAttributeKeys().get(i));
        }
    }

    @Test
    public void testGetBusinessObjectDataAttributesMissingOptionalParameters()
    {
        // List of test business object data attribute names.
        List<String> testBusinessObjectDataAttributeNames = Arrays.asList(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_NAME_2_MIXED_CASE);

        // Test if we can retrieve a list of attribute keys for the business object data
        // with any allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Create and persist business object data attribute entities with the relative number of subpartition values.
            for (String businessObjectDataAttributeName : testBusinessObjectDataAttributeNames)
            {
                createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, businessObjectDataAttributeName, ATTRIBUTE_VALUE_1);
            }

            // Retrieve the list of attribute keys for the business object data using the relative endpoint.
            BusinessObjectDataAttributeKeys resultBusinessObjectDataAttributeKeys = null;
            switch (i)
            {
                case 0:
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttributes(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            DATA_VERSION);
                    break;
                case 1:
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttributes(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            SUBPARTITION_VALUES.get(0), DATA_VERSION);
                    break;
                case 2:
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttributes(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), DATA_VERSION);
                    break;
                case 3:
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttributes(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), DATA_VERSION);
                    break;
                case 4:
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttributes(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION);
                    break;
            }

            // Validate the returned object.
            assertNotNull(resultBusinessObjectDataAttributeKeys);
            assertEquals(testBusinessObjectDataAttributeNames.size(), resultBusinessObjectDataAttributeKeys.getBusinessObjectDataAttributeKeys().size());
            for (int j = 0; j < testBusinessObjectDataAttributeNames.size(); j++)
            {
                validateBusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, testBusinessObjectDataAttributeNames.get(j),
                    resultBusinessObjectDataAttributeKeys.getBusinessObjectDataAttributeKeys().get(j));
            }
        }
    }

    @Test
    public void testGetBusinessObjectDataAttributesMissingOptionalParametersLegacy()
    {
        // List of test business object data attribute names.
        List<String> testBusinessObjectDataAttributeNames = Arrays.asList(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_NAME_2_MIXED_CASE);

        // Create a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Test if we can retrieve a list of attribute keys for the business object data
        // with any allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Create and persist business object data attribute entities with the relative number of subpartition values.
            for (String businessObjectDataAttributeName : testBusinessObjectDataAttributeNames)
            {
                createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, businessObjectDataAttributeName, ATTRIBUTE_VALUE_1);
            }

            // Retrieve the list of attribute keys for the business object data using the relative endpoint.
            BusinessObjectDataAttributeKeys resultBusinessObjectDataAttributeKeys = null;
            switch (i)
            {
                case 0:
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttributes(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION);
                    break;
                case 1:
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttributes(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            SUBPARTITION_VALUES.get(0), DATA_VERSION);
                    break;
                case 2:
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttributes(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), DATA_VERSION);
                    break;
                case 3:
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttributes(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), DATA_VERSION);
                    break;
                case 4:
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeRestController
                        .getBusinessObjectDataAttributes(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION);
                    break;
            }

            // Validate the returned object.
            assertNotNull(resultBusinessObjectDataAttributeKeys);
            assertEquals(testBusinessObjectDataAttributeNames.size(), resultBusinessObjectDataAttributeKeys.getBusinessObjectDataAttributeKeys().size());
            for (int j = 0; j < testBusinessObjectDataAttributeNames.size(); j++)
            {
                validateBusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, testBusinessObjectDataAttributeNames.get(j),
                    resultBusinessObjectDataAttributeKeys.getBusinessObjectDataAttributeKeys().get(j));
            }
        }
    }

    @Test
    public void testUpdateBusinessObjectDataAttribute()
    {
        // Create and persist a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        // Update the business object data attribute.
        BusinessObjectDataAttribute updatedBusinessObjectDataAttribute = businessObjectDataAttributeRestController
            .updateBusinessObjectDataAttribute(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE, createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_2,
            updatedBusinessObjectDataAttribute);
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeLegacy()
    {
        // Create a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create and persist a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        // Update the business object data attribute.
        BusinessObjectDataAttribute updatedBusinessObjectDataAttribute = businessObjectDataAttributeRestController
            .updateBusinessObjectDataAttribute(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES.get(0),
                SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE,
                createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_2,
            updatedBusinessObjectDataAttribute);
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeMissingOptionalParameters()
    {
        // Test if we can update an attribute for the business object data with any allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Create and persist an attribute for the business object data with the relative number of subpartition values.
            BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
                createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

            // Update the attribute of the business object data using the relative endpoint.
            BusinessObjectDataAttribute resultBusinessObjectDataAttribute = null;
            switch (i)
            {
                case 0:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .updateBusinessObjectDataAttribute(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
                case 1:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .updateBusinessObjectDataAttribute(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE,
                            createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
                case 2:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .updateBusinessObjectDataAttribute(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE,
                            createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
                case 3:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .updateBusinessObjectDataAttribute(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE,
                            createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
                case 4:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .updateBusinessObjectDataAttribute(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), subPartitionValues.get(3), DATA_VERSION,
                            ATTRIBUTE_NAME_1_MIXED_CASE, createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
            }

            // Validate the returned object.
            validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, PARTITION_VALUE, subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_2,
                resultBusinessObjectDataAttribute);
        }
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeMissingOptionalParametersLegacy()
    {
        // Create a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Test if we can update an attribute for the business object data with any allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Create and persist an attribute for the business object data with the relative number of subpartition values.
            BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
                createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

            // Update the attribute of the business object data using the relative endpoint.
            BusinessObjectDataAttribute resultBusinessObjectDataAttribute = null;
            switch (i)
            {
                case 0:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .updateBusinessObjectDataAttribute(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION,
                            ATTRIBUTE_NAME_1_MIXED_CASE, createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
                case 1:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .updateBusinessObjectDataAttribute(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE,
                            createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
                case 2:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .updateBusinessObjectDataAttribute(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE,
                            createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
                case 3:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .updateBusinessObjectDataAttribute(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE,
                            createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
                case 4:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .updateBusinessObjectDataAttribute(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), subPartitionValues.get(3), DATA_VERSION,
                            ATTRIBUTE_NAME_1_MIXED_CASE, createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
            }

            // Validate the returned object.
            validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, PARTITION_VALUE, subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_2,
                resultBusinessObjectDataAttribute);
        }
    }

    @Test
    public void testDeleteBusinessObjectDataAttribute()
    {
        // Create and persist a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        // Validate that this business object data attribute exists.
        dmDaoHelper.getBusinessObjectDataAttributeEntity(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));

        // Delete this business object data attribute.
        BusinessObjectDataAttribute deletedBusinessObjectDataAttribute = businessObjectDataAttributeRestController
            .deleteBusinessObjectDataAttribute(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE);

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1,
            deletedBusinessObjectDataAttribute);

        // Ensure that this business object data attribute is no longer there.
        try
        {
            dmDaoHelper.getBusinessObjectDataAttributeEntity(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an ObjectNotFoundException when business object data attribute does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                String.format("Attribute with name \"%s\" does not exist for business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", " +
                    "businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, " +
                    "businessObjectDataPartitionValue: \"%s\", businessObjectDataSubPartitionValues: \"%s,%s,%s,%s\", businessObjectDataVersion: %d}.",
                    ATTRIBUTE_NAME_1_MIXED_CASE, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION),
                e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectDataAttributeLegacy()
    {
        // Create a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create and persist a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        // Validate that this business object data attribute exists.
        dmDaoHelper.getBusinessObjectDataAttributeEntity(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));

        // Delete this business object data attribute.
        BusinessObjectDataAttribute deletedBusinessObjectDataAttribute = businessObjectDataAttributeRestController
            .deleteBusinessObjectDataAttribute(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES.get(0),
                SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1,
            deletedBusinessObjectDataAttribute);

        // Ensure that this business object data attribute is no longer there.
        try
        {
            dmDaoHelper.getBusinessObjectDataAttributeEntity(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an ObjectNotFoundException when business object data attribute does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                String.format("Attribute with name \"%s\" does not exist for business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", " +
                    "businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, " +
                    "businessObjectDataPartitionValue: \"%s\", businessObjectDataSubPartitionValues: \"%s,%s,%s,%s\", businessObjectDataVersion: %d}.",
                    ATTRIBUTE_NAME_1_MIXED_CASE, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3), DATA_VERSION),
                e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectDataAttributeMissingOptionalParameters()
    {
        // Test if we can delete an attribute for the business object data with any allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Create and persist a business object data attribute entity with the relative number of subpartition values.
            BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
                createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

            // Validate that this business object data attribute exists.
            dmDaoHelper.getBusinessObjectDataAttributeEntity(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));

            // Delete this business object data attribute using the relative endpoint.
            BusinessObjectDataAttribute deletedBusinessObjectDataAttribute = null;
            switch (i)
            {
                case 0:
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .deleteBusinessObjectDataAttribute(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
                case 1:
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .deleteBusinessObjectDataAttribute(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
                case 2:
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .deleteBusinessObjectDataAttribute(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
                case 3:
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .deleteBusinessObjectDataAttribute(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
                case 4:
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .deleteBusinessObjectDataAttribute(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), subPartitionValues.get(3), DATA_VERSION,
                            ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
            }

            // Validate the returned object.
            validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, PARTITION_VALUE, subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1,
                deletedBusinessObjectDataAttribute);

            // Ensure that this business object data attribute is no longer there.
            try
            {
                dmDaoHelper.getBusinessObjectDataAttributeEntity(
                    new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
                fail("Should throw an ObjectNotFoundException when business object data attribute does not exist.");
            }
            catch (ObjectNotFoundException e)
            {
                assertEquals(String.format("Attribute with name \"%s\" does not exist for business object data {namespace: \"%s\", " +
                    "businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", " +
                    "businessObjectFormatVersion: %d, businessObjectDataPartitionValue: \"%s\", businessObjectDataSubPartitionValues: \"%s\", " +
                    "businessObjectDataVersion: %d}.", ATTRIBUTE_NAME_1_MIXED_CASE, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                    FORMAT_VERSION, PARTITION_VALUE, StringUtils.join(subPartitionValues, ","), DATA_VERSION), e.getMessage());
            }
        }
    }

    @Test
    public void testDeleteBusinessObjectDataAttributeMissingOptionalParametersLegacy()
    {
        // Create a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Test if we can delete an attribute for the business object data with any allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Create and persist a business object data attribute entity with the relative number of subpartition values.
            BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
                createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

            // Validate that this business object data attribute exists.
            dmDaoHelper.getBusinessObjectDataAttributeEntity(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));

            // Delete this business object data attribute using the relative endpoint.
            BusinessObjectDataAttribute deletedBusinessObjectDataAttribute = null;
            switch (i)
            {
                case 0:
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .deleteBusinessObjectDataAttribute(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, DATA_VERSION,
                            ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
                case 1:
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .deleteBusinessObjectDataAttribute(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
                case 2:
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .deleteBusinessObjectDataAttribute(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
                case 3:
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .deleteBusinessObjectDataAttribute(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
                case 4:
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeRestController
                        .deleteBusinessObjectDataAttribute(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), subPartitionValues.get(3), DATA_VERSION,
                            ATTRIBUTE_NAME_1_MIXED_CASE);
                    break;
            }

            // Validate the returned object.
            validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, PARTITION_VALUE, subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1,
                deletedBusinessObjectDataAttribute);

            // Ensure that this business object data attribute is no longer there.
            try
            {
                dmDaoHelper.getBusinessObjectDataAttributeEntity(
                    new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
                fail("Should throw an ObjectNotFoundException when business object data attribute does not exist.");
            }
            catch (ObjectNotFoundException e)
            {
                assertEquals(String.format("Attribute with name \"%s\" does not exist for business object data {namespace: \"%s\", " +
                    "businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", " +
                    "businessObjectFormatVersion: %d, businessObjectDataPartitionValue: \"%s\", businessObjectDataSubPartitionValues: \"%s\", " +
                    "businessObjectDataVersion: %d}.", ATTRIBUTE_NAME_1_MIXED_CASE, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                    FORMAT_VERSION, PARTITION_VALUE, StringUtils.join(subPartitionValues, ","), DATA_VERSION), e.getMessage());
            }
        }
    }
}
