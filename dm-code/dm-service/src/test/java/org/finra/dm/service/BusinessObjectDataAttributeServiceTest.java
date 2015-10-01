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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.dm.model.AlreadyExistsException;
import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.dm.model.jpa.BusinessObjectDataEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataAttribute;
import org.finra.dm.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.dm.model.api.xml.BusinessObjectDataAttributeKeys;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.service.helper.DmDaoHelper;

/**
 * This class tests various functionality within the business object data attribute REST controller.
 */
public class BusinessObjectDataAttributeServiceTest extends AbstractServiceTest
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
        BusinessObjectDataAttribute resultBusinessObjectDataAttribute = businessObjectDataAttributeService.createBusinessObjectDataAttribute(
            createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1, resultBusinessObjectDataAttribute);
    }

    @Test
    public void testCreateBusinessObjectDataAttributeLegacy()
    {
        // Create a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Create a business object data attribute.
        BusinessObjectDataAttribute resultBusinessObjectDataAttribute = businessObjectDataAttributeService.createBusinessObjectDataAttribute(
            createBusinessObjectDataAttributeCreateRequest(BLANK_TEXT, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1, resultBusinessObjectDataAttribute);
    }

    @Test
    public void testCreateBusinessObjectDataAttributeMissingRequiredParameters()
    {
        // Try to create a business object data attribute instance when business object definition name is not specified.
        try
        {
            businessObjectDataAttributeService.createBusinessObjectDataAttribute(
                createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to create a business object data attribute instance when business object format usage is not specified.
        try
        {
            businessObjectDataAttributeService.createBusinessObjectDataAttribute(
                createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BOD_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to create a business object data attribute instance when business object format file type is not specified.
        try
        {
            businessObjectDataAttributeService.createBusinessObjectDataAttribute(
                createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to create a business object data attribute instance when business object format version is not specified.
        try
        {
            businessObjectDataAttributeService.createBusinessObjectDataAttribute(
                createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to create a business object data attribute instance when partition value is not specified.
        try
        {
            businessObjectDataAttributeService.createBusinessObjectDataAttribute(
                createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }

        // Try to create a business object data attribute instance when subpartition value is not specified.
        try
        {
            businessObjectDataAttributeService.createBusinessObjectDataAttribute(
                createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, Arrays.asList(BLANK_TEXT), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to create a business object data attribute instance when business object data version is not specified.
        try
        {
            businessObjectDataAttributeService.createBusinessObjectDataAttribute(
                createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, null, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when business object data version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data version must be specified.", e.getMessage());
        }

        // Try to create a business object data attribute instance when business object data attribute name is not specified.
        try
        {
            businessObjectDataAttributeService.createBusinessObjectDataAttribute(
                createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, BLANK_TEXT, ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when business object data attribute name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data attribute name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataAttributeMissingOptionalParameters()
    {
        // Create a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create and persist a business object data entity without subpartition values.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            NO_SUBPARTITION_VALUES, DATA_VERSION, true, BDATA_STATUS);

        // Create a business object data attribute.
        BusinessObjectDataAttribute resultBusinessObjectDataAttribute = businessObjectDataAttributeService.createBusinessObjectDataAttribute(
            createBusinessObjectDataAttributeCreateRequest(BLANK_TEXT, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                null, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            NO_SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1, resultBusinessObjectDataAttribute);
    }

    @Test
    public void testCreateBusinessObjectDataAttributeTrimParameters()
    {
        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Create a business object data attribute using input parameters with leading and trailing empty spaces.
        BusinessObjectDataAttribute resultBusinessObjectDataAttribute = businessObjectDataAttributeService.createBusinessObjectDataAttribute(
            createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUBPARTITION_VALUES), DATA_VERSION,
                addWhitespace(ATTRIBUTE_NAME_1_MIXED_CASE), addWhitespace(ATTRIBUTE_VALUE_1)));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, addWhitespace(ATTRIBUTE_VALUE_1), resultBusinessObjectDataAttribute);
    }

    @Test
    public void testCreateBusinessObjectDataAttributeUpperCaseParameters()
    {
        // Create and persist a business object data entity using lower case values.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
            FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION, true, BDATA_STATUS);

        // Create a business object data attribute using upper case input parameters (except for case-sensitive partition values).
        BusinessObjectDataAttribute resultBusinessObjectDataAttribute = businessObjectDataAttributeService.createBusinessObjectDataAttribute(
            createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase()));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(null, NAMESPACE_CD, BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
            FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(),
            ATTRIBUTE_VALUE_1.toUpperCase(), resultBusinessObjectDataAttribute);
    }

    @Test
    public void testCreateBusinessObjectDataAttributeLowerCaseParameters()
    {
        // Create and persist a business object data entity using upper case values.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
            FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION, true, BDATA_STATUS);

        // Create a business object data attribute using lower case input parameters (except for case-sensitive partition values).
        BusinessObjectDataAttribute resultBusinessObjectDataAttribute = businessObjectDataAttributeService.createBusinessObjectDataAttribute(
            createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase()));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(null, NAMESPACE_CD, BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
            FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(),
            ATTRIBUTE_VALUE_1.toLowerCase(), resultBusinessObjectDataAttribute);
    }

    @Test
    public void testCreateBusinessObjectDataAttributeBusinessObjectFormatNoExists()
    {
        // Try to create a business object data attribute instance using non-existing business object format.
        try
        {
            businessObjectDataAttributeService.createBusinessObjectDataAttribute(
                createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));
            fail("Should throw an ObjectNotFoundException when business object format does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object format with namespace \"%s\", business object definition name \"%s\", format usage \"%s\", " +
                "format file type \"%s\", and format version \"%d\" doesn't exist.", NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataAttributeBusinessObjectDataNoExists()
    {
        // Create and persist a business object format entity.
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION, true,
            PARTITION_KEY);

        // Try to create a business object data attribute instance using non-existing business object data.
        try
        {
            businessObjectDataAttributeService.createBusinessObjectDataAttribute(
                createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));
            fail("Should throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataAttributeBusinessObjectDataAttributeAlreadyExists()
    {
        // Create and persist a business object data attribute entity.
        createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1);

        // Try to add a duplicate business object data attribute.
        try
        {
            businessObjectDataAttributeService.createBusinessObjectDataAttribute(
                createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_2));
            fail("Should throw an AlreadyExistsException when business object data attribute already exists.");
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(
                String.format("Unable to create business object data attribute with name \"%s\" because it already exists for the the business object data {" +
                    "namespace: \"%s\", " +
                    "businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", " +
                    "businessObjectFormatVersion: %d, businessObjectDataPartitionValue: \"%s\", businessObjectDataSubPartitionValues: \"%s,%s,%s,%s\", " +
                    "businessObjectDataVersion: %d}.", ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE,
                    FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2),
                    SUBPARTITION_VALUES.get(3), DATA_VERSION), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDataAttributeBlankValue()
    {
        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Create a business object data attribute with a null value.
        BusinessObjectDataAttribute resultBusinessObjectDataAttribute = businessObjectDataAttributeService.createBusinessObjectDataAttribute(
            createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, BLANK_TEXT));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, BLANK_TEXT, resultBusinessObjectDataAttribute);
    }

    @Test
    public void testCreateBusinessObjectDataAttributeNullValue()
    {
        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Create a business object data attribute with a null value.
        BusinessObjectDataAttribute resultBusinessObjectDataAttribute = businessObjectDataAttributeService.createBusinessObjectDataAttribute(
            createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, null));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, null, resultBusinessObjectDataAttribute);
    }

    @Test
    public void testCreateBusinessObjectDataAttributeRequiredAttribute()
    {
        // Create and persist a business object data attribute definition entity.
        createBusinessObjectDataAttributeDefinitionEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Create a required business object data attribute.
        BusinessObjectDataAttribute resultBusinessObjectDataAttribute = businessObjectDataAttributeService.createBusinessObjectDataAttribute(
            createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1, resultBusinessObjectDataAttribute);
    }

    @Test
    public void testCreateBusinessObjectDataAttributeRequiredAttributeMissingValue()
    {
        // Create and persist a business object data attribute definition entity.
        createBusinessObjectDataAttributeDefinitionEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Try to create a required business object data attribute instance when business object data attribute value is not specified.
        try
        {
            businessObjectDataAttributeService.createBusinessObjectDataAttribute(
                createBusinessObjectDataAttributeCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                    PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when business object data attribute value is not specified for a required attribute.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("A business object data attribute value must be specified since \"%s\" is a required attribute for business object " +
                "format {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", " +
                "businessObjectFormatVersion: %d}.", ATTRIBUTE_NAME_1_MIXED_CASE, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataAttribute()
    {
        // Create and persist a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        // Retrieve the business object data attribute.
        BusinessObjectDataAttribute resultBusinessObjectDataAttribute = businessObjectDataAttributeService.getBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));

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
        BusinessObjectDataAttribute resultBusinessObjectDataAttribute = businessObjectDataAttributeService.getBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1,
            resultBusinessObjectDataAttribute);
    }

    @Test
    public void testGetBusinessObjectDataAttributeMissingRequiredParameters()
    {
        // Try to delete a business object data attribute instance when business object definition name is not specified.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to get a business object data attribute instance when business object format usage is not specified.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to get a business object data attribute instance when business object format file type is not specified.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to get a business object data attribute instance when business object format version is not specified.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to get a business object data attribute instance when partition value is not specified.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }

        // Try to get a business object data attribute instance without specifying 1st subpartition value.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(BLANK_TEXT, SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION,
                    ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when 1st subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to get a business object data attribute instance without specifying 2nd subpartition value.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), BLANK_TEXT, SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION,
                    ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when 2nd subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to get a business object data attribute instance without specifying 3rd subpartition value.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), BLANK_TEXT, SUBPARTITION_VALUES.get(3)), DATA_VERSION,
                    ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when 3rd subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to get a business object data attribute instance without specifying 4th subpartition value.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), BLANK_TEXT), DATA_VERSION,
                    ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when 4th subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to get a business object data attribute instance when business object data version is not specified.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, null, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when business object data version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data version must be specified.", e.getMessage());
        }

        // Try to get a business object data attribute instance when business object data attribute name is not specified.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when business object data attribute name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data attribute name must be specified.", e.getMessage());
        }
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
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            NO_SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
                    break;
                case 1:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
                    break;
                case 2:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
                    break;
                case 3:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2)), DATA_VERSION,
                            ATTRIBUTE_NAME_1_MIXED_CASE));
                    break;
                case 4:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), subPartitionValues.get(3)),
                            DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
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
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            NO_SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
                    break;
                case 1:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
                    break;
                case 2:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
                    break;
                case 3:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2)), DATA_VERSION,
                            ATTRIBUTE_NAME_1_MIXED_CASE));
                    break;
                case 4:
                    resultBusinessObjectDataAttribute = businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2), subPartitionValues.get(3)),
                            DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
                    break;
            }

            // Validate the returned object.
            validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, PARTITION_VALUE, subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1,
                resultBusinessObjectDataAttribute);
        }
    }

    @Test
    public void testGetBusinessObjectDataAttributeTrimParameters()
    {
        // Create and persist a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        // Retrieve the business object data attribute using input parameters with leading and trailing empty spaces.
        BusinessObjectDataAttribute resultBusinessObjectDataAttribute = businessObjectDataAttributeService.getBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD, addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE), addWhitespace(FORMAT_FILE_TYPE_CODE),
                FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUBPARTITION_VALUES), DATA_VERSION, addWhitespace(ATTRIBUTE_NAME_1_MIXED_CASE)));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1,
            resultBusinessObjectDataAttribute);
    }

    @Test
    public void testGetBusinessObjectDataAttributeUpperCaseParameters()
    {
        // Create and persist a business object data attribute entity using lower case values.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase());

        // Get the business object data attribute using upper case input parameters (except for case-sensitive partition values).
        BusinessObjectDataAttribute resultBusinessObjectDataAttribute = businessObjectDataAttributeService.getBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase()));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
            FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION,
            ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase(), resultBusinessObjectDataAttribute);
    }

    @Test
    public void testGetBusinessObjectDataAttributeLowerCaseParameters()
    {
        // Create and persist a business object data attribute entity using upper case values.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase());

        // Get the business object data attribute using lower case input parameters (except for case-sensitive partition values).
        BusinessObjectDataAttribute resultBusinessObjectDataAttribute = businessObjectDataAttributeService.getBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase()));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
            FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION,
            ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase(), resultBusinessObjectDataAttribute);
    }

    @Test
    public void testGetBusinessObjectDataAttributeBusinessObjectDataNoExists()
    {
        // Try to get a business object data attribute instance using non-existing business object data.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataAttributeBusinessObjectDataAttributeNoExists()
    {
        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Try to get a non-existing business object data attribute.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttribute(
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
        BusinessObjectDataAttributeKeys resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeService.getBusinessObjectDataAttributes(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION));

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
        BusinessObjectDataAttributeKeys resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeService.getBusinessObjectDataAttributes(
            new BusinessObjectDataKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION));

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
    public void testGetBusinessObjectDataAttributesMissingRequiredParameters()
    {
        // Try to get business object data attributes when business object definition name is not specified.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                new BusinessObjectDataKey(NAMESPACE_CD, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to get business object data attributes when business object format usage is not specified.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION));
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to get business object data attributes when business object format file type is not specified.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION));
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to get business object data attributes when business object format version is not specified.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION));
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to get business object data attributes when partition value is not specified.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT, SUBPARTITION_VALUES,
                    DATA_VERSION));
            fail("Should throw an IllegalArgumentException when partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }

        // Try to get business object data attributes instance without specifying 1st subpartition value.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(BLANK_TEXT, SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION));
            fail("Should throw an IllegalArgumentException when 1st subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to get business object data attributes instance without specifying 2nd subpartition value.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), BLANK_TEXT, SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION));
            fail("Should throw an IllegalArgumentException when 2nd subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to get business object data attributes instance without specifying 3rd subpartition value.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), BLANK_TEXT, SUBPARTITION_VALUES.get(3)), DATA_VERSION));
            fail("Should throw an IllegalArgumentException when 3rd subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to get business object data attributes instance without specifying 4th subpartition value.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), BLANK_TEXT), DATA_VERSION));
            fail("Should throw an IllegalArgumentException when 4th subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to get business object data attributes when business object data version is not specified.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    NO_SUBPARTITION_VALUES, null));
            fail("Should throw an IllegalArgumentException when business object data version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data version must be specified.", e.getMessage());
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
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                        new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            NO_SUBPARTITION_VALUES, DATA_VERSION));
                    break;
                case 1:
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                        new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(SUBPARTITION_VALUES.get(0)), DATA_VERSION));
                    break;
                case 2:
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                        new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1)), DATA_VERSION));
                    break;
                case 3:
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                        new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2)), DATA_VERSION));
                    break;
                case 4:
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                        new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            SUBPARTITION_VALUES, DATA_VERSION));
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
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                        new BusinessObjectDataKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            NO_SUBPARTITION_VALUES, DATA_VERSION));
                    break;
                case 1:
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                        new BusinessObjectDataKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(SUBPARTITION_VALUES.get(0)), DATA_VERSION));
                    break;
                case 2:
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                        new BusinessObjectDataKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1)), DATA_VERSION));
                    break;
                case 3:
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                        new BusinessObjectDataKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2)), DATA_VERSION));
                    break;
                case 4:
                    resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                        new BusinessObjectDataKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            SUBPARTITION_VALUES, DATA_VERSION));
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
    public void testGetBusinessObjectDataAttributesTrimParameters()
    {
        // List of test business object data attribute names.
        List<String> testBusinessObjectDataAttributeNames = Arrays.asList(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_NAME_2_MIXED_CASE);

        // Create and persist a business object data attribute entities.
        for (String businessObjectDataAttributeName : testBusinessObjectDataAttributeNames)
        {
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, businessObjectDataAttributeName, ATTRIBUTE_VALUE_1);
        }

        // Retrieve a list of business object data attribute keys using input parameters with leading and trailing empty spaces.
        BusinessObjectDataAttributeKeys resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeService.getBusinessObjectDataAttributes(
            new BusinessObjectDataKey(NAMESPACE_CD, addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE), addWhitespace(FORMAT_FILE_TYPE_CODE),
                FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUBPARTITION_VALUES), DATA_VERSION));

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
    public void testGetBusinessObjectDataAttributesUpperCaseParameters()
    {
        // List of test business object data attribute names.
        List<String> testBusinessObjectDataAttributeNames = Arrays.asList(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_NAME_2_MIXED_CASE);

        // Create and persist a business object data attribute entities using lower case values.
        for (String businessObjectDataAttributeName : testBusinessObjectDataAttributeNames)
        {
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION,
                businessObjectDataAttributeName.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase());
        }

        // Retrieve a list of business object data attribute keys using upper case input parameters (except for case-sensitive partition values).
        BusinessObjectDataAttributeKeys resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeService.getBusinessObjectDataAttributes(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION));

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataAttributeKeys);
        assertEquals(testBusinessObjectDataAttributeNames.size(), resultBusinessObjectDataAttributeKeys.getBusinessObjectDataAttributeKeys().size());
        for (int i = 0; i < testBusinessObjectDataAttributeNames.size(); i++)
        {
            validateBusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION,
                testBusinessObjectDataAttributeNames.get(i).toLowerCase(), resultBusinessObjectDataAttributeKeys.getBusinessObjectDataAttributeKeys().get(i));
        }
    }

    @Test
    public void testGetBusinessObjectDataAttributesLowerCaseParameters()
    {
        // List of test business object data attribute names.
        List<String> testBusinessObjectDataAttributeNames = Arrays.asList(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_NAME_2_MIXED_CASE);

        // Create and persist a business object data attribute entities using upper case values.
        for (String businessObjectDataAttributeName : testBusinessObjectDataAttributeNames)
        {
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION,
                businessObjectDataAttributeName.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase());
        }

        // Retrieve a list of business object data attribute keys using lower case input parameters (except for case-sensitive partition values).
        BusinessObjectDataAttributeKeys resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeService.getBusinessObjectDataAttributes(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION));

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataAttributeKeys);
        assertEquals(testBusinessObjectDataAttributeNames.size(), resultBusinessObjectDataAttributeKeys.getBusinessObjectDataAttributeKeys().size());
        for (int i = 0; i < testBusinessObjectDataAttributeNames.size(); i++)
        {
            validateBusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION,
                testBusinessObjectDataAttributeNames.get(i).toUpperCase(), resultBusinessObjectDataAttributeKeys.getBusinessObjectDataAttributeKeys().get(i));
        }
    }

    @Test
    public void testGetBusinessObjectDataAttributesBusinessObjectDataNoExists()
    {
        // Try to retrieve a list of business object data attribute keys for a non-existing business object data.
        try
        {
            businessObjectDataAttributeService.getBusinessObjectDataAttributes(
                new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION));
            fail("Should throw an ObjectNotFoundException when business object format does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDataAttributesBusinessObjectDataAttributesNoExist()
    {
        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Retrieve a list of business object data attribute keys, when none of the business object data attributes exist.
        BusinessObjectDataAttributeKeys resultBusinessObjectDataAttributeKeys = businessObjectDataAttributeService.getBusinessObjectDataAttributes(
            new BusinessObjectDataKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION));

        // Validate the returned object.
        assertNotNull(resultBusinessObjectDataAttributeKeys);
        assertEquals(0, resultBusinessObjectDataAttributeKeys.getBusinessObjectDataAttributeKeys().size());
    }

    @Test
    public void testUpdateBusinessObjectDataAttribute()
    {
        // Create and persist a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        // Update the business object data attribute.
        BusinessObjectDataAttribute updatedBusinessObjectDataAttribute = businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));

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
        BusinessObjectDataAttribute updatedBusinessObjectDataAttribute = businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_2,
            updatedBusinessObjectDataAttribute);
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeMissingRequiredParameters()
    {
        // Try to update a business object data attribute instance when business object definition name is not specified.
        try
        {
            businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to update a business object data attribute instance when business object format usage is not specified.
        try
        {
            businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to update a business object data attribute instance when business object format file type is not specified.
        try
        {
            businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to update a business object data attribute instance when business object format version is not specified.
        try
        {
            businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to update a business object data attribute instance when partition value is not specified.
        try
        {
            businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT,
                    NO_SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }

        // Try to update a business object data attribute instance without specifying 1st subpartition value.
        try
        {
            businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(BLANK_TEXT, SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION,
                    ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when 1st subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to update a business object data attribute instance without specifying 2nd subpartition value.
        try
        {
            businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), BLANK_TEXT, SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION,
                    ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when 2nd subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to update a business object data attribute instance without specifying 3rd subpartition value.
        try
        {
            businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), BLANK_TEXT, SUBPARTITION_VALUES.get(3)), DATA_VERSION,
                    ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when 3rd subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to update a business object data attribute instance without specifying 4th subpartition value.
        try
        {
            businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), BLANK_TEXT), DATA_VERSION,
                    ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when 4th subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to update a business object data attribute instance when business object data version is not specified.
        try
        {
            businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    NO_SUBPARTITION_VALUES, null, ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when business object data version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data version must be specified.", e.getMessage());
        }

        // Try to update a business object data attribute instance when business object data attribute name is not specified.
        try
        {
            businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, BLANK_TEXT), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1));
            fail("Should throw an IllegalArgumentException when business object data attribute name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data attribute name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeMissingOptionalParameters()
    {
        // Test if we can retrieve an attribute for the business object data with any allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Create and persist a business object data attribute entity without subpartition values.
            BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
                createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

            //  Update the business object data attribute using the relative endpoint.
            BusinessObjectDataAttribute updatedBusinessObjectDataAttribute = null;
            switch (i)
            {
                case 0:
                    updatedBusinessObjectDataAttribute = businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            NO_SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE),
                        createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
                case 1:
                    updatedBusinessObjectDataAttribute = businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE),
                        createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
                case 2:
                    updatedBusinessObjectDataAttribute = businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE),
                        createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
                case 3:
                    updatedBusinessObjectDataAttribute = businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2)), DATA_VERSION,
                            ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
                case 4:
                    updatedBusinessObjectDataAttribute = businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
            }

            // Validate the returned object.
            validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, PARTITION_VALUE, subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_2,
                updatedBusinessObjectDataAttribute);
        }
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeMissingOptionalParametersLegacy()
    {
        // Create a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Test if we can retrieve an attribute for the business object data with any allowed number of subpartition values (from 0 to MAX_SUBPARTITIONS).
        for (int i = 0; i <= BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            // Build a list of subpartition values.
            List<String> subPartitionValues = SUBPARTITION_VALUES.subList(0, i);

            // Create and persist a business object data attribute entity without subpartition values.
            BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
                createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

            //  Update the business object data attribute using the relative endpoint.
            BusinessObjectDataAttribute updatedBusinessObjectDataAttribute = null;
            switch (i)
            {
                case 0:
                    updatedBusinessObjectDataAttribute = businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            NO_SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE),
                        createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
                case 1:
                    updatedBusinessObjectDataAttribute = businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE),
                        createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
                case 2:
                    updatedBusinessObjectDataAttribute = businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE),
                        createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
                case 3:
                    updatedBusinessObjectDataAttribute = businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2)), DATA_VERSION,
                            ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
                case 4:
                    updatedBusinessObjectDataAttribute = businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));
                    break;
            }

            // Validate the returned object.
            validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, PARTITION_VALUE, subPartitionValues, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_2,
                updatedBusinessObjectDataAttribute);
        }
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeTrimParameters()
    {
        // Create and persist a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        // Update the business object data attribute using input parameters with leading and trailing empty spaces.
        BusinessObjectDataAttribute updatedBusinessObjectDataAttribute = businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(addWhitespace(NAMESPACE_CD), addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE),
                addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUBPARTITION_VALUES), DATA_VERSION,
                addWhitespace(ATTRIBUTE_NAME_1_MIXED_CASE)), createBusinessObjectDataAttributeUpdateRequest(addWhitespace(ATTRIBUTE_VALUE_2)));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, addWhitespace(ATTRIBUTE_VALUE_2),
            updatedBusinessObjectDataAttribute);
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeUpperCaseParameters()
    {
        // Create and persist a business object data attribute entity using lower case values.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase());

        // Update the business object data attribute using upper case input parameters (except for case-sensitive partition values).
        BusinessObjectDataAttribute updatedBusinessObjectDataAttribute = businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase()), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2.toUpperCase()));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(),
            FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE.toLowerCase(),
            convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_2.toUpperCase(),
            updatedBusinessObjectDataAttribute);
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeLowerCaseParameters()
    {
        // Create and persist a business object data attribute entity using upper case values.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase());

        // Update the business object data attribute using lower case input parameters (except for case-sensitive partition values).
        BusinessObjectDataAttribute updatedBusinessObjectDataAttribute = businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase()), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2.toLowerCase()));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(),
            FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE.toUpperCase(),
            convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_2.toLowerCase(),
            updatedBusinessObjectDataAttribute);
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeBusinessObjectFormatNoExists()
    {
        // Try to update a business object data attribute instance using non-existing business object format.
        try
        {
            businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1));
            fail("Should throw an ObjectNotFoundException when business object format does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object format with namespace \"%s\", business object definition name \"%s\", format usage \"%s\", " +
                "format file type \"%s\", and format version \"%d\" doesn't exist.", NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION), e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeBusinessObjectDataNoExists()
    {
        // Create and persist a business object format entity.
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION, true,
            PARTITION_KEY);

        // Try to update a business object data attribute instance using non-existing business object data.
        try
        {
            businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1));
            fail("Should throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeBusinessObjectDataAttributeNoExists()
    {
        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Try to update a non-existing business object data attribute.
        try
        {
            businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_1));
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
    public void testUpdateBusinessObjectDataAttributeBlankValue()
    {
        // Create and persist a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        // Update the business object data attribute with a null value.
        BusinessObjectDataAttribute updatedBusinessObjectDataAttribute = businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(BLANK_TEXT));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, BLANK_TEXT, updatedBusinessObjectDataAttribute);
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeNullValue()
    {
        // Create and persist a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        // Update the business object data attribute with a null value.
        BusinessObjectDataAttribute updatedBusinessObjectDataAttribute = businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(null));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, null, updatedBusinessObjectDataAttribute);
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeRequiredAttribute()
    {
        // Create and persist a business object data attribute definition entity.
        createBusinessObjectDataAttributeDefinitionEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create and persist a required business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        // Update the required business object data attribute.
        BusinessObjectDataAttribute updatedBusinessObjectDataAttribute = businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(ATTRIBUTE_VALUE_2));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_2,
            updatedBusinessObjectDataAttribute);
    }

    @Test
    public void testUpdateBusinessObjectDataAttributeRequiredAttributeMissingValue()
    {
        // Create and persist a business object data attribute definition entity.
        createBusinessObjectDataAttributeDefinitionEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create and persist a required business object data attribute entity.
        createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        // Try to update the required business object data attribute instance when business object data attribute value is not specified.
        try
        {
            businessObjectDataAttributeService.updateBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE), createBusinessObjectDataAttributeUpdateRequest(BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when business object data attribute value is not specified for a required attribute.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("A business object data attribute value must be specified since \"%s\" is a required attribute for business object " +
                "format {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", " +
                "businessObjectFormatVersion: %d}.", ATTRIBUTE_NAME_1_MIXED_CASE, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION), e.getMessage());
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
        BusinessObjectDataAttribute deletedBusinessObjectDataAttribute = businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));

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
        BusinessObjectDataAttribute deletedBusinessObjectDataAttribute = businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));

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
    public void testDeleteBusinessObjectDataAttributeMissingRequiredParameters()
    {
        // Try to delete a business object data attribute instance when business object definition name is not specified.
        try
        {
            businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to delete a business object data attribute instance when business object format usage is not specified.
        try
        {
            businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, BLANK_TEXT, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when business object format usage is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to delete a business object data attribute instance when business object format file type is not specified.
        try
        {
            businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, BLANK_TEXT, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when business object format file type is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format file type must be specified.", e.getMessage());
        }

        // Try to delete a business object data attribute instance when business object format version is not specified.
        try
        {
            businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null, PARTITION_VALUE, SUBPARTITION_VALUES,
                    DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when business object format version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format version must be specified.", e.getMessage());
        }

        // Try to delete a business object data attribute instance when partition value is not specified.
        try
        {
            businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, BLANK_TEXT,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when partition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition value must be specified.", e.getMessage());
        }

        // Try to delete a business object data attribute instance without specifying 1st subpartition value.
        try
        {
            businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(BLANK_TEXT, SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION,
                    ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when 1st subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to delete a business object data attribute instance without specifying 2nd subpartition value.
        try
        {
            businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), BLANK_TEXT, SUBPARTITION_VALUES.get(2), SUBPARTITION_VALUES.get(3)), DATA_VERSION,
                    ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when 2nd subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to delete a business object data attribute instance without specifying 3rd subpartition value.
        try
        {
            businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), BLANK_TEXT, SUBPARTITION_VALUES.get(3)), DATA_VERSION,
                    ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when 3rd subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to delete a business object data attribute instance without specifying 4th subpartition value.
        try
        {
            businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    Arrays.asList(SUBPARTITION_VALUES.get(0), SUBPARTITION_VALUES.get(1), SUBPARTITION_VALUES.get(2), BLANK_TEXT), DATA_VERSION,
                    ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when 4th subpartition value is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A subpartition value must be specified.", e.getMessage());
        }

        // Try to delete a business object data attribute instance when business object data version is not specified.
        try
        {
            businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, null, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when business object data version is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data version must be specified.", e.getMessage());
        }

        // Try to delete a business object data attribute instance when business object data attribute name is not specified.
        try
        {
            businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when business object data attribute name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object data attribute name must be specified.", e.getMessage());
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
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            NO_SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
                    break;
                case 1:
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
                    break;
                case 2:
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
                    break;
                case 3:
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2)), DATA_VERSION,
                            ATTRIBUTE_NAME_1_MIXED_CASE));
                    break;
                case 4:
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
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
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            NO_SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
                    break;
                case 1:
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
                    break;
                case 2:
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1)), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
                    break;
                case 3:
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            Arrays.asList(subPartitionValues.get(0), subPartitionValues.get(1), subPartitionValues.get(2)), DATA_VERSION,
                            ATTRIBUTE_NAME_1_MIXED_CASE));
                    break;
                case 4:
                    deletedBusinessObjectDataAttribute = businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                        new BusinessObjectDataAttributeKey(null, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                            SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
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
    public void testDeleteBusinessObjectDataAttributeTrimParameters()
    {
        // Create and persist a business object data attribute entity.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        // Validate that this business object data attribute exists.
        dmDaoHelper.getBusinessObjectDataAttributeEntity(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));

        // Delete the business object data attribute using input parameters with leading and trailing empty spaces.
        BusinessObjectDataAttribute deletedBusinessObjectDataAttribute = businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD, addWhitespace(BOD_NAME), addWhitespace(FORMAT_USAGE_CODE), addWhitespace(FORMAT_FILE_TYPE_CODE),
                FORMAT_VERSION, addWhitespace(PARTITION_VALUE), addWhitespace(SUBPARTITION_VALUES), DATA_VERSION, addWhitespace(ATTRIBUTE_NAME_1_MIXED_CASE)));

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
    public void testDeleteBusinessObjectDataAttributeUpperCaseParameters()
    {
        // Create and persist a business object data attribute entity using lower case values.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase());

        // Validate that this business object data attribute exists.
        dmDaoHelper.getBusinessObjectDataAttributeEntity(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase()));

        // Delete the business object data attribute using upper case input parameters (except for case-sensitive partition values).
        BusinessObjectDataAttribute deletedBusinessObjectDataAttribute = businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase()));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(),
            FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE.toLowerCase(),
            convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), ATTRIBUTE_VALUE_1.toLowerCase(),
            deletedBusinessObjectDataAttribute);

        // Ensure that this business object data attribute is no longer there.
        try
        {
            dmDaoHelper.getBusinessObjectDataAttributeEntity(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(), FORMAT_FILE_TYPE_CODE.toLowerCase(),
                    FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), convertListToLowerCase(SUBPARTITION_VALUES), DATA_VERSION,
                    ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase()));
            fail("Should throw an ObjectNotFoundException when business object data attribute does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                String.format("Attribute with name \"%s\" does not exist for business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", " +
                    "businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, " +
                    "businessObjectDataPartitionValue: \"%s\", businessObjectDataSubPartitionValues: \"%s,%s,%s,%s\", businessObjectDataVersion: %d}.",
                    ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase(), NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                    FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE.toLowerCase(), SUBPARTITION_VALUES.get(0).toLowerCase(),
                    SUBPARTITION_VALUES.get(1).toLowerCase(), SUBPARTITION_VALUES.get(2).toLowerCase(), SUBPARTITION_VALUES.get(3).toLowerCase(), DATA_VERSION),
                e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectDataAttributeLowerCaseParameters()
    {
        // Create and persist a business object data attribute entity using upper case values.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            createBusinessObjectDataAttributeEntity(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase());

        // Validate that this business object data attribute exists.
        dmDaoHelper.getBusinessObjectDataAttributeEntity(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase()));

        // Delete the business object data attribute using lower case input parameters (except for case-sensitive partition values).
        BusinessObjectDataAttribute deletedBusinessObjectDataAttribute = businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
            new BusinessObjectDataAttributeKey(NAMESPACE_CD.toLowerCase(), BOD_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION,
                ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase()));

        // Validate the returned object.
        validateBusinessObjectDataAttribute(businessObjectDataAttributeEntity.getId(), NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(),
            FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE.toUpperCase(),
            convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), ATTRIBUTE_VALUE_1.toUpperCase(),
            deletedBusinessObjectDataAttribute);

        // Ensure that this business object data attribute is no longer there.
        try
        {
            dmDaoHelper.getBusinessObjectDataAttributeEntity(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(), FORMAT_FILE_TYPE_CODE.toUpperCase(),
                    FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), convertListToUpperCase(SUBPARTITION_VALUES), DATA_VERSION,
                    ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase()));
            fail("Should throw an ObjectNotFoundException when business object data attribute does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(
                String.format("Attribute with name \"%s\" does not exist for business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", " +
                    "businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, " +
                    "businessObjectDataPartitionValue: \"%s\", businessObjectDataSubPartitionValues: \"%s,%s,%s,%s\", businessObjectDataVersion: %d}.",
                    ATTRIBUTE_NAME_1_MIXED_CASE.toUpperCase(), NAMESPACE_CD.toUpperCase(), BOD_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                    FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, PARTITION_VALUE.toUpperCase(), SUBPARTITION_VALUES.get(0).toUpperCase(),
                    SUBPARTITION_VALUES.get(1).toUpperCase(), SUBPARTITION_VALUES.get(2).toUpperCase(), SUBPARTITION_VALUES.get(3).toUpperCase(), DATA_VERSION),
                e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectDataAttributeBusinessObjectFormatNoExists()
    {
        // Try to delete a business object data attribute instance using non-existing business object format.
        try
        {
            businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an ObjectNotFoundException when business object format does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object format with namespace \"%s\", business object definition name \"%s\", format usage \"%s\", " +
                "format file type \"%s\", and format version \"%d\" doesn't exist.", NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION), e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectDataAttributeBusinessObjectDataNoExists()
    {
        // Create and persist a business object format entity.
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION, true,
            PARTITION_KEY);

        // Try to delete a business object data attribute instance using non-existing business object data.
        try
        {
            businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an ObjectNotFoundException when business object data does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(getExpectedBusinessObjectDataNotFoundErrorMessage(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_VALUE, SUBPARTITION_VALUES, DATA_VERSION, null), e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectDataAttributeBusinessObjectDataAttributeNoExists()
    {
        // Create and persist a business object data entity.
        createBusinessObjectDataEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, SUBPARTITION_VALUES,
            DATA_VERSION, true, BDATA_STATUS);

        // Try to delete a non-existing business object data attribute.
        try
        {
            businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
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
    public void testDeleteBusinessObjectDataAttributeRequiredAttribute()
    {
        // Create and persist a business object data attribute definition entity.
        createBusinessObjectDataAttributeDefinitionEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
            ATTRIBUTE_NAME_1_MIXED_CASE);

        // Create and persist a required business object data attribute entity.
        createBusinessObjectDataAttributeEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        // Try to delete the required business object data attribute.
        try
        {
            businessObjectDataAttributeService.deleteBusinessObjectDataAttribute(
                new BusinessObjectDataAttributeKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE));
            fail("Should throw an IllegalArgumentException when deleting a required business object data attribute.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Cannot delete \"%s\" attribute since it is a required attribute for business object format " +
                "{namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", businessObjectFormatFileType: \"%s\", " +
                "businessObjectFormatVersion: %d}.", ATTRIBUTE_NAME_1_MIXED_CASE, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION), e.getMessage());
        }
    }
}
