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

import static org.finra.herd.service.impl.BusinessObjectDefinitionColumnServiceImpl.DESCRIPTION_FIELD;
import static org.finra.herd.service.impl.BusinessObjectDefinitionColumnServiceImpl.SCHEMA_COLUMN_NAME_FIELD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Arrays;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumn;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnSearchResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionColumnEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.SchemaColumnEntity;

/**
 * This class tests functionality within the business object definition column service.
 */
public class BusinessObjectDefinitionColumnServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreateBusinessObjectDefinitionColumn()
    {
        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format.
        SchemaColumnEntity schemaColumnEntity = schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME);

        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create a business object definition column.
        BusinessObjectDefinitionColumn resultBusinessObjectDefinitionColumn = businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(
            new BusinessObjectDefinitionColumnCreateRequest(businessObjectDefinitionColumnKey, COLUMN_NAME, BDEF_COLUMN_DESCRIPTION));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(resultBusinessObjectDefinitionColumn.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION), resultBusinessObjectDefinitionColumn);

        // Validate that the schema column is now linked with the business object definition column.
        assertEquals(Long.valueOf(resultBusinessObjectDefinitionColumn.getId()), Long.valueOf(schemaColumnEntity.getBusinessObjectDefinitionColumn().getId()));
    }

    @Test
    public void testCreateBusinessObjectDefinitionColumnBusinessObjectDefinitionColumnAlreadyExists()
    {
        // Create and persist a business object definition column entity.
        businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME),
                BDEF_COLUMN_DESCRIPTION);

        // Try to add a duplicate business object definition column.
        for (String businessObjectDefinitionColumnName : Arrays.asList(BDEF_COLUMN_NAME, BDEF_COLUMN_NAME.toUpperCase(), BDEF_COLUMN_NAME.toLowerCase()))
        {
            try
            {
                businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(new BusinessObjectDefinitionColumnCreateRequest(
                    new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, businessObjectDefinitionColumnName), COLUMN_NAME,
                    BDEF_COLUMN_DESCRIPTION_2));
                fail("Should throw an AlreadyExistsException when business object definition column already exists.");
            }
            catch (AlreadyExistsException e)
            {
                assertEquals(String.format(
                    "Unable to create business object definition column with name \"%s\" because it already exists for the business object definition {%s}.",
                    businessObjectDefinitionColumnName,
                    businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionKeyAsString(BDEF_NAMESPACE, BDEF_NAME)), e.getMessage());
            }
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionColumnBusinessObjectDefinitionNoExists()
    {
        // Try to create a business object definition column for a non-existing business object definition.
        try
        {
            businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(
                new BusinessObjectDefinitionColumnCreateRequest(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME), COLUMN_NAME,
                    BDEF_COLUMN_DESCRIPTION));
            fail("Should throw an ObjectNotFoundException when business object definition does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME),
                e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionColumnInvalidParameters()
    {
        // Try to create a business object definition column when business object definition namespace contains a forward slash character.
        try
        {
            businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(
                new BusinessObjectDefinitionColumnCreateRequest(new BusinessObjectDefinitionColumnKey(addSlash(BDEF_NAMESPACE), BDEF_NAME, BDEF_COLUMN_NAME),
                    COLUMN_NAME, BDEF_COLUMN_DESCRIPTION));
            fail("Should throw an IllegalArgumentException when business object definition namespace contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a business object definition column when business object definition name contains a forward slash character.
        try
        {
            businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(
                new BusinessObjectDefinitionColumnCreateRequest(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, addSlash(BDEF_NAME), BDEF_COLUMN_NAME),
                    COLUMN_NAME, BDEF_COLUMN_DESCRIPTION));
            fail("Should throw an IllegalArgumentException when business object definition name contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object definition name can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a business object definition column when business object definition column name contains a forward slash character.
        try
        {
            businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(
                new BusinessObjectDefinitionColumnCreateRequest(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, addSlash(BDEF_COLUMN_NAME)),
                    COLUMN_NAME, BDEF_COLUMN_DESCRIPTION));
            fail("Should throw an IllegalArgumentException when business object definition column name contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object definition column name can not contain a forward slash character.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionColumnLowerCaseParameters()
    {
        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME);

        // Create a business object definition column using lower case parameter values.
        BusinessObjectDefinitionColumn resultBusinessObjectDefinitionColumn = businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(
            new BusinessObjectDefinitionColumnCreateRequest(
                new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), BDEF_COLUMN_NAME.toLowerCase()),
                COLUMN_NAME.toLowerCase(), BDEF_COLUMN_DESCRIPTION.toLowerCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(resultBusinessObjectDefinitionColumn.getId(),
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME.toLowerCase()), COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION.toLowerCase()), resultBusinessObjectDefinitionColumn);
    }

    @Test
    public void testCreateBusinessObjectDefinitionColumnMissingOptionalParameters()
    {
        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME);

        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create a business object definition column without passing optional parameters.
        BusinessObjectDefinitionColumn resultBusinessObjectDefinitionColumn = businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(
            new BusinessObjectDefinitionColumnCreateRequest(businessObjectDefinitionColumnKey, COLUMN_NAME, NO_BDEF_COLUMN_DESCRIPTION));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(resultBusinessObjectDefinitionColumn.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            NO_BDEF_COLUMN_DESCRIPTION), resultBusinessObjectDefinitionColumn);
    }

    @Test
    public void testCreateBusinessObjectDefinitionColumnMissingRequiredParameters()
    {
        // Try to create a business object definition column when business object definition namespace is not specified.
        try
        {
            businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(
                new BusinessObjectDefinitionColumnCreateRequest(new BusinessObjectDefinitionColumnKey(BLANK_TEXT, BDEF_NAME, BDEF_COLUMN_NAME), COLUMN_NAME,
                    BDEF_COLUMN_DESCRIPTION));
            fail("Should throw an IllegalArgumentException when business object definition namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to create a business object definition column when business object definition name is not specified.
        try
        {
            businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(
                new BusinessObjectDefinitionColumnCreateRequest(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BLANK_TEXT, BDEF_COLUMN_NAME),
                    COLUMN_NAME, BDEF_COLUMN_DESCRIPTION));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to create a business object definition column when business object definition column name is not specified.
        try
        {
            businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(
                new BusinessObjectDefinitionColumnCreateRequest(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BLANK_TEXT), COLUMN_NAME,
                    BDEF_COLUMN_DESCRIPTION));
            fail("Should throw an IllegalArgumentException when business object definition column name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition column name must be specified.", e.getMessage());
        }

        // Try to create a business object definition column when business object definition column name is not specified.
        try
        {
            businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(
                new BusinessObjectDefinitionColumnCreateRequest(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME), BLANK_TEXT,
                    BDEF_COLUMN_DESCRIPTION));
            fail("Should throw an IllegalArgumentException when schema column name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A schema column name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionColumnSchemaColumnAlreadyLinked()
    {
        // Create and persist a business object definition column.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity = businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME),
                BDEF_COLUMN_DESCRIPTION);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format that is linked with the business object definition column.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME, businessObjectDefinitionColumnEntity);

        // Try to create another business object definition column that references the already linked schema column.
        for (String schemaColumnName : Arrays.asList(COLUMN_NAME.toUpperCase(), COLUMN_NAME.toLowerCase()))
        {
            try
            {
                businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(
                    new BusinessObjectDefinitionColumnCreateRequest(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME_2),
                        schemaColumnName, BDEF_COLUMN_DESCRIPTION_2));
                fail("Should throw an AlreadyExistsException when a business object definition column already exists with this schema column name.");
            }
            catch (AlreadyExistsException e)
            {
                assertEquals(String.format("Unable to create business object definition column because a business object definition column " +
                        "with schema column name \"%s\" already exists for the business object definition {%s}.", schemaColumnName,
                    businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionKeyAsString(BDEF_NAMESPACE, BDEF_NAME)), e.getMessage());
            }
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionColumnSchemaColumnNoExists()
    {
        // Create and persist a business object definition entity.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Try to create a business object definition column when there are no matching schema columns.
        try
        {
            businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(
                new BusinessObjectDefinitionColumnCreateRequest(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME), COLUMN_NAME,
                    BDEF_COLUMN_DESCRIPTION));
            fail("Should throw an ObjectNotFoundException when no matching schema column exists.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Unable to create business object definition column because there are no format schema columns " +
                    "with name \"%s\" for the business object definition {%s}.", COLUMN_NAME,
                businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionKeyAsString(BDEF_NAMESPACE, BDEF_NAME)), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionColumnSchemaColumnNoExistsForDescriptiveFormat()
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Add descriptive format to definition
        businessObjectDefinitionEntity.setDescriptiveBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity);

        // Try to create a business object definition column when there are no matching schema columns.
        try
        {
            businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(
                new BusinessObjectDefinitionColumnCreateRequest(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME), COLUMN_NAME,
                    BDEF_COLUMN_DESCRIPTION));
            fail("Should throw an ObjectNotFoundException when no matching schema column exists.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Unable to create business object definition column because there are no format schema " +
                    "columns with name \"%s\" in the descriptive business object format for the business object definition {%s}.", COLUMN_NAME,
                businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionKeyAsString(BDEF_NAMESPACE, BDEF_NAME)), e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionColumnTrimParameters()
    {
        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME);

        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create a business object definition column using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinitionColumn resultBusinessObjectDefinitionColumn = businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(
            new BusinessObjectDefinitionColumnCreateRequest(
                new BusinessObjectDefinitionColumnKey(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(BDEF_COLUMN_NAME)),
                addWhitespace(COLUMN_NAME), addWhitespace(BDEF_COLUMN_DESCRIPTION)));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(resultBusinessObjectDefinitionColumn.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            addWhitespace(BDEF_COLUMN_DESCRIPTION)), resultBusinessObjectDefinitionColumn);
    }

    @Test
    public void testCreateBusinessObjectDefinitionColumnUpperCaseParameters()
    {
        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME);

        // Create a business object definition column using upper case parameter values.
        BusinessObjectDefinitionColumn resultBusinessObjectDefinitionColumn = businessObjectDefinitionColumnService.createBusinessObjectDefinitionColumn(
            new BusinessObjectDefinitionColumnCreateRequest(
                new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), BDEF_COLUMN_NAME.toUpperCase()),
                COLUMN_NAME.toUpperCase(), BDEF_COLUMN_DESCRIPTION.toUpperCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(resultBusinessObjectDefinitionColumn.getId(),
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME.toUpperCase()), COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION.toUpperCase()), resultBusinessObjectDefinitionColumn);
    }

    @Test
    public void testDeleteBusinessObjectDefinitionColumn()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create and persist a business object definition column.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format that is linked with the business object definition column.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME, businessObjectDefinitionColumnEntity);

        // Validate that this business object definition column exists.
        assertNotNull(businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnByKey(businessObjectDefinitionColumnKey));

        // Delete this business object definition column.
        BusinessObjectDefinitionColumn deletedBusinessObjectDefinitionColumn =
            businessObjectDefinitionColumnService.deleteBusinessObjectDefinitionColumn(businessObjectDefinitionColumnKey);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION), deletedBusinessObjectDefinitionColumn);

        // Ensure that this business object definition column is no longer there.
        assertNull(businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnByKey(businessObjectDefinitionColumnKey));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionColumnBusinessObjectDefinitionColumnNoExists()
    {
        // Try to delete a non-existing business object definition column.
        try
        {
            businessObjectDefinitionColumnService
                .deleteBusinessObjectDefinitionColumn(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME));
            fail("Should throw an ObjectNotFoundException when business object definition column does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Column with name \"%s\" does not exist for business object definition {%s}.", BDEF_COLUMN_NAME,
                businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionKeyAsString(BDEF_NAMESPACE, BDEF_NAME)), e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectDefinitionColumnLowerCaseParameters()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create and persist a business object definition column.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format that is linked with the business object definition column.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME, businessObjectDefinitionColumnEntity);

        // Validate that this business object definition column exists.
        assertNotNull(businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnByKey(businessObjectDefinitionColumnKey));

        // Delete this business object definition column using lower case parameter values.
        BusinessObjectDefinitionColumn deletedBusinessObjectDefinitionColumn = businessObjectDefinitionColumnService.deleteBusinessObjectDefinitionColumn(
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), BDEF_COLUMN_NAME.toLowerCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION), deletedBusinessObjectDefinitionColumn);

        // Ensure that this business object definition column is no longer there.
        assertNull(businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnByKey(businessObjectDefinitionColumnKey));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionColumnMissingRequiredParameters()
    {
        // Try to delete a business object definition column when business object definition namespace is not specified.
        try
        {
            businessObjectDefinitionColumnService
                .deleteBusinessObjectDefinitionColumn(new BusinessObjectDefinitionColumnKey(BLANK_TEXT, BDEF_NAME, BDEF_COLUMN_NAME));
            fail("Should throw an IllegalArgumentException when business object definition namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to delete a business object definition column when business object definition name is not specified.
        try
        {
            businessObjectDefinitionColumnService
                .deleteBusinessObjectDefinitionColumn(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BLANK_TEXT, BDEF_COLUMN_NAME));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to delete a business object definition column when business object definition column name is not specified.
        try
        {
            businessObjectDefinitionColumnService
                .deleteBusinessObjectDefinitionColumn(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when business object definition column name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition column name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectDefinitionColumnNoLinkedSchemaColumn()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create and persist a business object definition column.
        businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Validate that this business object definition column exists.
        assertNotNull(businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnByKey(businessObjectDefinitionColumnKey));

        // Delete this business object definition column.
        BusinessObjectDefinitionColumn deletedBusinessObjectDefinitionColumn =
            businessObjectDefinitionColumnService.deleteBusinessObjectDefinitionColumn(businessObjectDefinitionColumnKey);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(deletedBusinessObjectDefinitionColumn.getId(), businessObjectDefinitionColumnKey, NO_COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION), deletedBusinessObjectDefinitionColumn);

        // Ensure that this business object definition column is no longer there.
        assertNull(businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnByKey(businessObjectDefinitionColumnKey));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionColumnTrimParameters()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create and persist a business object definition column.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format that is linked with the business object definition column.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME, businessObjectDefinitionColumnEntity);

        // Validate that this business object definition column exists.
        assertNotNull(businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnByKey(businessObjectDefinitionColumnKey));

        // Delete this business object definition column using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinitionColumn deletedBusinessObjectDefinitionColumn = businessObjectDefinitionColumnService.deleteBusinessObjectDefinitionColumn(
            new BusinessObjectDefinitionColumnKey(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(BDEF_COLUMN_NAME)));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION), deletedBusinessObjectDefinitionColumn);

        // Ensure that this business object definition column is no longer there.
        assertNull(businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnByKey(businessObjectDefinitionColumnKey));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionColumnUpperCaseParameters()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create and persist a business object definition column.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format that is linked with the business object definition column.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME, businessObjectDefinitionColumnEntity);

        // Validate that this business object definition column exists.
        assertNotNull(businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnByKey(businessObjectDefinitionColumnKey));

        // Delete this business object definition column using upper case parameter values.
        BusinessObjectDefinitionColumn deletedBusinessObjectDefinitionColumn = businessObjectDefinitionColumnService.deleteBusinessObjectDefinitionColumn(
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), BDEF_COLUMN_NAME.toUpperCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION), deletedBusinessObjectDefinitionColumn);

        // Ensure that this business object definition column is no longer there.
        assertNull(businessObjectDefinitionColumnDao.getBusinessObjectDefinitionColumnByKey(businessObjectDefinitionColumnKey));
    }

    @Test
    public void testGetBusinessObjectDefinitionColumn()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create and persist a business object definition column.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format that is linked with the business object definition column.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME, businessObjectDefinitionColumnEntity);

        // Get the business object definition column.
        BusinessObjectDefinitionColumn resultBusinessObjectDefinitionColumn =
            businessObjectDefinitionColumnService.getBusinessObjectDefinitionColumn(businessObjectDefinitionColumnKey);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION), resultBusinessObjectDefinitionColumn);
    }

    @Test
    public void testGetBusinessObjectDefinitionColumnBusinessObjectDefinitionColumnNoExists()
    {
        // Try to get a non-existing business object definition column.
        try
        {
            businessObjectDefinitionColumnService
                .getBusinessObjectDefinitionColumn(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME));
            fail("Should throw an ObjectNotFoundException when business object definition column does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Column with name \"%s\" does not exist for business object definition {%s}.", BDEF_COLUMN_NAME,
                businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionKeyAsString(BDEF_NAMESPACE, BDEF_NAME)), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDefinitionColumnLowerCaseParameters()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create and persist a business object definition column.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format that is linked with the business object definition column.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME, businessObjectDefinitionColumnEntity);

        // Get this business object definition column using lower case parameter values.
        BusinessObjectDefinitionColumn resultBusinessObjectDefinitionColumn = businessObjectDefinitionColumnService.getBusinessObjectDefinitionColumn(
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), BDEF_COLUMN_NAME.toLowerCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION), resultBusinessObjectDefinitionColumn);
    }

    @Test
    public void testGetBusinessObjectDefinitionColumnMissingRequiredParameters()
    {
        // Try to get a business object definition column when business object definition namespace is not specified.
        try
        {
            businessObjectDefinitionColumnService
                .getBusinessObjectDefinitionColumn(new BusinessObjectDefinitionColumnKey(BLANK_TEXT, BDEF_NAME, BDEF_COLUMN_NAME));
            fail("Should throw an IllegalArgumentException when business object definition namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to get a business object definition column when business object definition name is not specified.
        try
        {
            businessObjectDefinitionColumnService
                .getBusinessObjectDefinitionColumn(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BLANK_TEXT, BDEF_COLUMN_NAME));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to get a business object definition column when business object definition column name is not specified.
        try
        {
            businessObjectDefinitionColumnService
                .getBusinessObjectDefinitionColumn(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when business object definition column name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition column name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDefinitionColumnTrimParameters()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create and persist a business object definition column.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format that is linked with the business object definition column.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME, businessObjectDefinitionColumnEntity);

        // Get this business object definition column using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinitionColumn resultBusinessObjectDefinitionColumn = businessObjectDefinitionColumnService.getBusinessObjectDefinitionColumn(
            new BusinessObjectDefinitionColumnKey(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(BDEF_COLUMN_NAME)));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION), resultBusinessObjectDefinitionColumn);
    }

    @Test
    public void testGetBusinessObjectDefinitionColumnUpperCaseParameters()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create and persist a business object definition column.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format that is linked with the business object definition column.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME, businessObjectDefinitionColumnEntity);

        // Get this business object definition column using upper case parameter values.
        BusinessObjectDefinitionColumn resultBusinessObjectDefinitionColumn = businessObjectDefinitionColumnService.getBusinessObjectDefinitionColumn(
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), BDEF_COLUMN_NAME.toUpperCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION), resultBusinessObjectDefinitionColumn);
    }

    @Test
    public void testGetBusinessObjectDefinitionColumns()
    {
        // Create and persist business object definition column entities.
        businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME_2), DESCRIPTION_2);
        businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME), DESCRIPTION);

        // Get a list of business object definition column keys.
        BusinessObjectDefinitionColumnKeys resultBusinessObjectDefinitionColumnKeys =
            businessObjectDefinitionColumnService.getBusinessObjectDefinitionColumns(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumnKeys(Arrays.asList(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME),
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME_2))), resultBusinessObjectDefinitionColumnKeys);
    }

    @Test
    public void testGetBusinessObjectDefinitionColumnsBusinessObjectDefinitionColumnsNoExist()
    {
        // Create and persist a business object definition entity.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Try to get a list of business object definition column keys when business object definition columns do not exist.
        assertEquals(0, businessObjectDefinitionColumnService.getBusinessObjectDefinitionColumns(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME))
            .getBusinessObjectDefinitionColumnKeys().size());
    }

    @Test
    public void testGetBusinessObjectDefinitionColumnsBusinessObjectDefinitionNoExists()
    {
        // Try to get a list of business object definition column keys when business object definition does not exist.
        try
        {
            businessObjectDefinitionColumnService.getBusinessObjectDefinitionColumns(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME));
            fail("Should throw an ObjectNotFoundException when business object definition does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME),
                e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDefinitionColumnsLowerCaseParameters()
    {
        // Create and persist business object definition column entities.
        businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME_2), DESCRIPTION_2);
        businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME), DESCRIPTION);

        // Get a list of business object definition column keys using lower case parameter values.
        BusinessObjectDefinitionColumnKeys resultBusinessObjectDefinitionColumnKeys = businessObjectDefinitionColumnService
            .getBusinessObjectDefinitionColumns(new BusinessObjectDefinitionKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumnKeys(Arrays.asList(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME),
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME_2))), resultBusinessObjectDefinitionColumnKeys);
    }

    @Test
    public void testGetBusinessObjectDefinitionColumnsMissingRequiredParameters()
    {
        // Try to get a list of business object definition column keys when business object definition namespace is not specified.
        try
        {
            businessObjectDefinitionColumnService.getBusinessObjectDefinitionColumns(new BusinessObjectDefinitionKey(BLANK_TEXT, BDEF_NAME));
            fail("Should throw an IllegalArgumentException when business object definition namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to get a list of business object definition column keys when business object definition name is not specified.
        try
        {
            businessObjectDefinitionColumnService.getBusinessObjectDefinitionColumns(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDefinitionColumnsTrimParameters()
    {
        // Create and persist business object definition column entities.
        businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME_2), DESCRIPTION_2);
        businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME), DESCRIPTION);

        // Get a list of business object definition column keys using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinitionColumnKeys resultBusinessObjectDefinitionColumnKeys = businessObjectDefinitionColumnService
            .getBusinessObjectDefinitionColumns(new BusinessObjectDefinitionKey(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME)));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumnKeys(Arrays.asList(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME),
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME_2))), resultBusinessObjectDefinitionColumnKeys);
    }

    @Test
    public void testGetBusinessObjectDefinitionColumnsUpperCaseParameters()
    {
        // Create and persist business object definition column entities.
        businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME_2), DESCRIPTION_2);
        businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME), DESCRIPTION);

        // Get a list of business object definition column keys using upper case parameter values.
        BusinessObjectDefinitionColumnKeys resultBusinessObjectDefinitionColumnKeys = businessObjectDefinitionColumnService
            .getBusinessObjectDefinitionColumns(new BusinessObjectDefinitionKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumnKeys(Arrays.asList(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME),
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME_2))), resultBusinessObjectDefinitionColumnKeys);
    }
    
    @Test
    public void testSearchBusinessObjectDefinitionColumns()
    {
        // Create and persist business object definition column entities.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity = businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME), DESCRIPTION);
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity2 = businessObjectDefinitionColumnDaoTestHelper
            .createBusinessObjectDefinitionColumnEntity(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME_2), DESCRIPTION_2);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format that is linked with the business object definition column.
        SchemaColumnEntity schemaColumnEntity =
            schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME, businessObjectDefinitionColumnEntity);
        SchemaColumnEntity schemaColumnEntity2 =
            schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME_2, businessObjectDefinitionColumnEntity2);

        // Get a list of business object definition column keys.
        BusinessObjectDefinitionColumnSearchResponse businessObjectDefinitionColumnSearchResponse = businessObjectDefinitionColumnService
            .searchBusinessObjectDefinitionColumns(new BusinessObjectDefinitionColumnSearchRequest(Arrays
                    .asList(new BusinessObjectDefinitionColumnSearchFilter(Arrays.asList(new BusinessObjectDefinitionColumnSearchKey(BDEF_NAMESPACE, BDEF_NAME))))),
                Sets.newHashSet(SCHEMA_COLUMN_NAME_FIELD, DESCRIPTION_FIELD));

        // Validate the response object.
        assertEquals(new BusinessObjectDefinitionColumnSearchResponse(Arrays.asList(
            new BusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity.getId(),
                new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME), schemaColumnEntity.getName(),
                businessObjectDefinitionColumnEntity.getDescription()), new BusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity2.getId(),
                new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME_2), schemaColumnEntity2.getName(),
                businessObjectDefinitionColumnEntity2.getDescription()))), businessObjectDefinitionColumnSearchResponse);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionColumn()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create and persist a business object definition column.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format that is linked with the business object definition column.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME, businessObjectDefinitionColumnEntity);

        // Update the business object definition column.
        BusinessObjectDefinitionColumn updatedBusinessObjectDefinitionColumn = businessObjectDefinitionColumnService
            .updateBusinessObjectDefinitionColumn(businessObjectDefinitionColumnKey,
                new BusinessObjectDefinitionColumnUpdateRequest(BDEF_COLUMN_DESCRIPTION_2));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION_2), updatedBusinessObjectDefinitionColumn);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionColumnBusinessObjectDefinitionColumnNoExists()
    {
        // Try to update a non-existing business object definition column.
        try
        {
            businessObjectDefinitionColumnService
                .updateBusinessObjectDefinitionColumn(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME),
                    new BusinessObjectDefinitionColumnUpdateRequest(BDEF_COLUMN_DESCRIPTION));
            fail("Should throw an ObjectNotFoundException when business object definition column does not exist.");
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Column with name \"%s\" does not exist for business object definition {%s}.", BDEF_COLUMN_NAME,
                businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionKeyAsString(BDEF_NAMESPACE, BDEF_NAME)), e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDefinitionColumnLowerCaseParameters()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create and persist a business object definition column.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format that is linked with the business object definition column.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME, businessObjectDefinitionColumnEntity);

        // Update the business object definition column using lower case parameter values.
        BusinessObjectDefinitionColumn updatedBusinessObjectDefinitionColumn = businessObjectDefinitionColumnService.updateBusinessObjectDefinitionColumn(
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), BDEF_COLUMN_NAME.toLowerCase()),
            new BusinessObjectDefinitionColumnUpdateRequest(BDEF_COLUMN_DESCRIPTION_2.toLowerCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION_2.toLowerCase()), updatedBusinessObjectDefinitionColumn);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionColumnMissingOptionalParameters()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create and persist a business object definition column.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format that is linked with the business object definition column.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME, businessObjectDefinitionColumnEntity);

        // Update the business object definition column without passing optional parameters.
        BusinessObjectDefinitionColumn updatedBusinessObjectDefinitionColumn = businessObjectDefinitionColumnService
            .updateBusinessObjectDefinitionColumn(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME),
                new BusinessObjectDefinitionColumnUpdateRequest(NO_BDEF_COLUMN_DESCRIPTION));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            NO_BDEF_COLUMN_DESCRIPTION), updatedBusinessObjectDefinitionColumn);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionColumnMissingRequiredParameters()
    {
        // Try to update a business object definition column when business object definition namespace is not specified.
        try
        {
            businessObjectDefinitionColumnService
                .updateBusinessObjectDefinitionColumn(new BusinessObjectDefinitionColumnKey(BLANK_TEXT, BDEF_NAME, BDEF_COLUMN_NAME),
                    new BusinessObjectDefinitionColumnUpdateRequest(NO_BDEF_COLUMN_DESCRIPTION));
            fail("Should throw an IllegalArgumentException when business object definition namespace is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to update a business object definition column when business object definition name is not specified.
        try
        {
            businessObjectDefinitionColumnService
                .updateBusinessObjectDefinitionColumn(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BLANK_TEXT, BDEF_COLUMN_NAME),
                    new BusinessObjectDefinitionColumnUpdateRequest(NO_BDEF_COLUMN_DESCRIPTION));
            fail("Should throw an IllegalArgumentException when business object definition name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to update a business object definition column when business object definition column name is not specified.
        try
        {
            businessObjectDefinitionColumnService
                .updateBusinessObjectDefinitionColumn(new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BLANK_TEXT),
                    new BusinessObjectDefinitionColumnUpdateRequest(NO_BDEF_COLUMN_DESCRIPTION));
            fail("Should throw an IllegalArgumentException when business object definition column name is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition column name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testUpdateBusinessObjectDefinitionColumnTrimParameters()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create and persist a business object definition column.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format that is linked with the business object definition column.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME, businessObjectDefinitionColumnEntity);

        // Update the business object definition column using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinitionColumn updatedBusinessObjectDefinitionColumn = businessObjectDefinitionColumnService.updateBusinessObjectDefinitionColumn(
            new BusinessObjectDefinitionColumnKey(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(BDEF_COLUMN_NAME)),
            new BusinessObjectDefinitionColumnUpdateRequest(addWhitespace(BDEF_COLUMN_DESCRIPTION_2)));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            addWhitespace(BDEF_COLUMN_DESCRIPTION_2)), updatedBusinessObjectDefinitionColumn);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionColumnUpperCaseParameters()
    {
        // Create a business object definition column key.
        BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey =
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE, BDEF_NAME, BDEF_COLUMN_NAME);

        // Create and persist a business object definition column.
        BusinessObjectDefinitionColumnEntity businessObjectDefinitionColumnEntity =
            businessObjectDefinitionColumnDaoTestHelper.createBusinessObjectDefinitionColumnEntity(businessObjectDefinitionColumnKey, BDEF_COLUMN_DESCRIPTION);

        // Create and persist a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION,
                LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create and persist a schema column for this business object format that is linked with the business object definition column.
        schemaColumnDaoTestHelper.createSchemaColumnEntity(businessObjectFormatEntity, COLUMN_NAME, businessObjectDefinitionColumnEntity);

        // Update the business object definition column using upper case parameter values.
        BusinessObjectDefinitionColumn updatedBusinessObjectDefinitionColumn = businessObjectDefinitionColumnService.updateBusinessObjectDefinitionColumn(
            new BusinessObjectDefinitionColumnKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), BDEF_COLUMN_NAME.toUpperCase()),
            new BusinessObjectDefinitionColumnUpdateRequest(BDEF_COLUMN_DESCRIPTION_2.toUpperCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionColumn(businessObjectDefinitionColumnEntity.getId(), businessObjectDefinitionColumnKey, COLUMN_NAME,
            BDEF_COLUMN_DESCRIPTION_2.toUpperCase()), updatedBusinessObjectDefinitionColumn);
    }
}
