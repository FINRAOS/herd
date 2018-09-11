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
import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.RelationalTableRegistrationCreateRequest;
import org.finra.herd.model.dto.RelationalStorageAttributesDto;
import org.finra.herd.model.dto.RelationalTableRegistrationDto;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;

public class RelationalTableRegistrationHelperServiceTest extends AbstractServiceTest
{
    @Autowired
    @Qualifier(value = "relationalTableRegistrationHelperServiceImpl")
    private RelationalTableRegistrationHelperService relationalTableRegistrationHelperServiceImpl;

    @Test
    public void testPrepareForRelationalTableRegistrationBusinessObjectDefinitionAlreadyExists()
    {
        // Create a namespace.
        namespaceDaoTestHelper.createNamespaceEntity(BDEF_NAMESPACE);

        // Create a business object definition.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Try to a get relational storage attributes when specified business object definition already exists.
        try
        {
            relationalTableRegistrationHelperService.prepareForRelationalTableRegistration(
                new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
                    RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, STORAGE_NAME), APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_FALSE);
            fail();
        }
        catch (AlreadyExistsException ex)
        {
            Assert.assertEquals(String.format("Business object definition with name \"%s\" already exists for namespace \"%s\".", BDEF_NAME, BDEF_NAMESPACE),
                ex.getMessage());
        }
    }

    @Test
    public void testPrepareForRelationalTableRegistrationBusinessObjectFormatAlreadyExists()
    {
        // Create a namespace.
        namespaceDaoTestHelper.createNamespaceEntity(BDEF_NAMESPACE);

        // Create a business object definition.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE, 1, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY, PARTITION_KEY_GROUP);

        // Try to a get relational storage attributes when specified business object definition already exists.
        try
        {
            relationalTableRegistrationHelperService.prepareForRelationalTableRegistration(
                new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
                    RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, STORAGE_NAME), APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_TRUE);
            fail();
        }
        catch (AlreadyExistsException alreadyExistsException)
        {
            Assert.assertEquals(String.format("Format with file type \"%s\" and usage \"%s\" already exists for business object definition \"%s\".",
                FileTypeEntity.RELATIONAL_TABLE_FILE_TYPE, FORMAT_USAGE_CODE, BDEF_NAME), alreadyExistsException.getMessage());
        }
    }

    @Test
    public void testPrepareForRelationalTableRegistrationInvalidStoragePlatform()
    {
        // Create database entities required for relational table registration testing.
        relationalTableRegistrationServiceTestHelper
            .createDatabaseEntitiesForRelationalTableRegistrationTesting(BDEF_NAMESPACE, DATA_PROVIDER_NAME, STORAGE_NAME);

        // Create another storage of a storage platfom type that is not supported by the relational table registration feature.
        storageDaoTestHelper.createStorageEntity(STORAGE_NAME_2, STORAGE_PLATFORM_CODE);

        // Try to a get relational storage attributes when specified storage has an invalid storage platform type.
        try
        {
            relationalTableRegistrationHelperService.prepareForRelationalTableRegistration(
                new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
                    RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, STORAGE_NAME_2), APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_FALSE);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            Assert.assertEquals(String.format(
                "Cannot register relational table in \"%s\" storage of %s storage platform type. Only %s storage platform type is supported by this feature.",
                STORAGE_NAME_2, STORAGE_PLATFORM_CODE, StoragePlatformEntity.RELATIONAL), e.getMessage());
        }
    }

    @Test
    public void testPrepareForRelationalTableRegistrationRequiredDatabaseEntitiesNoExist()
    {
        // Create database entities required for relational table registration testing.
        relationalTableRegistrationServiceTestHelper
            .createDatabaseEntitiesForRelationalTableRegistrationTesting(BDEF_NAMESPACE, DATA_PROVIDER_NAME, STORAGE_NAME);

        // Try to get a relational storage attributes when specified namespace does not exist.
        try
        {
            relationalTableRegistrationHelperService.prepareForRelationalTableRegistration(
                new RelationalTableRegistrationCreateRequest(I_DO_NOT_EXIST, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
                    RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, STORAGE_NAME), APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_FALSE);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            Assert.assertEquals(String.format("Namespace \"%s\" doesn't exist.", I_DO_NOT_EXIST), e.getMessage());
        }

        // Try to get a relational storage attributes when specified data provider does not exist.
        try
        {
            relationalTableRegistrationHelperService.prepareForRelationalTableRegistration(
                new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, I_DO_NOT_EXIST,
                    RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, STORAGE_NAME), APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_FALSE);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            Assert.assertEquals(String.format("Data provider with name \"%s\" doesn't exist.", I_DO_NOT_EXIST), e.getMessage());
        }

        // Try to get a relational storage attributes when specified storage does not exist.
        try
        {
            relationalTableRegistrationHelperService.prepareForRelationalTableRegistration(
                new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
                    RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, I_DO_NOT_EXIST), APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_FALSE);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            Assert.assertEquals(String.format("Storage with name \"%s\" doesn't exist.", I_DO_NOT_EXIST), e.getMessage());
        }
    }

    @Test
    public void testRegisterRelationalTableBusinessObjectDefinitionAlreadyExists()
    {
        // Create a namespace.
        namespaceDaoTestHelper.createNamespaceEntity(BDEF_NAMESPACE);

        // Create a business object definition.
        businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME), DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Try to register a relational table when specified business object definition already exists.
        try
        {
            relationalTableRegistrationHelperService.registerRelationalTable(
                new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
                    RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, STORAGE_NAME), relationalTableRegistrationServiceTestHelper.getExpectedSchemaColumns(),
                APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_FALSE);
            fail();
        }
        catch (AlreadyExistsException ex)
        {
            Assert.assertEquals(String
                .format("Unable to create business object definition with name \"%s\" because it already exists for namespace \"%s\".", BDEF_NAME,
                    BDEF_NAMESPACE), ex.getMessage());
        }
    }

    @Test
    public void testRegisterRelationalTableBusinessObjectDefinitionDoesNotExist()
    {
        // Create a namespace.
        namespaceDaoTestHelper.createNamespaceEntity(BDEF_NAMESPACE);

        // Try to register a relational table when specified business object definition already exists.
        try
        {
            relationalTableRegistrationHelperService.registerRelationalTable(
                new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
                    RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, STORAGE_NAME), relationalTableRegistrationServiceTestHelper.getExpectedSchemaColumns(),
                APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_TRUE);
            fail();
        }
        catch (ObjectNotFoundException objectNotFoundException)
        {
            Assert.assertEquals(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", BDEF_NAME, BDEF_NAMESPACE),
                objectNotFoundException.getMessage());
        }
    }

    @Test
    public void testRegisterRelationalTableRequiredDatabaseEntitiesNoExist()
    {
        // Create database entities required for relational table registration testing.
        relationalTableRegistrationServiceTestHelper
            .createDatabaseEntitiesForRelationalTableRegistrationTesting(BDEF_NAMESPACE, DATA_PROVIDER_NAME, STORAGE_NAME);

        // Try to register a relational table when specified namespace does not exist.
        try
        {
            relationalTableRegistrationHelperService.registerRelationalTable(
                new RelationalTableRegistrationCreateRequest(I_DO_NOT_EXIST, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
                    RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, STORAGE_NAME), relationalTableRegistrationServiceTestHelper.getExpectedSchemaColumns(),
                APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_FALSE);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            Assert.assertEquals(String.format("Namespace \"%s\" doesn't exist.", I_DO_NOT_EXIST), e.getMessage());
        }

        // Try to register a relational table when specified data provider does not exist.
        try
        {
            relationalTableRegistrationHelperService.registerRelationalTable(
                new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, I_DO_NOT_EXIST,
                    RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, STORAGE_NAME), relationalTableRegistrationServiceTestHelper.getExpectedSchemaColumns(),
                APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_FALSE);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            Assert.assertEquals(String.format("Data provider with name \"%s\" doesn't exist.", I_DO_NOT_EXIST), e.getMessage());
        }

        // Try to register a relational table when specified storage does not exist.
        try
        {
            relationalTableRegistrationHelperService.registerRelationalTable(
                new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
                    RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, I_DO_NOT_EXIST), relationalTableRegistrationServiceTestHelper.getExpectedSchemaColumns(),
                APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_FALSE);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            Assert.assertEquals(String.format("Storage with name \"%s\" doesn't exist.", I_DO_NOT_EXIST), e.getMessage());
        }
    }

    /**
     * This unit test is to get coverage for the methods that have an explicit annotation for transaction propagation.
     */
    @Test
    public void testRelationalTableRegistrationHelperServiceMethodsNewTransactionPropagation()
    {
        try
        {
            relationalTableRegistrationHelperServiceImpl.prepareForRelationalTableRegistration(
                new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
                    RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, STORAGE_NAME), APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_FALSE);
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Namespace \"%s\" doesn't exist.", BDEF_NAMESPACE), e.getMessage());
        }

        try
        {
            relationalTableRegistrationHelperServiceImpl.prepareForRelationalTableSchemaUpdate(
                new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Business object data storage unit {%s, storageName: \"%s\"} doesn't exist.", businessObjectDataServiceTestHelper
                .getExpectedBusinessObjectDataKeyAsString(
                    new BusinessObjectDataKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                        SUBPARTITION_VALUES, DATA_VERSION)), STORAGE_NAME), e.getMessage());
        }

        try
        {
            relationalTableRegistrationHelperServiceImpl
                .registerRelationalTable(new RelationalTableRegistrationCreateRequest(), new ArrayList<>(), APPEND_TO_EXISTING_BUSINESS_OBJECT_DEFINTION_FALSE);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        try
        {
            relationalTableRegistrationHelperServiceImpl
                .retrieveRelationalTableColumns(new RelationalStorageAttributesDto(JDBC_URL, USERNAME, NO_USER_CREDENTIAL_NAME), RELATIONAL_SCHEMA_NAME,
                    RELATIONAL_TABLE_NAME);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format(
                "Failed to retrieve description of a relational table with \"%s\" name under \"%s\" schema at jdbc.url=\"%s\" using jdbc.username=\"%s\". " +
                    "Reason: Wrong user name or password [28000-196]", RELATIONAL_TABLE_NAME, RELATIONAL_SCHEMA_NAME, JDBC_URL, USERNAME), e.getMessage());
        }

        try
        {
            relationalTableRegistrationHelperServiceImpl.updateRelationalTableSchema(new RelationalTableRegistrationDto(
                new BusinessObjectDataStorageUnitKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    SUBPARTITION_VALUES, DATA_VERSION, STORAGE_NAME), new RelationalStorageAttributesDto(), RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME,
                new BusinessObjectFormat(ID, BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
                    PARTITION_KEY, DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA,
                    NO_BUSINESS_OBJECT_FORMAT_PARENTS, NO_BUSINESS_OBJECT_FORMAT_CHILDREN, NO_RECORD_FLAG_SET, NO_RETENTION_PERIOD_IN_DAYS, NO_RETENTION_TYPE,
                    NO_ALLOW_NON_BACKWARDS_COMPATIBLE_CHANGES_SET)), NO_COLUMNS);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A schema must have at least one column.", e.getMessage());
        }

        try
        {
            relationalTableRegistrationHelperServiceImpl
                .validateAndTrimRelationalTableRegistrationCreateRequest(new RelationalTableRegistrationCreateRequest());
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }
    }

    @Test
    public void testRetrieveRelationalTableColumnsRelationTableNoExists()
    {
        // Create and initialize a relational storage attributes DTO to point to the in-memory database setup as part of DAO mocks.
        RelationalStorageAttributesDto relationalStorageAttributesDto = new RelationalStorageAttributesDto();
        relationalStorageAttributesDto.setJdbcUrl(JDBC_URL);
        relationalStorageAttributesDto.setJdbcUsername(EMPTY_STRING);
        relationalStorageAttributesDto.setJdbcUserCredentialName(NO_USER_CREDENTIAL_NAME);

        // Try to get a list of schema columns for a non-existing relational table.
        try
        {
            relationalTableRegistrationHelperService.retrieveRelationalTableColumns(relationalStorageAttributesDto, RELATIONAL_SCHEMA_NAME, I_DO_NOT_EXIST);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String
                .format("Relational table with \"%s\" name not found under \"%s\" schema at jdbc.url=\"%s\" for jdbc.username=\"%s\".", I_DO_NOT_EXIST,
                    RELATIONAL_SCHEMA_NAME, JDBC_URL, EMPTY_STRING), e.getMessage());
        }
    }

    @Test
    public void testRetrieveRelationalTableColumnsSqlException()
    {
        // Create and initialize a relational storage attributes DTO with an invalid JDBC URL.
        RelationalStorageAttributesDto relationalStorageAttributesDto = new RelationalStorageAttributesDto();
        relationalStorageAttributesDto.setJdbcUrl(INVALID_VALUE);
        relationalStorageAttributesDto.setJdbcUsername(USERNAME);
        relationalStorageAttributesDto.setJdbcUserCredentialName(NO_USER_CREDENTIAL_NAME);

        // Try to get a list of schema columns using an invalid JDBC URL.
        try
        {
            relationalTableRegistrationHelperService
                .retrieveRelationalTableColumns(relationalStorageAttributesDto, RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format(
                "Failed to retrieve description of a relational table with \"%s\" name under \"%s\" schema at jdbc.url=\"%s\" using jdbc.username=\"%s\". " +
                    "Reason: No suitable driver found for %s", RELATIONAL_TABLE_NAME, RELATIONAL_SCHEMA_NAME, INVALID_VALUE, USERNAME, INVALID_VALUE),
                e.getMessage());
        }
    }

    @Test
    public void testValidateAndTrimRelationalTableRegistrationCreateRequestMissingOptionalParametersAsBlanks()
    {
        // Create a relational table registration create request with optional parameters passed as blank strings.
        RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest =
            new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BLANK_TEXT, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME, RELATIONAL_SCHEMA_NAME,
                RELATIONAL_TABLE_NAME, STORAGE_NAME);

        // Validate and trim the create request.
        relationalTableRegistrationHelperService.validateAndTrimRelationalTableRegistrationCreateRequest(relationalTableRegistrationCreateRequest);

        // Validate the results.
        assertEquals(
            new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, EMPTY_STRING, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME, RELATIONAL_SCHEMA_NAME,
                RELATIONAL_TABLE_NAME, STORAGE_NAME), relationalTableRegistrationCreateRequest);
    }

    @Test
    public void testValidateAndTrimRelationalTableRegistrationCreateRequestMissingOptionalParametersAsNulls()
    {
        // Create a relational table registration create request with optional parameters passed as nulls.
        RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest =
            new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, NO_BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
                RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, STORAGE_NAME);

        // Validate and trim the create request.
        relationalTableRegistrationHelperService.validateAndTrimRelationalTableRegistrationCreateRequest(relationalTableRegistrationCreateRequest);

        // Validate the results.
        assertEquals(new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, NO_BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
            RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, STORAGE_NAME), relationalTableRegistrationCreateRequest);
    }

    @Test
    public void testValidateAndTrimRelationalTableRegistrationCreateRequestMissingRequiredParameters()
    {
        // Try to validate a null create request.
        try
        {
            relationalTableRegistrationHelperService.validateAndTrimRelationalTableRegistrationCreateRequest(null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A relational table registration create request must be specified.", e.getMessage());
        }

        // Try to validate a create request with a missing namespace.
        try
        {
            relationalTableRegistrationHelperService.validateAndTrimRelationalTableRegistrationCreateRequest(
                new RelationalTableRegistrationCreateRequest(BLANK_TEXT, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
                    RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, STORAGE_NAME));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to validate a create request with a missing business object definition name.
        try
        {
            relationalTableRegistrationHelperService.validateAndTrimRelationalTableRegistrationCreateRequest(
                new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BLANK_TEXT, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
                    RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, STORAGE_NAME));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to validate a create request with a missing business object format usage.
        try
        {
            relationalTableRegistrationHelperService.validateAndTrimRelationalTableRegistrationCreateRequest(
                new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, BLANK_TEXT, DATA_PROVIDER_NAME,
                    RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, STORAGE_NAME));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object format usage must be specified.", e.getMessage());
        }

        // Try to validate a create request with a missing data provider name.
        try
        {
            relationalTableRegistrationHelperService.validateAndTrimRelationalTableRegistrationCreateRequest(
                new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, BLANK_TEXT,
                    RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, STORAGE_NAME));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A data provider name must be specified.", e.getMessage());
        }

        // Try to validate a create request with a missing relational schema name.
        try
        {
            relationalTableRegistrationHelperService.validateAndTrimRelationalTableRegistrationCreateRequest(
                new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME, BLANK_TEXT,
                    RELATIONAL_TABLE_NAME, STORAGE_NAME));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A relational schema name must be specified.", e.getMessage());
        }

        // Try to validate a create request with a missing relational table name.
        try
        {
            relationalTableRegistrationHelperService.validateAndTrimRelationalTableRegistrationCreateRequest(
                new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
                    RELATIONAL_SCHEMA_NAME, BLANK_TEXT, STORAGE_NAME));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A relational table name must be specified.", e.getMessage());
        }

        // Try to validate a create request with a missing storage name.
        try
        {
            relationalTableRegistrationHelperService.validateAndTrimRelationalTableRegistrationCreateRequest(
                new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
                    RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, BLANK_TEXT));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A storage name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testValidateAndTrimRelationalTableRegistrationCreateRequestTrimParameters()
    {
        // Create a relational table registration create request with parameters having leading and trailing empty spaces.
        RelationalTableRegistrationCreateRequest relationalTableRegistrationCreateRequest =
            new RelationalTableRegistrationCreateRequest(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(BDEF_DISPLAY_NAME),
                addWhitespace(FORMAT_USAGE_CODE), addWhitespace(DATA_PROVIDER_NAME), addWhitespace(RELATIONAL_SCHEMA_NAME),
                addWhitespace(RELATIONAL_TABLE_NAME), addWhitespace(STORAGE_NAME));

        // Validate and trim the create request.
        relationalTableRegistrationHelperService.validateAndTrimRelationalTableRegistrationCreateRequest(relationalTableRegistrationCreateRequest);

        // Validate the results.
        assertEquals(new RelationalTableRegistrationCreateRequest(BDEF_NAMESPACE, BDEF_NAME, BDEF_DISPLAY_NAME, FORMAT_USAGE_CODE, DATA_PROVIDER_NAME,
            RELATIONAL_SCHEMA_NAME, RELATIONAL_TABLE_NAME, STORAGE_NAME), relationalTableRegistrationCreateRequest);
    }
}
