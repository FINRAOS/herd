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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.AttributeDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatAttributeDefinitionsUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatAttributesUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdl;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKeys;
import org.finra.herd.model.api.xml.BusinessObjectFormatParentsUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatRetentionInformationUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatSchemaBackwardsCompatibilityUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatUpdateRequest;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.RetentionTypeEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.BusinessObjectFormatService;
import org.finra.herd.service.helper.Hive13DdlGenerator;

/**
 * This class tests various functionality within the business object format REST controller.
 */
public class BusinessObjectFormatRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private BusinessObjectFormatRestController businessObjectFormatRestController;

    @Mock
    private BusinessObjectFormatService businessObjectFormatService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateBusinessObjectFormat()
    {
        // Create an initial version of the business object format.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema());

        // Create an initial version of a business object format.
        BusinessObjectFormat businessObjectFormat =
            new BusinessObjectFormat(ID, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 0, true, PARTITION_KEY, FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema(), null, null,
                NO_RECORD_FLAG_SET, NO_RETENTION_PERIOD_IN_DAYS, NO_RETENTION_TYPE, NO_ALLOW_NON_BACKWARDS_COMPATIBLE_CHANGES_SET);

        when(businessObjectFormatService.createBusinessObjectFormat(request)).thenReturn(businessObjectFormat);
        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Verify the external calls.
        verify(businessObjectFormatService).createBusinessObjectFormat(request);
        verifyNoMoreInteractions(businessObjectFormatService);
        // Validate the returned object.
        assertEquals(businessObjectFormat, resultBusinessObjectFormat);
    }

    @Test
    public void testDeleteBusinessObjectFormat() throws Exception
    {
        BusinessObjectFormat businessObjectFormat =
            new BusinessObjectFormat(ID, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, true, PARTITION_KEY,
                FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(),
                businessObjectFormatServiceTestHelper.getTestSchema(), null, null, NO_RECORD_FLAG_SET, NO_RETENTION_PERIOD_IN_DAYS, NO_RETENTION_TYPE,
                NO_ALLOW_NON_BACKWARDS_COMPATIBLE_CHANGES_SET);

        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);

        when(businessObjectFormatService.deleteBusinessObjectFormat(businessObjectFormatKey)).thenReturn(businessObjectFormat);

        // Create an initial version of a business object format.
        businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat();
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatRestController
            .deleteBusinessObjectFormat(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);

        // Verify the external calls.
        verify(businessObjectFormatService).deleteBusinessObjectFormat(businessObjectFormatKey);
        verifyNoMoreInteractions(businessObjectFormatService);
        // Validate the returned object.
        assertEquals(businessObjectFormat, deletedBusinessObjectFormat);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdl()
    {
        BusinessObjectFormatDdl ddl = new BusinessObjectFormatDdl();
        ddl.setBusinessObjectDefinitionName(BDEF_NAME);
        ddl.setCustomDdlName(CUSTOM_DDL_NAME);
        ddl.setNamespace(NAMESPACE);
        ddl.setDdl(businessObjectFormatServiceTestHelper
            .getExpectedBusinessObjectFormatDdl(AbstractServiceTest.PARTITION_COLUMNS.length, AbstractServiceTest.FIRST_COLUMN_NAME,
                AbstractServiceTest.FIRST_COLUMN_DATA_TYPE, AbstractServiceTest.ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, true, true));

        // Retrieve business object format ddl.
        BusinessObjectFormatDdlRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);

        when(businessObjectFormatService.generateBusinessObjectFormatDdl(request)).thenReturn(ddl);

        BusinessObjectFormatDdl resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Verify the external calls.
        verify(businessObjectFormatService).generateBusinessObjectFormatDdl(request);
        verifyNoMoreInteractions(businessObjectFormatService);
        // Validate the returned object.
        assertEquals(ddl, resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlCollection()
    {
        BusinessObjectFormatDdlCollectionRequest request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlCollectionRequest();

        BusinessObjectFormatDdlCollectionResponse businessObjectFormatDdlCollectionResponse =
            businessObjectFormatServiceTestHelper.getExpectedBusinessObjectFormatDdlCollectionResponse();

        when(businessObjectFormatService.generateBusinessObjectFormatDdlCollection(request)).thenReturn(businessObjectFormatDdlCollectionResponse);

        // Generate DDL for a collection of business object formats.
        BusinessObjectFormatDdlCollectionResponse resultBusinessObjectFormatDdlCollectionResponse =
            businessObjectFormatRestController.generateBusinessObjectFormatDdlCollection(request);

        // Verify the external calls.
        verify(businessObjectFormatService).generateBusinessObjectFormatDdlCollection(request);
        verifyNoMoreInteractions(businessObjectFormatService);
        // Validate the returned object.
        assertEquals(businessObjectFormatDdlCollectionResponse, resultBusinessObjectFormatDdlCollectionResponse);
    }

    @Test
    public void testGetBusinessObjectFormat()
    {
        BusinessObjectFormat businessObjectFormat =
            new BusinessObjectFormat(ID, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, true, PARTITION_KEY,
                FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(),
                businessObjectFormatServiceTestHelper.getTestSchema(), null, null, NO_RECORD_FLAG_SET, NO_RETENTION_PERIOD_IN_DAYS, NO_RETENTION_TYPE,
                NO_ALLOW_NON_BACKWARDS_COMPATIBLE_CHANGES_SET);

        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);

        when(businessObjectFormatService.getBusinessObjectFormat(businessObjectFormatKey)).thenReturn(businessObjectFormat);

        // Call GET Business Object Format.
        BusinessObjectFormat resultBusinessObjectFormat =
            businessObjectFormatRestController.getBusinessObjectFormat(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);

        // Verify the external calls.
        verify(businessObjectFormatService).getBusinessObjectFormat(businessObjectFormatKey);
        verifyNoMoreInteractions(businessObjectFormatService);
        // Validate the returned object.
        assertEquals(businessObjectFormat, resultBusinessObjectFormat);
    }

    @Test
    public void testGetBusinessObjectFormats()
    {
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        BusinessObjectFormatKeys businessObjectFormatKeys = new BusinessObjectFormatKeys(Arrays
            .asList(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, INITIAL_FORMAT_VERSION),
                new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION, INITIAL_FORMAT_VERSION)));

        when(businessObjectFormatService.getBusinessObjectFormats(businessObjectDefinitionKey, false)).thenReturn(businessObjectFormatKeys);
        // Retrieve a list of business object format keys for the specified business object definition.
        BusinessObjectFormatKeys resultKeys = businessObjectFormatRestController.getBusinessObjectFormats(NAMESPACE, BDEF_NAME, false);

        // Verify the external calls.
        verify(businessObjectFormatService).getBusinessObjectFormats(businessObjectDefinitionKey, false);
        verifyNoMoreInteractions(businessObjectFormatService);
        // Validate the returned object.
        assertEquals(businessObjectFormatKeys, resultKeys);
    }

    @Test
    public void testGetBusinessObjectFormatsWithFilters()
    {
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);
        BusinessObjectFormatKeys businessObjectFormatKeys = new BusinessObjectFormatKeys(Arrays
            .asList(new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, INITIAL_FORMAT_VERSION),
                new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION, INITIAL_FORMAT_VERSION)));

        when(businessObjectFormatService.getBusinessObjectFormatsWithFilters(businessObjectDefinitionKey, FORMAT_USAGE_CODE, false))
            .thenReturn(businessObjectFormatKeys);
        // Retrieve a list of business object format keys for the specified business object definition.
        BusinessObjectFormatKeys resultKeys =
            businessObjectFormatRestController.getBusinessObjectFormatsWithFilters(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, false);

        // Verify the external calls.
        verify(businessObjectFormatService).getBusinessObjectFormatsWithFilters(businessObjectDefinitionKey, FORMAT_USAGE_CODE, false);
        verifyNoMoreInteractions(businessObjectFormatService);
        // Validate the returned object.
        assertEquals(businessObjectFormatKeys, resultKeys);
    }

    @Test
    public void testUpdateBusinessObjectFormat()
    {
        BusinessObjectFormat businessObjectFormat =
            new BusinessObjectFormat(ID, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, 1, true, PARTITION_KEY, FORMAT_DESCRIPTION_2,
                NO_FORMAT_DOCUMENT_SCHEMA, NO_ATTRIBUTES, businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(),
                businessObjectFormatServiceTestHelper.getTestSchema2(), null, null, NO_RECORD_FLAG_SET, NO_RETENTION_PERIOD_IN_DAYS, NO_RETENTION_TYPE,
                NO_ALLOW_NON_BACKWARDS_COMPATIBLE_CHANGES_SET);

        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);

        // Perform an update by changing the description and schema.
        BusinessObjectFormatUpdateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, FORMAT_DOCUMENT_SCHEMA_2, NO_ATTRIBUTES, businessObjectFormatServiceTestHelper.getTestSchema2());

        when(businessObjectFormatService.updateBusinessObjectFormat(businessObjectFormatKey, request)).thenReturn(businessObjectFormat);

        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormat(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);

        // Verify the external calls.
        verify(businessObjectFormatService).updateBusinessObjectFormat(businessObjectFormatKey, request);
        verifyNoMoreInteractions(businessObjectFormatService);
        // Validate the returned object.
        assertEquals(businessObjectFormat, updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatAttributes()
    {
        List<Attribute> attributes = businessObjectDefinitionServiceTestHelper.getNewAttributes2();
        BusinessObjectFormat businessObjectFormat = new BusinessObjectFormat();
        businessObjectFormat.setAttributes(attributes);

        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);
        BusinessObjectFormatAttributesUpdateRequest request = new BusinessObjectFormatAttributesUpdateRequest(attributes);

        when(businessObjectFormatService.updateBusinessObjectFormatAttributes(businessObjectFormatKey, request)).thenReturn(businessObjectFormat);

        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormatAttributes(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);

        // Verify the external calls.
        verify(businessObjectFormatService).updateBusinessObjectFormatAttributes(businessObjectFormatKey, request);
        verifyNoMoreInteractions(businessObjectFormatService);
        // Validate the returned object.
        assertEquals(businessObjectFormat, updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatAttributeDefinitions()
    {
        List<AttributeDefinition> attributeDefinitions = businessObjectFormatServiceTestHelper.getTestAttributeDefinitions();
        BusinessObjectFormat businessObjectFormat = new BusinessObjectFormat();
        businessObjectFormat.setAttributeDefinitions(attributeDefinitions);

        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);
        BusinessObjectFormatAttributeDefinitionsUpdateRequest request = new BusinessObjectFormatAttributeDefinitionsUpdateRequest(attributeDefinitions);

        when(businessObjectFormatService.updateBusinessObjectFormatAttributeDefinitions(businessObjectFormatKey, request)).thenReturn(businessObjectFormat);

        BusinessObjectFormat result = businessObjectFormatRestController
            .updateBusinessObjectFormatAttributeDefinitions(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);

        // Verify the external calls.
        verify(businessObjectFormatService).updateBusinessObjectFormatAttributeDefinitions(businessObjectFormatKey, request);
        verifyNoMoreInteractions(businessObjectFormatService);
        // Validate the returned object.
        assertEquals(businessObjectFormat, result);
    }

    @Test
    public void testUpdateBusinessObjectFormatParents()
    {
        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null);
        BusinessObjectFormatKey parentBusinessObjectFormatKey =
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, null);
        BusinessObjectFormatParentsUpdateRequest updateRequest = new BusinessObjectFormatParentsUpdateRequest();
        updateRequest.setBusinessObjectFormatParents(Arrays.asList(parentBusinessObjectFormatKey));

        BusinessObjectFormat businessObjectFormat = new BusinessObjectFormat();
        businessObjectFormat.setBusinessObjectFormatParents(Arrays.asList(parentBusinessObjectFormatKey));

        when(businessObjectFormatService.updateBusinessObjectFormatParents(businessObjectFormatKey, updateRequest)).thenReturn(businessObjectFormat);

        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormatParents(businessObjectFormatKey.getNamespace(), businessObjectFormatKey.getBusinessObjectDefinitionName(),
                businessObjectFormatKey.getBusinessObjectFormatUsage(), businessObjectFormatKey.getBusinessObjectFormatFileType(), updateRequest);

        // Verify the external calls.
        verify(businessObjectFormatService).updateBusinessObjectFormatParents(businessObjectFormatKey, updateRequest);
        verifyNoMoreInteractions(businessObjectFormatService);
        // Validate the returned object.
        assertEquals(businessObjectFormat, resultBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatRetentionInformation()
    {
        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null);
        BusinessObjectFormatRetentionInformationUpdateRequest updateRequest = new BusinessObjectFormatRetentionInformationUpdateRequest();
        updateRequest.setRetentionType(RetentionTypeEntity.PARTITION_VALUE);
        updateRequest.setRecordFlag(true);
        updateRequest.setRetentionPeriodInDays(RETENTION_PERIOD_DAYS);

        BusinessObjectFormat businessObjectFormat = new BusinessObjectFormat();
        when(businessObjectFormatService.updateBusinessObjectFormatRetentionInformation(businessObjectFormatKey, updateRequest))
            .thenReturn(businessObjectFormat);

        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormatRetentionInformation(businessObjectFormatKey.getNamespace(), businessObjectFormatKey.getBusinessObjectDefinitionName(),
                businessObjectFormatKey.getBusinessObjectFormatUsage(), businessObjectFormatKey.getBusinessObjectFormatFileType(), updateRequest);

        // Verify the external calls.
        verify(businessObjectFormatService).updateBusinessObjectFormatRetentionInformation(businessObjectFormatKey, updateRequest);
        verifyNoMoreInteractions(businessObjectFormatService);
        // Validate the returned object.
        assertEquals(businessObjectFormat, updatedBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatSchemaCompatibleChanges()
    {
        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null);
        BusinessObjectFormatSchemaBackwardsCompatibilityUpdateRequest updateRequest = new BusinessObjectFormatSchemaBackwardsCompatibilityUpdateRequest();
        updateRequest.setAllowNonBackwardsCompatibleChanges(true);

        BusinessObjectFormat businessObjectFormat = new BusinessObjectFormat();
        when(businessObjectFormatService.updateBusinessObjectFormatSchemaBackwardsCompatibilityChanges(businessObjectFormatKey, updateRequest))
            .thenReturn(businessObjectFormat);

        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormatSchemaBackwardsCompatibleChanges(businessObjectFormatKey.getNamespace(),
                businessObjectFormatKey.getBusinessObjectDefinitionName(), businessObjectFormatKey.getBusinessObjectFormatUsage(),
                businessObjectFormatKey.getBusinessObjectFormatFileType(), updateRequest);

        // Verify the external calls.
        verify(businessObjectFormatService).updateBusinessObjectFormatSchemaBackwardsCompatibilityChanges(businessObjectFormatKey, updateRequest);
        verifyNoMoreInteractions(businessObjectFormatService);
        // Validate the returned object.
        assertEquals(businessObjectFormat, updatedBusinessObjectFormat);
    }
}
