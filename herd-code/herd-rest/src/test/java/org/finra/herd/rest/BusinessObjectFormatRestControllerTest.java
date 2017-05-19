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
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatAttributesUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdl;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKeys;
import org.finra.herd.model.api.xml.BusinessObjectFormatParentsUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatUpdateRequest;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.helper.Hive13DdlGenerator;

/**
 * This class tests various functionality within the business object format REST controller.
 */
public class BusinessObjectFormatRestControllerTest extends AbstractRestTest
{
    @Test
    public void testCreateBusinessObjectFormat()
    {
        // Create relative database entities.
        businessObjectFormatServiceTestHelper.createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of the business object format.
        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(),
                businessObjectFormatServiceTestHelper.getTestSchema());

        // Create an initial version of a business object format.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
                PARTITION_KEY, FORMAT_DESCRIPTION, businessObjectDefinitionServiceTestHelper.getNewAttributes(),
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema(),
                businessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormat()
    {
        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormat originalBusinessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat();

        // Create a new partition key group for the update request.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update by changing the description and schema.
        BusinessObjectFormatUpdateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, NO_ATTRIBUTES, businessObjectFormatServiceTestHelper.getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormat(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION_2, NO_ATTRIBUTES,
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema2(),
                updatedBusinessObjectFormat);
    }

    @Test
    public void testGetBusinessObjectFormat()
    {
        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormat originalBusinessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat();

        // Call GET Business Object Format.
        BusinessObjectFormat resultBusinessObjectFormat =
            businessObjectFormatRestController.getBusinessObjectFormat(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, NO_ATTRIBUTES,
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema(),
                resultBusinessObjectFormat);
    }

    @Test
    public void testDeleteBusinessObjectFormat() throws Exception
    {
        // Create an initial version of a business object format.
        businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat();

        // Retrieve the business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Delete the business object format.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatRestController
            .deleteBusinessObjectFormat(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, NO_ATTRIBUTES,
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema(),
                deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testGetBusinessObjectFormats()
    {
        // Create and persist the relative business object definitions.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : businessObjectFormatDaoTestHelper.getTestBusinessObjectFormatKeys())
        {
            businessObjectFormatDaoTestHelper
                .createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                    key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, false, PARTITION_KEY);
        }

        // Retrieve a list of business object format keys for the specified business object definition.
        BusinessObjectFormatKeys resultKeys = businessObjectFormatRestController.getBusinessObjectFormats(NAMESPACE, BDEF_NAME, false);

        // Validate the returned object.
        assertEquals(businessObjectFormatDaoTestHelper.getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());

        // Retrieve a list of the latest version business object format keys for the specified business object definition.
        resultKeys = businessObjectFormatRestController.getBusinessObjectFormats(NAMESPACE, BDEF_NAME, true);

        // Validate the returned object.
        assertEquals(businessObjectFormatDaoTestHelper.getExpectedBusinessObjectFormatLatestVersionKeys(), resultKeys.getBusinessObjectFormatKeys());
    }

    @Test
    public void testGenerateBusinessObjectFormatDdl()
    {
        // Prepare test data.
        businessObjectFormatServiceTestHelper.createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        BusinessObjectFormatDdlRequest request;
        BusinessObjectFormatDdl resultDdl;

        // Retrieve business object format ddl.
        request = businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        businessObjectFormatServiceTestHelper.validateBusinessObjectFormatDdl(CUSTOM_DDL_NAME, businessObjectFormatServiceTestHelper
            .getExpectedBusinessObjectFormatDdl(AbstractServiceTest.PARTITION_COLUMNS.length, AbstractServiceTest.FIRST_COLUMN_NAME,
                AbstractServiceTest.FIRST_COLUMN_DATA_TYPE, AbstractServiceTest.ROW_FORMAT, Hive13DdlGenerator.TEXT_HIVE_FILE_FORMAT,
                FileTypeEntity.TXT_FILE_TYPE, true, true), resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlCollection()
    {
        // Prepare database entities required for testing.
        businessObjectFormatServiceTestHelper.createDatabaseEntitiesForBusinessObjectFormatDdlCollectionTesting();

        // Generate DDL for a collection of business object formats.
        BusinessObjectFormatDdlCollectionResponse resultBusinessObjectFormatDdlCollectionResponse = businessObjectFormatRestController
            .generateBusinessObjectFormatDdlCollection(businessObjectFormatServiceTestHelper.getTestBusinessObjectFormatDdlCollectionRequest());

        // Validate the response object.
        assertEquals(businessObjectFormatServiceTestHelper.getExpectedBusinessObjectFormatDdlCollectionResponse(),
            resultBusinessObjectFormatDdlCollectionResponse);
    }

    @Test
    public void testUpdateBusinessObjectFormatParents()
    {
        // Create relative database entities including a business object definition.
        businessObjectFormatServiceTestHelper
            .createTestDatabaseEntitiesForBusinessObjectFormatTesting(NAMESPACE, DATA_PROVIDER_NAME, BDEF_NAME, FORMAT_FILE_TYPE_CODE, PARTITION_KEY_GROUP);

        BusinessObjectFormatCreateRequest request = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, NO_FORMAT_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA);

        businessObjectFormatService.createBusinessObjectFormat(request);

        BusinessObjectFormatCreateRequest request2 = businessObjectFormatServiceTestHelper
            .createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, NO_FORMAT_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes(), NO_ATTRIBUTE_DEFINITIONS, NO_SCHEMA);

        businessObjectFormatService.createBusinessObjectFormat(request2);

        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, null);
        BusinessObjectFormatKey parentBusinessObjectFormatKey = new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, null);
        BusinessObjectFormatParentsUpdateRequest updateRequest = new  BusinessObjectFormatParentsUpdateRequest();
        updateRequest.setBusinessObjectFormatParents(Arrays.asList(parentBusinessObjectFormatKey));

        BusinessObjectFormat expectedFormat = businessObjectFormatService.getBusinessObjectFormat(businessObjectFormatKey);
        expectedFormat.setBusinessObjectFormatParents(Arrays.asList(parentBusinessObjectFormatKey));

        BusinessObjectFormat resultBusinessObjectFormat = businessObjectFormatRestController.updateBusinessObjectFormatParents(businessObjectFormatKey.getNamespace(), businessObjectFormatKey.getBusinessObjectDefinitionName(), businessObjectFormatKey.getBusinessObjectFormatUsage(), businessObjectFormatKey.getBusinessObjectFormatFileType() , updateRequest);

        assertEquals(expectedFormat, resultBusinessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormatAttributes()
    {
        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormat originalBusinessObjectFormat = businessObjectFormatServiceTestHelper.createTestBusinessObjectFormat();

        List<Attribute> attributes = businessObjectDefinitionServiceTestHelper.getNewAttributes2();
        BusinessObjectFormatAttributesUpdateRequest request = new BusinessObjectFormatAttributesUpdateRequest(attributes);
        
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormatAttributes(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);

        // Validate the returned object.
        businessObjectFormatServiceTestHelper
            .validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
                PARTITION_KEY, FORMAT_DESCRIPTION, attributes,
                businessObjectFormatServiceTestHelper.getTestAttributeDefinitions(), businessObjectFormatServiceTestHelper.getTestSchema(),
                updatedBusinessObjectFormat);
    }
}
