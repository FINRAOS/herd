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

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdl;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKeys;
import org.finra.herd.model.api.xml.BusinessObjectFormatUpdateRequest;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;

/**
 * This class tests various functionality within the business object format REST controller.
 */
public class BusinessObjectFormatRestControllerTest extends AbstractRestTest
{
    @Test
    public void testCreateBusinessObjectFormat()
    {
        // Create relative database entities.
        createTestDatabaseEntitiesForBusinessObjectFormatTesting();

        // Create an initial version of the business object format.
        BusinessObjectFormatCreateRequest request =
            createBusinessObjectFormatCreateRequest(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, PARTITION_KEY, FORMAT_DESCRIPTION,
                getNewAttributes(), getTestAttributeDefinitions(), getTestSchema());

        // Create an initial version of a business object format.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatRestController.createBusinessObjectFormat(request);

        // Validate the returned object.
        validateBusinessObjectFormat(null, NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, LATEST_VERSION_FLAG_SET,
            PARTITION_KEY, FORMAT_DESCRIPTION, getNewAttributes(), getTestAttributeDefinitions(), getTestSchema(), businessObjectFormat);
    }

    @Test
    public void testUpdateBusinessObjectFormat()
    {
        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat();

        // Create a new partition key group for the update request.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Perform an update by changing the description and schema.
        BusinessObjectFormatUpdateRequest request = createBusinessObjectFormatUpdateRequest(FORMAT_DESCRIPTION_2, NO_ATTRIBUTES, getTestSchema2());
        BusinessObjectFormat updatedBusinessObjectFormat = businessObjectFormatRestController
            .updateBusinessObjectFormat(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, request);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION_2, NO_ATTRIBUTES, getTestAttributeDefinitions(), getTestSchema2(),
            updatedBusinessObjectFormat);
    }

    @Test
    public void testGetBusinessObjectFormat()
    {
        // Create an initial version of a business object format with format description and schema information.
        BusinessObjectFormat originalBusinessObjectFormat = createTestBusinessObjectFormat();

        // Call GET Business Object Format.
        BusinessObjectFormat resultBusinessObjectFormat =
            businessObjectFormatRestController.getBusinessObjectFormat(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);

        // Validate the returned object.
        validateBusinessObjectFormat(originalBusinessObjectFormat.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
            INITIAL_FORMAT_VERSION, true, PARTITION_KEY, FORMAT_DESCRIPTION, NO_ATTRIBUTES, getTestAttributeDefinitions(), getTestSchema(),
            resultBusinessObjectFormat);
    }

    @Test
    public void testDeleteBusinessObjectFormat() throws Exception
    {
        // Create an initial version of a business object format.
        createTestBusinessObjectFormat();

        // Retrieve the business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION));

        // Delete the business object format.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatRestController
            .deleteBusinessObjectFormat(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION);

        // Validate the returned object.
        validateBusinessObjectFormat(businessObjectFormatEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION,
            true, PARTITION_KEY, FORMAT_DESCRIPTION, NO_ATTRIBUTES, getTestAttributeDefinitions(), getTestSchema(), deletedBusinessObjectFormat);

        // Ensure that this business object format is no longer there.
        assertNull(businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION)));
    }

    @Test
    public void testGetBusinessObjectFormats()
    {
        // Create and persist the relative business object definitions.
        createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);
        createBusinessObjectDefinitionEntity(NAMESPACE_2, BDEF_NAME_2, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, null);

        // Create and persist business object format entities.
        for (BusinessObjectFormatKey key : getTestBusinessObjectFormatKeys())
        {
            createBusinessObjectFormatEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), key.getBusinessObjectFormatUsage(),
                key.getBusinessObjectFormatFileType(), key.getBusinessObjectFormatVersion(), FORMAT_DESCRIPTION, false, PARTITION_KEY);
        }

        // Retrieve a list of business object format keys for the specified business object definition.
        BusinessObjectFormatKeys resultKeys = businessObjectFormatRestController.getBusinessObjectFormats(NAMESPACE, BDEF_NAME, false);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatKeys(), resultKeys.getBusinessObjectFormatKeys());

        // Retrieve a list of the latest version business object format keys for the specified business object definition.
        resultKeys = businessObjectFormatRestController.getBusinessObjectFormats(NAMESPACE, BDEF_NAME, true);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectFormatLatestVersionKeys(), resultKeys.getBusinessObjectFormatKeys());
    }

    @Test
    public void testGenerateBusinessObjectFormatDdl()
    {
        // Prepare test data.
        createDatabaseEntitiesForBusinessObjectFormatDdlTesting();

        BusinessObjectFormatDdlRequest request;
        BusinessObjectFormatDdl resultDdl;

        // Retrieve business object data ddl.
        request = getTestBusinessObjectFormatDdlRequest(CUSTOM_DDL_NAME);
        resultDdl = businessObjectFormatRestController.generateBusinessObjectFormatDdl(request);

        // Validate the results.
        validateBusinessObjectFormatDdl(CUSTOM_DDL_NAME, getBusinessObjectFormatExpectedDdl(), resultDdl);
    }

    @Test
    public void testGenerateBusinessObjectFormatDdlCollection()
    {
        // Prepare database entities required for testing.
        createDatabaseEntitiesForBusinessObjectFormatDdlCollectionTesting();

        // Generate DDL for a collection of business object data.
        BusinessObjectFormatDdlCollectionResponse resultBusinessObjectFormatDdlCollectionResponse =
            businessObjectFormatRestController.generateBusinessObjectFormatDdlCollection(getTestBusinessObjectFormatDdlCollectionRequest());

        // Validate the response object.
        assertEquals(getExpectedBusinessObjectFormatDdlCollectionResponse(), resultBusinessObjectFormatDdlCollectionResponse);
    }
}
