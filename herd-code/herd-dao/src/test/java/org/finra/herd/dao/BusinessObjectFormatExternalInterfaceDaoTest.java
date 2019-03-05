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
package org.finra.herd.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatExternalInterfaceEntity;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;

public class BusinessObjectFormatExternalInterfaceDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetBusinessObjectFormatExternalInterfaceByBusinessObjectFormatAndExternalInterface()
    {
        // Create business object format entities.
        List<BusinessObjectFormatEntity> businessObjectFormatEntities = Arrays.asList(businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY), businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, NO_FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY));

        // Create external interface entities.
        List<ExternalInterfaceEntity> externalInterfaceEntities = Arrays
            .asList(externalInterfaceDaoTestHelper.createExternalInterfaceEntity(EXTERNAL_INTERFACE),
                externalInterfaceDaoTestHelper.createExternalInterfaceEntity(EXTERNAL_INTERFACE_2));

        // Create a business object format to external interface mapping entity.
        BusinessObjectFormatExternalInterfaceEntity businessObjectFormatExternalInterfaceEntity = businessObjectFormatExternalInterfaceDaoTestHelper
            .createBusinessObjectFormatExternalInterfaceEntity(businessObjectFormatEntities.get(0), externalInterfaceEntities.get(0));

        // Get the business object format to external interface mapping entity.
        assertEquals(businessObjectFormatExternalInterfaceEntity, businessObjectFormatExternalInterfaceDao
            .getBusinessObjectFormatExternalInterfaceByBusinessObjectFormatAndExternalInterface(businessObjectFormatEntities.get(0),
                externalInterfaceEntities.get(0)));

        // Confirm negative results when using invalid key parameters.
        assertNull(businessObjectFormatExternalInterfaceDao
            .getBusinessObjectFormatExternalInterfaceByBusinessObjectFormatAndExternalInterface(businessObjectFormatEntities.get(1),
                externalInterfaceEntities.get(0)));
        assertNull(businessObjectFormatExternalInterfaceDao
            .getBusinessObjectFormatExternalInterfaceByBusinessObjectFormatAndExternalInterface(businessObjectFormatEntities.get(0),
                externalInterfaceEntities.get(1)));
    }

    @Test
    public void testGetBusinessObjectFormatExternalInterfaceByBusinessObjectFormatAndExternalInterfaceDuplicateEntities()
    {
        // Create a business object format entity.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_FORMAT_DESCRIPTION,
                NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL, NO_LATEST_VERSION_FLAG_SET, PARTITION_KEY);

        // Create external interface entities.
        ExternalInterfaceEntity externalInterfaceEntity = externalInterfaceDaoTestHelper.createExternalInterfaceEntity(EXTERNAL_INTERFACE);

        // Create duplicate business object format to external interface mapping entities.
        businessObjectFormatExternalInterfaceDaoTestHelper
            .createBusinessObjectFormatExternalInterfaceEntity(businessObjectFormatEntity, externalInterfaceEntity);
        businessObjectFormatExternalInterfaceDaoTestHelper
            .createBusinessObjectFormatExternalInterfaceEntity(businessObjectFormatEntity, externalInterfaceEntity);

        // Try to get business object format to external interface mapping entity.
        try
        {
            businessObjectFormatExternalInterfaceDao
                .getBusinessObjectFormatExternalInterfaceByBusinessObjectFormatAndExternalInterface(businessObjectFormatEntity, externalInterfaceEntity);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found more than one business object format to external interface mapping with parameters {namespace=\"%s\", " +
                "businessObjectDefinitionName=\"%s\", businessObjectFormatUsage=\"%s\", businessObjectFormatFileType=\"%s\", " +
                "externalInterfaceName=\"%s\"}.", NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, EXTERNAL_INTERFACE), e.getMessage());
        }
    }
}
