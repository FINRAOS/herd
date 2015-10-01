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
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.dm.model.jpa.CustomDdlEntity;
import org.finra.dm.model.api.xml.CustomDdl;
import org.finra.dm.model.api.xml.CustomDdlKey;
import org.finra.dm.model.api.xml.CustomDdlKeys;

/**
 * This class tests various functionality within the custom DDL REST controller.
 */
public class CustomDdlRestControllerTest extends AbstractRestTest
{
    @Test
    public void testCreateCustomDdl()
    {
        // Create and persist a business object format entity.
        createBusinessObjectFormatEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, FORMAT_DESCRIPTION, true,
            PARTITION_KEY);

        // Create a custom DDL.
        CustomDdl resultCustomDdl = customDdlRestController.createCustomDdl(
            createCustomDdlCreateRequest(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL));

        // Validate the returned object.
        validateCustomDdl(null, NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL, resultCustomDdl);
    }

    @Test
    public void testGetCustomDdl()
    {
        // Create and persist a custom DDL entity.
        CustomDdlEntity customDdlEntity =
            createCustomDdlEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL);

        // Retrieve the custom DDL.
        CustomDdl resultCustomDdl =
            customDdlRestController.getCustomDdl(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME);

        // Validate the returned object.
        validateCustomDdl(customDdlEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL,
            resultCustomDdl);
    }

    @Test
    public void testGetCustomDdlLegacy()
    {
        // Create and persist a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create and persist a custom DDL entity.
        CustomDdlEntity customDdlEntity =
            createCustomDdlEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL);

        // Retrieve the custom DDL by calling a legacy endpoint.
        CustomDdl resultCustomDdl = customDdlRestController.getCustomDdl(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME);

        // Validate the returned object.
        validateCustomDdl(customDdlEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL,
            resultCustomDdl);
    }

    @Test
    public void testGetCustomDdls()
    {
        // List of test custom DDL names.
        List<String> testCustomDdlNames = Arrays.asList(CUSTOM_DDL_NAME, CUSTOM_DDL_NAME_2);

        // Create and persist a custom DDL entities.
        for (String customDdlName : testCustomDdlNames)
        {
            createCustomDdlEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, customDdlName, TEST_DDL);
        }

        // Retrieve a list of custom DDL keys.
        CustomDdlKeys resultCustomDdlKeys =
            customDdlRestController.getCustomDdls(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Validate the returned object.
        assertNotNull(resultCustomDdlKeys);
        assertEquals(testCustomDdlNames.size(), resultCustomDdlKeys.getCustomDdlKeys().size());
        for (int i = 0; i < testCustomDdlNames.size(); i++)
        {
            validateCustomDdlKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testCustomDdlNames.get(i),
                resultCustomDdlKeys.getCustomDdlKeys().get(i));
        }
    }

    @Test
    public void testGetCustomDdlsLegacy()
    {
        // Create and persist a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // List of test custom DDL names.
        List<String> testCustomDdlNames = Arrays.asList(CUSTOM_DDL_NAME, CUSTOM_DDL_NAME_2);

        // Create and persist a custom DDL entities.
        for (String customDdlName : testCustomDdlNames)
        {
            createCustomDdlEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, customDdlName, TEST_DDL);
        }

        // Retrieve a list of custom DDL keys by calling a legacy endpoint.
        CustomDdlKeys resultCustomDdlKeys = customDdlRestController.getCustomDdls(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION);

        // Validate the returned object.
        assertNotNull(resultCustomDdlKeys);
        assertEquals(testCustomDdlNames.size(), resultCustomDdlKeys.getCustomDdlKeys().size());
        for (int i = 0; i < testCustomDdlNames.size(); i++)
        {
            validateCustomDdlKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, testCustomDdlNames.get(i),
                resultCustomDdlKeys.getCustomDdlKeys().get(i));
        }
    }

    @Test
    public void testUpdateCustomDdl()
    {
        // Create and persist a custom DDL entity.
        CustomDdlEntity customDdlEntity =
            createCustomDdlEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL);

        // Update the custom DDL.
        CustomDdl updatedCustomDdl = customDdlRestController
            .updateCustomDdl(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME,
                createCustomDdlUpdateRequest(TEST_DDL_2));

        // Validate the returned object.
        validateCustomDdl(customDdlEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME,
            TEST_DDL_2, updatedCustomDdl);
    }

    @Test
    public void testUpdateCustomDdlLegacy()
    {
        // Create and persist a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create and persist a custom DDL entity.
        CustomDdlEntity customDdlEntity =
            createCustomDdlEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL);

        // Update the custom DDL by calling a legacy endpoint.
        CustomDdl updatedCustomDdl = customDdlRestController
            .updateCustomDdl(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, createCustomDdlUpdateRequest(TEST_DDL_2));

        // Validate the returned object.
        validateCustomDdl(customDdlEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME,
            TEST_DDL_2, updatedCustomDdl);
    }

    @Test
    public void testDeleteCustomDdl()
    {
        // Create and persist a custom DDL entity.
        CustomDdlEntity customDdlEntity =
            createCustomDdlEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL);

        // Validate that this custom DDL exists.
        CustomDdlKey customDdlKey = new CustomDdlKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME);
        assertNotNull(dmDao.getCustomDdlByKey(customDdlKey));

        // Delete this custom DDL.
        CustomDdl deletedCustomDdl =
            customDdlRestController.deleteCustomDdl(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME);

        // Validate the returned object.
        validateCustomDdl(customDdlEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL,
            deletedCustomDdl);

        // Ensure that this custom DDL is no longer there.
        assertNull(dmDao.getCustomDdlByKey(customDdlKey));
    }

    @Test
    public void testDeleteCustomDdlLegacy()
    {
        // Create and persist a legacy business object definition.
        createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, true);

        // Create and persist a custom DDL entity.
        CustomDdlEntity customDdlEntity =
            createCustomDdlEntity(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL);

        // Validate that this custom DDL exists.
        CustomDdlKey customDdlKey = new CustomDdlKey(NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME);
        assertNotNull(dmDao.getCustomDdlByKey(customDdlKey));

        // Delete this custom DDL by calling a legacy endpoint.
        CustomDdl deletedCustomDdl =
            customDdlRestController.deleteCustomDdl(BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME);

        // Validate the returned object.
        validateCustomDdl(customDdlEntity.getId(), NAMESPACE_CD, BOD_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, CUSTOM_DDL_NAME, TEST_DDL,
            deletedCustomDdl);

        // Ensure that this custom DDL is no longer there.
        assertNull(dmDao.getCustomDdlByKey(customDdlKey));
    }
}
