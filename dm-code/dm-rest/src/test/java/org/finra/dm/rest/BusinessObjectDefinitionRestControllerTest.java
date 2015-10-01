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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

import org.finra.dm.dao.helper.DmDaoSecurityHelper;
import org.finra.dm.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.dm.model.api.xml.Attribute;
import org.finra.dm.model.api.xml.BusinessObjectDefinition;
import org.finra.dm.model.api.xml.BusinessObjectDefinitionCreateRequest;
import org.finra.dm.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.dm.model.api.xml.BusinessObjectDefinitionKeys;

/**
 * This class tests various functionality within the business object definition REST controller.
 */
public class BusinessObjectDefinitionRestControllerTest extends AbstractRestTest
{
    @Test
    public void testCreateBusinessObjectDefinition() throws Exception
    {
        // Create and persist database entities required for testing.
        createDatabaseEntitiesForBusinessObjectDefinitionTesting();

        // Create a business object definition.
        BusinessObjectDefinitionCreateRequest request =
            createBusinessObjectDefinitionCreateRequest(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, getNewAttributes());
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionRestController.createBusinessObjectDefinition(request);

        // Validate the returned object.
        validateBusinessObjectDefinition(null, NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, getNewAttributes(), resultBusinessObjectDefinition);

        // Retrieve the newly created business object definition and validate the created by field.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            dmDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(NAMESPACE_CD, BOD_NAME));

        // Validate that the newly created entity uses system username for the relative auditable fields.
        assertEquals(Integer.valueOf(resultBusinessObjectDefinition.getId()), businessObjectDefinitionEntity.getId());
        assertEquals(DmDaoSecurityHelper.SYSTEM_USER, businessObjectDefinitionEntity.getCreatedBy());
        assertEquals(DmDaoSecurityHelper.SYSTEM_USER, businessObjectDefinitionEntity.getUpdatedBy());
    }

    @Test
    public void testUpdateBusinessObjectDefinition() throws Exception
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, getNewAttributes(), null);

        // Perform an update by changing the description and updating the attributes.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionRestController
            .updateBusinessObjectDefinition(NAMESPACE_CD, BOD_NAME, createBusinessObjectDefinitionUpdateRequest(BOD_DESCRIPTION_2, getNewAttributes2()));

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION_2,
            getNewAttributes2(), updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionLegacy() throws Exception
    {
        // Create and persist a legacy business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, getNewAttributes(), true);

        // Perform an update by calling a legacy endpoint.
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionRestController
            .updateBusinessObjectDefinition(BOD_NAME, createBusinessObjectDefinitionUpdateRequest(BOD_DESCRIPTION_2, getNewAttributes2()));

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION_2,
            getNewAttributes2(), updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionMissingOptionalParametersPassedAsWhitespace()
    {
        // Create and persist a legacy business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, getNewAttributes(), true);

        // Perform an update without specifying any of the optional parameters (passing whitespace characters).
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionRestController.updateBusinessObjectDefinition(BLANK_TEXT, BOD_NAME,
            createBusinessObjectDefinitionUpdateRequest(BLANK_TEXT, Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, BLANK_TEXT))));

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BLANK_TEXT,
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, BLANK_TEXT)), updatedBusinessObjectDefinition);
    }

    @Test
    public void testUpdateBusinessObjectDefinitionMissingOptionalParametersPassedAsNulls()
    {
        // Create and persist a legacy business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, getNewAttributes(), true);

        // Perform an update without specifying any of the optional parameters (passing null values).
        BusinessObjectDefinition updatedBusinessObjectDefinition = businessObjectDefinitionRestController.updateBusinessObjectDefinition(null, BOD_NAME,
            createBusinessObjectDefinitionUpdateRequest(null, Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, null))));

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, null,
            Arrays.asList(new Attribute(ATTRIBUTE_NAME_1_MIXED_CASE, null)), updatedBusinessObjectDefinition);
    }

    @Test
    public void testGetBusinessObjectDefinition() throws Exception
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, getNewAttributes(), null);

        // Retrieve the business object definition.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionRestController.getBusinessObjectDefinition(NAMESPACE_CD, BOD_NAME);

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION,
            getNewAttributes(), resultBusinessObjectDefinition);
    }

    @Test
    public void testGetBusinessObjectDefinitionLegacy() throws Exception
    {
        // Create and persist a legacy business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, getNewAttributes(), true);

        // Perform an update by calling a legacy endpoint.
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionRestController.getBusinessObjectDefinition(BOD_NAME);

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION,
            getNewAttributes(), resultBusinessObjectDefinition);
    }

    @Test
    public void testGetBusinessObjectDefinitionMissingOptionalParametersPassedAsWhitespace()
    {
        // Create and persist a legacy business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, getNewAttributes(), true);

        // Retrieve the business object definition without specifying any of the optional parameters (passing whitespace characters).
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionRestController.getBusinessObjectDefinition(BLANK_TEXT, BOD_NAME);

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION,
            getNewAttributes(), resultBusinessObjectDefinition);
    }

    @Test
    public void testGetBusinessObjectDefinitionMissingOptionalParametersPassedAsNulls()
    {
        // Create and persist a legacy business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, getNewAttributes(), true);

        // Retrieve the business object definition without specifying any of the optional parameters (passing null values).
        BusinessObjectDefinition resultBusinessObjectDefinition = businessObjectDefinitionRestController.getBusinessObjectDefinition(null, BOD_NAME);

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION,
            getNewAttributes(), resultBusinessObjectDefinition);
    }

    @Test
    public void testGetBusinessObjectDefinitions() throws Exception
    {
        // Create and persist business object definition entities.
        for (BusinessObjectDefinitionKey key : getTestBusinessObjectDefinitionKeys())
        {
            createBusinessObjectDefinitionEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        }

        // Retrieve a list of business object definition keys for the specified namespace.
        BusinessObjectDefinitionKeys resultKeys = businessObjectDefinitionRestController.getBusinessObjectDefinitions(NAMESPACE_CD);

        // Validate the returned object.
        assertEquals(getExpectedBusinessObjectDefinitionKeys(), resultKeys.getBusinessObjectDefinitionKeys());
    }

    @Test
    public void testGetBusinessObjectDefinitionsLegacy() throws Exception
    {
        // Create and persist business object definition entities.
        for (BusinessObjectDefinitionKey key : getTestBusinessObjectDefinitionKeys())
        {
            createBusinessObjectDefinitionEntity(key.getNamespace(), key.getBusinessObjectDefinitionName(), DATA_PROVIDER_NAME, BOD_DESCRIPTION, null);
        }

        // Retrieve a list of business object definition keys stored in the system by calling a legacy endpoint.
        BusinessObjectDefinitionKeys resultKeys = businessObjectDefinitionRestController.getBusinessObjectDefinitions();

        // Validate the returned object.
        assertNotNull(resultKeys);
        assertTrue(resultKeys.getBusinessObjectDefinitionKeys().containsAll(getTestBusinessObjectDefinitionKeys()));
    }

    @Test
    public void testDeleteBusinessObjectDefinition() throws Exception
    {
        // Create and persist a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, getNewAttributes(), null);

        // Validate that this business object definition exists.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE_CD, BOD_NAME);
        assertNotNull(dmDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey));

        // Delete this business object definition.
        BusinessObjectDefinition deletedBusinessObjectDefinition =
            businessObjectDefinitionRestController.deleteBusinessObjectDefinition(NAMESPACE_CD, BOD_NAME);

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION,
            getNewAttributes(), deletedBusinessObjectDefinition);

        // Ensure that this business object definition is no longer there.
        assertNull(dmDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionLegacy() throws Exception
    {
        // Create and persist a legacy business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, getNewAttributes(), true);

        // Perform an update by calling a legacy endpoint.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE_CD, BOD_NAME);
        assertNotNull(dmDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey));

        // Delete this business object definition by calling a legacy endpoint.
        BusinessObjectDefinition deletedBusinessObjectDefinition = businessObjectDefinitionRestController.deleteBusinessObjectDefinition(BOD_NAME);

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION,
            getNewAttributes(), deletedBusinessObjectDefinition);

        // Ensure that this business object definition is no longer there.
        assertNull(dmDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionMissingOptionalParametersPassedAsWhitespace()
    {
        // Create and persist a legacy business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, getNewAttributes(), true);

        // Validate that this business object definition exists.
        assertNotNull(dmDao.getLegacyBusinessObjectDefinitionByName(BOD_NAME));

        // Delete this business object definition without specifying any of the optional parameters (passing whitespace characters).
        BusinessObjectDefinition deletedBusinessObjectDefinition = businessObjectDefinitionRestController.deleteBusinessObjectDefinition(BLANK_TEXT, BOD_NAME);

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION,
            getNewAttributes(), deletedBusinessObjectDefinition);

        // Ensure that this business object definition is no longer there.
        assertNull(dmDao.getLegacyBusinessObjectDefinitionByName(BOD_NAME));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionMissingOptionalParametersPassedAsNull()
    {
        // Create and persist a legacy business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            createBusinessObjectDefinitionEntity(NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION, getNewAttributes(), true);

        // Validate that this business object definition exists.
        assertNotNull(dmDao.getLegacyBusinessObjectDefinitionByName(BOD_NAME));

        // Delete this business object definition without specifying any of the optional parameters (passing null values).
        BusinessObjectDefinition deletedBusinessObjectDefinition = businessObjectDefinitionRestController.deleteBusinessObjectDefinition(null, BOD_NAME);

        // Validate the returned object.
        validateBusinessObjectDefinition(businessObjectDefinitionEntity.getId(), NAMESPACE_CD, BOD_NAME, DATA_PROVIDER_NAME, BOD_DESCRIPTION,
            getNewAttributes(), deletedBusinessObjectDefinition);

        // Ensure that this business object definition is no longer there.
        assertNull(dmDao.getLegacyBusinessObjectDefinitionByName(BOD_NAME));
    }
}
