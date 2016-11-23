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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpert;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertKeys;
import org.finra.herd.model.jpa.BusinessObjectDefinitionSubjectMatterExpertEntity;

/**
 * This class tests various functionality within the business object definition subject matter expert service.
 */
public class BusinessObjectDefinitionSubjectMatterExpertServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreateBusinessObjectDefinitionSubjectMatterExpert()
    {
        // Create a business object definition subject matter expert key.
        BusinessObjectDefinitionSubjectMatterExpertKey key = new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        // Create and persist the relative database entities.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, DESCRIPTION);

        // Create a business object definition subject matter expert.
        BusinessObjectDefinitionSubjectMatterExpert resultBusinessObjectDefinitionSubjectMatterExpert = businessObjectDefinitionSubjectMatterExpertService
            .createBusinessObjectDefinitionSubjectMatterExpert(new BusinessObjectDefinitionSubjectMatterExpertCreateRequest(key));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionSubjectMatterExpert(resultBusinessObjectDefinitionSubjectMatterExpert.getId(), key),
            resultBusinessObjectDefinitionSubjectMatterExpert);
    }

    @Test
    public void testDeleteBusinessObjectDefinitionSubjectMatterExpert()
    {
        // Create a business object definition subject matter expert key.
        BusinessObjectDefinitionSubjectMatterExpertKey key = new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        // Create and persist the relative database entities.
        BusinessObjectDefinitionSubjectMatterExpertEntity businessObjectDefinitionSubjectMatterExpertEntity =
            businessObjectDefinitionSubjectMatterExpertDaoTestHelper.createBusinessObjectDefinitionSubjectMatterExpertEntity(key);

        // Validate that this business object definition subject matter expert exists.
        assertNotNull(businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpertByKey(key));

        // Delete this business object definition subject matter expert.
        BusinessObjectDefinitionSubjectMatterExpert deletedBusinessObjectDefinitionSubjectMatterExpert =
            businessObjectDefinitionSubjectMatterExpertService.deleteBusinessObjectDefinitionSubjectMatterExpert(key);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionSubjectMatterExpert(businessObjectDefinitionSubjectMatterExpertEntity.getId(), key),
            deletedBusinessObjectDefinitionSubjectMatterExpert);

        // Ensure that this business object definition subject matter expert is no longer there.
        assertNull(businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpertByKey(key));
    }

    @Test
    public void testGetBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition() throws Exception
    {
        // Create business object definition subject matter expert keys. The keys are listed out of order to validate the sorting.
        List<BusinessObjectDefinitionSubjectMatterExpertKey> keys = Arrays
            .asList(new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID_2),
                new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID));

        // Create and persist the relative database entities.
        for (BusinessObjectDefinitionSubjectMatterExpertKey key : keys)
        {
            businessObjectDefinitionSubjectMatterExpertDaoTestHelper.createBusinessObjectDefinitionSubjectMatterExpertEntity(key);
        }

        // Get a list of business object definition subject matter expert keys for the specified business object definition.
        BusinessObjectDefinitionSubjectMatterExpertKeys resultBusinessObjectDefinitionSubjectMatterExperts = businessObjectDefinitionSubjectMatterExpertService
            .getBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionSubjectMatterExpertKeys(Arrays.asList(keys.get(1), keys.get(0))),
            resultBusinessObjectDefinitionSubjectMatterExperts);
    }
}
