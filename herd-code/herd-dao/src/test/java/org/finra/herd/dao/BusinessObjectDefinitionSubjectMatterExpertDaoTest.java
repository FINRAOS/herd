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

import org.junit.Test;

import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionSubjectMatterExpertEntity;

public class BusinessObjectDefinitionSubjectMatterExpertDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetBusinessObjectDefinitionSubjectMatterExpert()
    {
        // Create a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, DESCRIPTION);

        // Create a business object definition subject matter expert entity.
        BusinessObjectDefinitionSubjectMatterExpertEntity businessObjectDefinitionSubjectMatterExpertEntity =
            businessObjectDefinitionSubjectMatterExpertDaoTestHelper
                .createBusinessObjectDefinitionSubjectMatterExpertEntity(businessObjectDefinitionEntity, USER_ID);

        // Get business object definition subject matter expert.
        assertEquals(businessObjectDefinitionSubjectMatterExpertEntity,
            businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpert(businessObjectDefinitionEntity, USER_ID));

        // Get business object definition subject matter expert by passing all case-insensitive parameters in uppercase.
        assertEquals(businessObjectDefinitionSubjectMatterExpertEntity, businessObjectDefinitionSubjectMatterExpertDao
            .getBusinessObjectDefinitionSubjectMatterExpert(businessObjectDefinitionEntity, USER_ID.toUpperCase()));

        // Get business object definition subject matter expert by passing all case-insensitive parameters in lowercase.
        assertEquals(businessObjectDefinitionSubjectMatterExpertEntity, businessObjectDefinitionSubjectMatterExpertDao
            .getBusinessObjectDefinitionSubjectMatterExpert(businessObjectDefinitionEntity, USER_ID.toLowerCase()));

        // Try invalid values for all input parameters.
        assertNull(businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpert(
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME, DESCRIPTION), USER_ID));
        assertNull(
            businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpert(businessObjectDefinitionEntity, "I_DO_NOT_EXIST"));
    }

    @Test
    public void testGetBusinessObjectDefinitionSubjectMatterExpertByKey()
    {
        // Create a business object definition subject matter expert key.
        BusinessObjectDefinitionSubjectMatterExpertKey key = new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        // Create a business object definition subject matter expert entity.
        BusinessObjectDefinitionSubjectMatterExpertEntity businessObjectDefinitionSubjectMatterExpertEntity =
            businessObjectDefinitionSubjectMatterExpertDaoTestHelper.createBusinessObjectDefinitionSubjectMatterExpertEntity(key);

        // Get business object definition subject matter expert.
        assertEquals(businessObjectDefinitionSubjectMatterExpertEntity,
            businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpertByKey(key));

        // Get business object definition subject matter expert by passing all case-insensitive parameters in uppercase.
        assertEquals(businessObjectDefinitionSubjectMatterExpertEntity, businessObjectDefinitionSubjectMatterExpertDao
            .getBusinessObjectDefinitionSubjectMatterExpertByKey(
                new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), USER_ID.toUpperCase())));

        // Get business object definition subject matter expert by passing all case-insensitive parameters in lowercase.
        assertEquals(businessObjectDefinitionSubjectMatterExpertEntity, businessObjectDefinitionSubjectMatterExpertDao
            .getBusinessObjectDefinitionSubjectMatterExpertByKey(
                new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), USER_ID.toLowerCase())));

        // Try invalid values for all input parameters.
        assertNull(businessObjectDefinitionSubjectMatterExpertDao
            .getBusinessObjectDefinitionSubjectMatterExpertByKey(new BusinessObjectDefinitionSubjectMatterExpertKey("I_DO_NOT_EXIST", BDEF_NAME, USER_ID)));
        assertNull(businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpertByKey(
            new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, "I_DO_NOT_EXIST", USER_ID)));
        assertNull(businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpertByKey(
            new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, "I_DO_NOT_EXIST")));
    }

    @Test
    public void testGetBusinessObjectDefinitionSubjectMatterExpertByKeyDuplicateSubjectMatterExperts() throws Exception
    {
        // Create a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create duplicate business object definition subject matter experts.
        businessObjectDefinitionSubjectMatterExpertDaoTestHelper
            .createBusinessObjectDefinitionSubjectMatterExpertEntity(businessObjectDefinitionEntity, USER_ID.toUpperCase());
        businessObjectDefinitionSubjectMatterExpertDaoTestHelper
            .createBusinessObjectDefinitionSubjectMatterExpertEntity(businessObjectDefinitionEntity, USER_ID.toLowerCase());

        // Try to get business object definition subject matter expert when business object definition has duplicate subject matter experts.
        try
        {
            businessObjectDefinitionSubjectMatterExpertDao
                .getBusinessObjectDefinitionSubjectMatterExpertByKey(new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found more than one business object definition subject matter expert instance with parameters {namespace=\"%s\", " +
                "businessObjectDefinitionName=\"%s\", userId=\"%s\"}.", BDEF_NAMESPACE, BDEF_NAME, USER_ID), e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDefinitionSubjectMatterExpertDuplicateSubjectMatterExperts() throws Exception
    {
        // Create a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Create duplicate business object definition subject matter experts.
        businessObjectDefinitionSubjectMatterExpertDaoTestHelper
            .createBusinessObjectDefinitionSubjectMatterExpertEntity(businessObjectDefinitionEntity, USER_ID.toUpperCase());
        businessObjectDefinitionSubjectMatterExpertDaoTestHelper
            .createBusinessObjectDefinitionSubjectMatterExpertEntity(businessObjectDefinitionEntity, USER_ID.toLowerCase());

        // Try to get business object definition subject matter expert when business object definition has duplicate subject matter experts.
        try
        {
            businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpert(businessObjectDefinitionEntity, USER_ID);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found more than one business object definition subject matter expert instance with parameters {namespace=\"%s\", " +
                "businessObjectDefinitionName=\"%s\", userId=\"%s\"}.", BDEF_NAMESPACE, BDEF_NAME, USER_ID), e.getMessage());
        }
    }
}
