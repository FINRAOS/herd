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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;

public class BusinessObjectDataStatusDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetBusinessObjectDataStatusByCode()
    {
        // Create database entities required for testing.
        List<BusinessObjectDataStatusEntity> businessObjectDataStatusEntities = Arrays.asList(
            businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS, DESCRIPTION, NO_BDATA_STATUS_PRE_REGISTRATION_FLAG_SET),
            businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS_2, DESCRIPTION_2, BDATA_STATUS_PRE_REGISTRATION_FLAG_SET));

        // Retrieve the relative business object data status entities and validate the results.
        assertEquals(businessObjectDataStatusEntities.get(0), businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(BDATA_STATUS));
        assertEquals(businessObjectDataStatusEntities.get(1), businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(BDATA_STATUS_2));

        // Test case insensitivity for the business object data status code.
        assertEquals(businessObjectDataStatusEntities.get(0), businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(BDATA_STATUS.toUpperCase()));
        assertEquals(businessObjectDataStatusEntities.get(0), businessObjectDataStatusDao.getBusinessObjectDataStatusByCode(BDATA_STATUS.toLowerCase()));

        // Confirm negative results when using non-existing business object data status code.
        assertNull(businessObjectDataStatusDao.getBusinessObjectDataStatusByCode("I_DO_NOT_EXIST"));
    }
}
