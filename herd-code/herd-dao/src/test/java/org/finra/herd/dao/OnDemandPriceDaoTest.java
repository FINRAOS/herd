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

import java.math.BigDecimal;

import org.junit.Test;

import org.finra.herd.model.jpa.OnDemandPriceEntity;

public class OnDemandPriceDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetOnDemandPrice()
    {
        // Create database entities required for testing.
        OnDemandPriceEntity onDemandPriceEntity = createOnDemandPriceEntity(AWS_REGION, EC2_INSTANCE_TYPE);

        // Retrieve and validate this entity.
        assertEquals(onDemandPriceEntity, onDemandPriceDao.getOnDemandPrice(AWS_REGION, EC2_INSTANCE_TYPE));

        // Test case sensitivity for the region and EC2 instance type.
        assertNull(onDemandPriceDao.getOnDemandPrice(AWS_REGION.toUpperCase(), EC2_INSTANCE_TYPE));
        assertNull(onDemandPriceDao.getOnDemandPrice(AWS_REGION.toLowerCase(), EC2_INSTANCE_TYPE));
        assertNull(onDemandPriceDao.getOnDemandPrice(AWS_REGION, EC2_INSTANCE_TYPE.toUpperCase()));
        assertNull(onDemandPriceDao.getOnDemandPrice(AWS_REGION, EC2_INSTANCE_TYPE.toLowerCase()));

        // Confirm negative results when using wrong input parameters.
        assertNull(onDemandPriceDao.getOnDemandPrice("I_DO_NOT_EXIST", EC2_INSTANCE_TYPE));
        assertNull(onDemandPriceDao.getOnDemandPrice(AWS_REGION, "I_DO_NOT_EXIST"));
    }

    /**
     * Creates and persists a new storage unit entity.
     *
     * @param region the AWS region
     * @param instanceType the EC2 instance type
     *
     * @return the newly created storage unit entity.
     */
    private OnDemandPriceEntity createOnDemandPriceEntity(String region, String instanceType)
    {
        OnDemandPriceEntity onDemandPriceEntity = new OnDemandPriceEntity();
        onDemandPriceEntity.setOnDemandPriceId(LONG_VALUE);
        onDemandPriceEntity.setRegion(region);
        onDemandPriceEntity.setInstanceType(instanceType);
        onDemandPriceEntity.setValue(new BigDecimal(INTEGER_VALUE));
        return herdDao.saveAndRefresh(onDemandPriceEntity);
    }
}
