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

import org.finra.herd.model.jpa.Ec2OnDemandPricingEntity;

public class Ec2OnDemandPricingDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetEc2OnDemandPricing()
    {
        // Create database entities required for testing.
        Ec2OnDemandPricingEntity ec2OnDemandPricingEntity =
            ec2OnDemandPricingDaoTestHelper.createEc2OnDemandPricingEntity(AWS_REGION_NAME, EC2_INSTANCE_TYPE, HOURLY_PRICE);

        // Retrieve and validate this entity.
        assertEquals(ec2OnDemandPricingEntity, ec2OnDemandPricingDao.getEc2OnDemandPricing(AWS_REGION_NAME, EC2_INSTANCE_TYPE));

        // Confirm case sensitivity for the AWS region and EC2 instance type.
        assertNull(ec2OnDemandPricingDao.getEc2OnDemandPricing(AWS_REGION_NAME.toUpperCase(), EC2_INSTANCE_TYPE));
        assertNull(ec2OnDemandPricingDao.getEc2OnDemandPricing(AWS_REGION_NAME.toLowerCase(), EC2_INSTANCE_TYPE));
        assertNull(ec2OnDemandPricingDao.getEc2OnDemandPricing(AWS_REGION_NAME, EC2_INSTANCE_TYPE.toUpperCase()));
        assertNull(ec2OnDemandPricingDao.getEc2OnDemandPricing(AWS_REGION_NAME, EC2_INSTANCE_TYPE.toLowerCase()));

        // Confirm negative results when using wrong input parameters.
        assertNull(ec2OnDemandPricingDao.getEc2OnDemandPricing(I_DO_NOT_EXIST, EC2_INSTANCE_TYPE));
        assertNull(ec2OnDemandPricingDao.getEc2OnDemandPricing(AWS_REGION_NAME, I_DO_NOT_EXIST));
    }

    @Test
    public void testGetEc2OnDemandPricingEntities()
    {
        // Create database entities required for testing in random order.
        List<Ec2OnDemandPricingEntity> ec2OnDemandPricingEntities = Arrays
            .asList(ec2OnDemandPricingDaoTestHelper.createEc2OnDemandPricingEntity(AWS_REGION_NAME_2, EC2_INSTANCE_TYPE_2, HOURLY_PRICE),
                ec2OnDemandPricingDaoTestHelper.createEc2OnDemandPricingEntity(AWS_REGION_NAME, EC2_INSTANCE_TYPE, HOURLY_PRICE),
                ec2OnDemandPricingDaoTestHelper.createEc2OnDemandPricingEntity(AWS_REGION_NAME_2, EC2_INSTANCE_TYPE, HOURLY_PRICE),
                ec2OnDemandPricingDaoTestHelper.createEc2OnDemandPricingEntity(AWS_REGION_NAME, EC2_INSTANCE_TYPE_2, HOURLY_PRICE));

        // Retrieve a list of entities and validate the result.
        assertEquals(Arrays
            .asList(ec2OnDemandPricingEntities.get(1), ec2OnDemandPricingEntities.get(3), ec2OnDemandPricingEntities.get(2), ec2OnDemandPricingEntities.get(0)),
            ec2OnDemandPricingDao.getEc2OnDemandPricingEntities());
    }
}
