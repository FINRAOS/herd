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

import java.math.BigDecimal;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.jpa.Ec2OnDemandPricingEntity;

@Component
public class Ec2OnDemandPricingDaoTestHelper
{
    @Autowired
    private Ec2OnDemandPricingDao ec2OnDemandPricingDao;

    /**
     * Creates and persists EC2 on-demand pricing entities required for testing.
     */
    public void createEc2OnDemandPricingEntities()
    {
        createEc2OnDemandPricingEntity("REGION_1", "INSTANCE_TYPE_1", new BigDecimal(1.00));
        createEc2OnDemandPricingEntity("REGION_1", "INSTANCE_TYPE_2", new BigDecimal(1.00));
        createEc2OnDemandPricingEntity("REGION_1", "INSTANCE_TYPE_3", new BigDecimal(1.00));
        createEc2OnDemandPricingEntity("REGION_2", "INSTANCE_TYPE_1", new BigDecimal(0.50));
    }

    /**
     * Creates and persists a new EC2 on-demand pricing entity.
     *
     * @param regionName the AWS region name
     * @param instanceType the EC2 instance type
     * @param hourlyPrice the hourly price
     *
     * @return the newly created entity
     */
    public Ec2OnDemandPricingEntity createEc2OnDemandPricingEntity(String regionName, String instanceType, BigDecimal hourlyPrice)
    {
        Ec2OnDemandPricingEntity ec2OnDemandPricingEntity = new Ec2OnDemandPricingEntity();
        ec2OnDemandPricingEntity.setRegionName(regionName);
        ec2OnDemandPricingEntity.setInstanceType(instanceType);
        ec2OnDemandPricingEntity.setHourlyPrice(hourlyPrice);
        return ec2OnDemandPricingDao.saveAndRefresh(ec2OnDemandPricingEntity);
    }
}
