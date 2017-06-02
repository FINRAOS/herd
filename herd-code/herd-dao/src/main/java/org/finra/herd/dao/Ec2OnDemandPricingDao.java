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

import java.util.List;

import org.finra.herd.model.jpa.Ec2OnDemandPricingEntity;

public interface Ec2OnDemandPricingDao extends BaseJpaDao
{
    /**
     * Returns the EC2 on-demand pricing for the specified region name and instance type. Returns {@code null} if no EC2 on-demand price is found. Throws an
     * exception when more than one EC2 on-demand price is found.
     *
     * @param regionName the AWS region name
     * @param instanceType the EC2 instance type
     *
     * @return the EC2 on-demand pricing
     */
    public Ec2OnDemandPricingEntity getEc2OnDemandPricing(String regionName, String instanceType);

    /**
     * Gets a list of all EC2 on-demand pricing entities registered in the system. The list of entities returned by this method is sorted by region name and
     * instance type ascending.
     *
     * @return the list of EC2 on-demand pricing entities
     */
    public List<Ec2OnDemandPricingEntity> getEc2OnDemandPricingEntities();
}
