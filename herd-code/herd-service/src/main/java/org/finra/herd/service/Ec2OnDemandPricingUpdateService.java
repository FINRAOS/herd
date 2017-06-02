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

import java.util.List;

import org.finra.herd.model.dto.Ec2OnDemandPricing;

/**
 * The service that updates EC2 on-demand pricing.
 */
public interface Ec2OnDemandPricingUpdateService
{
    /**
     * Retrieves EC2 on-demand pricing information from AWS.
     *
     * @param ec2PricingListUrl the URL of the Amazon EC2 pricing list in JSON format
     *
     * @return the list of EC2 on-demand pricing entries
     */
    public List<Ec2OnDemandPricing> getEc2OnDemandPricing(String ec2PricingListUrl);

    /**
     * Updates the EC2 on-demand pricing configured in the system per specified pricing information.
     *
     * @param ec2OnDemandPricingEntries the list of EC2 on-demand pricing entries
     */
    public void updateEc2OnDemandPricing(List<Ec2OnDemandPricing> ec2OnDemandPricingEntries);
}
