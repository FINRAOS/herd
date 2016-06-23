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
package org.finra.herd.model.dto;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Describes the current state of the VPC in regards to the EMR spot price search.
 */
public class EmrVpcPricingState
{
    private Map<String, Integer> subnetAvailableIpAddressCounts = new HashMap<>();

    private Map<String, Map<String, BigDecimal>> spotPricesPerAvailabilityZone = new HashMap<>();

    private Map<String, Map<String, BigDecimal>> onDemandPricesPerAvailabilityZone = new HashMap<>();

    public Map<String, Integer> getSubnetAvailableIpAddressCounts()
    {
        return subnetAvailableIpAddressCounts;
    }

    public void setSubnetAvailableIpAddressCounts(Map<String, Integer> subnets)
    {
        this.subnetAvailableIpAddressCounts = subnets;
    }

    public Map<String, Map<String, BigDecimal>> getSpotPricesPerAvailabilityZone()
    {
        return spotPricesPerAvailabilityZone;
    }

    public void setSpotPricesPerAvailabilityZone(Map<String, Map<String, BigDecimal>> spotPricesPerAvailabilityZone)
    {
        this.spotPricesPerAvailabilityZone = spotPricesPerAvailabilityZone;
    }

    public Map<String, Map<String, BigDecimal>> getOnDemandPricesPerAvailabilityZone()
    {
        return onDemandPricesPerAvailabilityZone;
    }

    public void setOnDemandPricesPerAvailabilityZone(Map<String, Map<String, BigDecimal>> onDemandPricesPerAvailabilityZone)
    {
        this.onDemandPricesPerAvailabilityZone = onDemandPricesPerAvailabilityZone;
    }
}
