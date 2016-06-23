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
package org.finra.herd.dao.helper;

import java.math.BigDecimal;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Component;

import org.finra.herd.model.dto.EmrVpcPricingState;

/**
 * Formatter for a EMR VPC pricing state.
 */
@Component
public class EmrVpcPricingStateFormatter
{
    /**
     * Formats a EMR VPC pricing state into a human readable string.
     * 
     * @param emrVpcPricingState EMR VPC pricing state
     * @return Formatted string
     */
    public String format(EmrVpcPricingState emrVpcPricingState)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("Subnet IP Availability:%n"));
        for (Map.Entry<String, Integer> subnet : emrVpcPricingState.getSubnetAvailableIpAddressCounts().entrySet())
        {
            builder.append(String.format("\t%s=%s%n", subnet.getKey(), subnet.getValue()));
        }
        Map<String, Map<String, BigDecimal>> spotPricesPerAvailabilityZone = emrVpcPricingState.getSpotPricesPerAvailabilityZone();
        if (MapUtils.isNotEmpty(spotPricesPerAvailabilityZone))
        {
            builder.append(String.format("Spot Prices:%n"));
            for (Map.Entry<String, Map<String, BigDecimal>> availabilityZone : spotPricesPerAvailabilityZone.entrySet())
            {
                builder.append(String.format("\t%s%n", availabilityZone.getKey()));
                for (Map.Entry<String, BigDecimal> instanceType : availabilityZone.getValue().entrySet())
                {
                    builder.append(String.format("\t\t%s=$%s%n", instanceType.getKey(), instanceType.getValue()));
                }
            }
        }
        Map<String, Map<String, BigDecimal>> onDemandPricesPerAvailabilityZone = emrVpcPricingState.getOnDemandPricesPerAvailabilityZone();
        if (MapUtils.isNotEmpty(onDemandPricesPerAvailabilityZone))
        {
            builder.append(String.format("On-Demand Prices:%n"));
            for (Map.Entry<String, Map<String, BigDecimal>> availabilityZone : onDemandPricesPerAvailabilityZone.entrySet())
            {
                builder.append(String.format("\t%s%n", availabilityZone.getKey()));
                for (Map.Entry<String, BigDecimal> instanceType : availabilityZone.getValue().entrySet())
                {
                    builder.append(String.format("\t\t%s=$%s%n", instanceType.getKey(), instanceType.getValue()));
                }
            }
        }
        return builder.toString();
    }
}
