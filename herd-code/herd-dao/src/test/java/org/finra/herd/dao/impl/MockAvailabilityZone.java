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
package org.finra.herd.dao.impl;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.ec2.model.AvailabilityZone;

/**
 * In-memory availability zone.
 */
public class MockAvailabilityZone
{
    /**
     * Name of the zone.
     */
    private String zoneName;

    /**
     * Name of the region of this AZ.
     */
    private String regionName;

    /**
     * Map of subnet IDs to subnets.
     */
    private Map<String, MockSubnet> subnets = new HashMap<>();

    /**
     * Map of instance types to spot prices.
     */
    private Map<String, MockSpotPrice> spotPrices = new HashMap<>();

    public String getZoneName()
    {
        return zoneName;
    }

    public void setZoneName(String zoneName)
    {
        this.zoneName = zoneName;
    }

    public String getRegionName()
    {
        return regionName;
    }

    public void setRegionName(String regionName)
    {
        this.regionName = regionName;
    }

    public Map<String, MockSpotPrice> getSpotPrices()
    {
        return spotPrices;
    }

    public void setSpotPrices(Map<String, MockSpotPrice> spotPrices)
    {
        this.spotPrices = spotPrices;
    }

    /**
     * Puts the specified spot price into this AZ. The spot price's zone name is automatically assigned.
     * 
     * @param spotPrice The spot price to put.
     */
    public void putSpotPrice(MockSpotPrice spotPrice)
    {
        spotPrice.setAvailabilityZone(zoneName);
        this.spotPrices.put(spotPrice.getInstanceType(), spotPrice);
    }

    public Map<String, MockSubnet> getSubnets()
    {
        return subnets;
    }

    public void setSubnets(Map<String, MockSubnet> subnets)
    {
        this.subnets = subnets;
    }

    /**
     * Puts a subnet into this AZ. The zone name is automatically assigned.
     * 
     * @param subnet The subnet to put.
     */
    public void putSubnet(MockSubnet subnet)
    {
        subnet.setAvailabilityZone(zoneName);
        this.subnets.put(subnet.getSubnetId(), subnet);
    }

    /**
     * Converts this object into an AWS equivalent object.
     * 
     * @return A new equivalent AWS object
     */
    public AvailabilityZone toAwsObject()
    {
        AvailabilityZone availabilityZone = new AvailabilityZone();
        availabilityZone.setRegionName(regionName);
        availabilityZone.setZoneName(zoneName);
        return availabilityZone;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((zoneName == null) ? 0 : zoneName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MockAvailabilityZone other = (MockAvailabilityZone) obj;
        if (zoneName == null)
        {
            if (other.zoneName != null)
                return false;
        }
        else if (!zoneName.equals(other.zoneName))
            return false;
        return true;
    }
}
