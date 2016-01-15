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

import com.amazonaws.services.ec2.model.SpotPrice;

/**
 * The in-memory spot price information.
 */
public class MockSpotPrice
{
    /**
     * The instance type (ex. m3.large, t1.small, etc).
     */
    private String type;

    /**
     * The AZ which this spot price belongs to.
     */
    private String availabilityZone;

    /**
     * The spot price in USD per hour. (ex. "0.0034" no dollar signs)
     */
    private String spotPrice;

    public String getInstanceType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public String getAvailabilityZone()
    {
        return availabilityZone;
    }

    public void setAvailabilityZone(String availabilityZone)
    {
        this.availabilityZone = availabilityZone;
    }

    public String getSpotPrice()
    {
        return spotPrice;
    }

    public void setSpotPrice(String spotPrice)
    {
        this.spotPrice = spotPrice;
    }

    /**
     * Converts this object into an AWS equivalent object.
     * 
     * @return A new equivalent AWS object
     */
    public SpotPrice toAwsObject()
    {
        SpotPrice spotPrice = new SpotPrice();
        spotPrice.setAvailabilityZone(availabilityZone);
        spotPrice.setInstanceType(type);
        spotPrice.setSpotPrice(this.spotPrice);
        return spotPrice;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((availabilityZone == null) ? 0 : availabilityZone.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
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
        MockSpotPrice other = (MockSpotPrice) obj;
        if (availabilityZone == null)
        {
            if (other.availabilityZone != null)
                return false;
        }
        else if (!availabilityZone.equals(other.availabilityZone))
            return false;
        if (type == null)
        {
            if (other.type != null)
                return false;
        }
        else if (!type.equals(other.type))
            return false;
        return true;
    }
}
