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
package org.finra.dm.dao.impl;

import java.util.HashMap;
import java.util.Map;

/**
 * An in-memory EC2 region.
 * Contains a map of AZs.
 */
public class MockEc2Region
{
    /**
     * Name of the region.
     */
    private String name;

    /**
     * Mapping of AZ name to AZs.
     */
    private Map<String, MockAvailabilityZone> availabilityZones = new HashMap<>();

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public Map<String, MockAvailabilityZone> getAvailabilityZones()
    {
        return availabilityZones;
    }

    public void setAvailabilityZones(Map<String, MockAvailabilityZone> availabilityZones)
    {
        this.availabilityZones = availabilityZones;
    }

    /**
     * Puts the AZ into this region. The AZ's region name will be automatically assigned.
     * 
     * @param availabilityZone - The AZ to put.
     */
    public void putAvailabilityZone(MockAvailabilityZone availabilityZone)
    {
        availabilityZone.setRegionName(name);
        this.availabilityZones.put(availabilityZone.getZoneName(), availabilityZone);
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
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
        MockEc2Region other = (MockEc2Region) obj;
        if (name == null)
        {
            if (other.name != null)
                return false;
        }
        else if (!name.equals(other.name))
            return false;
        return true;
    }
}
