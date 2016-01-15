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

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StoragePolicyKey;

/**
 * A class to hold various fields required to identify a business object data match with a storage policy.
 */
public class StoragePolicySelection
{
    /**
     * Default no-arg constructor.
     */
    public StoragePolicySelection()
    {
        super();
    }

    /**
     * Fully-initialising value constructor.
     */
    public StoragePolicySelection(final BusinessObjectDataKey businessObjectDataKey, final StoragePolicyKey storagePolicyKey)
    {
        this.businessObjectDataKey = businessObjectDataKey;
        this.storagePolicyKey = storagePolicyKey;
    }

    /**
     * The business object data key.
     */
    private BusinessObjectDataKey businessObjectDataKey;

    /**
     * The storage policy Key.
     */
    private StoragePolicyKey storagePolicyKey;

    public BusinessObjectDataKey getBusinessObjectDataKey()
    {
        return businessObjectDataKey;
    }

    public void setBusinessObjectDataKey(BusinessObjectDataKey businessObjectDataKey)
    {
        this.businessObjectDataKey = businessObjectDataKey;
    }

    public StoragePolicyKey getStoragePolicyKey()
    {
        return storagePolicyKey;
    }

    public void setStoragePolicyKey(StoragePolicyKey storagePolicyKey)
    {
        this.storagePolicyKey = storagePolicyKey;
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (!(object instanceof StoragePolicySelection))
        {
            return false;
        }

        StoragePolicySelection that = (StoragePolicySelection) object;

        if (businessObjectDataKey != null ? !businessObjectDataKey.equals(that.businessObjectDataKey) : that.businessObjectDataKey != null)
        {
            return false;
        }
        if (storagePolicyKey != null ? !storagePolicyKey.equals(that.storagePolicyKey) : that.storagePolicyKey != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = businessObjectDataKey != null ? businessObjectDataKey.hashCode() : 0;
        result = 31 * result + (storagePolicyKey != null ? storagePolicyKey.hashCode() : 0);
        return result;
    }
}