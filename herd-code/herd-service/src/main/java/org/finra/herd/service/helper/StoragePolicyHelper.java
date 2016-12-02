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
package org.finra.herd.service.helper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.StoragePolicyKey;
import org.finra.herd.model.jpa.StoragePolicyEntity;

/**
 * A helper class for storage policy operations.
 */
@Component
public class StoragePolicyHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    /**
     * Returns a storage policy key for the storage policy entity.
     *
     * @param storagePolicyEntity the storage policy entity
     *
     * @return the storage policy key for the storage policy entity
     */
    public StoragePolicyKey getStoragePolicyKey(StoragePolicyEntity storagePolicyEntity)
    {
        return new StoragePolicyKey(storagePolicyEntity.getNamespace().getCode(), storagePolicyEntity.getName());
    }

    /**
     * Returns a string representation of the storage policy key along with the storage policy version.
     *
     * @param storagePolicyKey the storage policy key
     * @param storagePolicyVersion the storage policy version
     *
     * @return the string representation of the storage policy key and version
     */
    public String storagePolicyKeyAndVersionToString(StoragePolicyKey storagePolicyKey, Integer storagePolicyVersion)
    {
        return String.format("namespace: \"%s\", storagePolicyName: \"%s\", storagePolicyVersion: \"%d\"", storagePolicyKey.getNamespace(),
            storagePolicyKey.getStoragePolicyName(), storagePolicyVersion);
    }

    /**
     * Validates the storage policy key. This method also trims the key parameters.
     *
     * @param key the storage policy key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateStoragePolicyKey(StoragePolicyKey key) throws IllegalArgumentException
    {
        Assert.notNull(key, "A storage policy key must be specified.");
        key.setNamespace(alternateKeyHelper.validateStringParameter("namespace", key.getNamespace()));
        key.setStoragePolicyName(alternateKeyHelper.validateStringParameter("storage policy name", key.getStoragePolicyName()));
    }
}
