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

import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.StoragePolicyKey;

/**
 * A helper class for storage policy operations.
 */
@Component
public class StoragePolicyHelper
{
    /**
     * Validates the storage policy key. This method also trims the key parameters.
     *
     * @param storagePolicyKey the storage policy key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateStoragePolicyKey(StoragePolicyKey storagePolicyKey) throws IllegalArgumentException
    {
        Assert.notNull(storagePolicyKey, "A storage policy key must be specified.");

        Assert.hasText(storagePolicyKey.getNamespace(), "A namespace must be specified.");
        storagePolicyKey.setNamespace(storagePolicyKey.getNamespace().trim());

        Assert.hasText(storagePolicyKey.getStoragePolicyName(), "A storage policy name must be specified.");
        storagePolicyKey.setStoragePolicyName(storagePolicyKey.getStoragePolicyName().trim());
    }

    /**
     * Returns a string representation of the storage policy key.
     *
     * @param storagePolicyKey the storage policy key
     *
     * @return the string representation of the storage policy key
     */
    public String storagePolicyKeyToString(StoragePolicyKey storagePolicyKey)
    {
        return String.format("namespace: \"%s\", storagePolicyName: \"%s\"", storagePolicyKey.getNamespace(), storagePolicyKey.getStoragePolicyName());
    }
}
