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
package org.finra.dm.model.dto;

/**
 * A DTO that contains the alternate key fields for storage.
 */
public class StorageAlternateKeyDto
{
    /**
     * The storage name.
     */
    private String storageName;

    public String getStorageName()
    {
        return storageName;
    }

    public void setStorageName(String storageName)
    {
        this.storageName = storageName;
    }

    /**
     * Returns a builder that can easily build this DTO.
     *
     * @return the builder.
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * A builder that makes it easier to construct this DTO.
     */
    public static class Builder
    {
        private StorageAlternateKeyDto alternateKey = new StorageAlternateKeyDto();

        public Builder storageName(String storageName)
        {
            alternateKey.setStorageName(storageName);
            return this;
        }

        public StorageAlternateKeyDto build()
        {
            return alternateKey;
        }
    }
}
