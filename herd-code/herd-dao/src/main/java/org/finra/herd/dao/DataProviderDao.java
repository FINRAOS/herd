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
package org.finra.herd.dao;

import java.util.List;

import org.finra.herd.model.api.xml.DataProviderKey;
import org.finra.herd.model.jpa.DataProviderEntity;

public interface DataProviderDao extends BaseJpaDao
{
    /**
     * Gets a data provider by it's key.
     *
     * @param dataProviderKey the data provider key (case-insensitive)
     *
     * @return the data provider for the specified key
     */
    public DataProviderEntity getDataProviderByKey(DataProviderKey dataProviderKey);

    /**
     * Gets a data provider by it's name.
     *
     * @param dataProviderName the data provider name (case-insensitive)
     *
     * @return the data provider for the specified name
     */
    public DataProviderEntity getDataProviderByName(String dataProviderName);

    /**
     * Gets a list of data provider keys for all data providers defined in the system.
     *
     * @return the list of data provider keys
     */
    public List<DataProviderKey> getDataProviders();
}
