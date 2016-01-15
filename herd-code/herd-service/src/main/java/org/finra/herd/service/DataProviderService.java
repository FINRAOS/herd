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
package org.finra.herd.service;

import org.finra.herd.model.api.xml.DataProvider;
import org.finra.herd.model.api.xml.DataProviderCreateRequest;
import org.finra.herd.model.api.xml.DataProviderKey;
import org.finra.herd.model.api.xml.DataProviderKeys;

/**
 * The data provider service.
 */
public interface DataProviderService
{
    /**
     * Creates a new data provider.
     *
     * @param dataProviderCreateRequest the data provider create request
     *
     * @return the created data provider
     */
    public DataProvider createDataProvider(DataProviderCreateRequest dataProviderCreateRequest);

    /**
     * Gets a dataProvider for the specified key.
     *
     * @param dataProviderKey the data provider key
     *
     * @return the data provider
     */
    public DataProvider getDataProvider(DataProviderKey dataProviderKey);

    /**
     * Deletes a data provider for the specified key.
     *
     * @param dataProviderKey the data provider key
     *
     * @return the data provider that was deleted
     */
    public DataProvider deleteDataProvider(DataProviderKey dataProviderKey);

    /**
     * Gets a list of data provider keys for all data providers defined in the system.
     *
     * @return the data provider keys
     */
    public DataProviderKeys getDataProviders();
}
