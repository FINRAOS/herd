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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.jpa.DataProviderEntity;

@Component
public class DataProviderDaoTestHelper
{
    @Autowired
    private DataProviderDao dataProviderDao;

    /**
     * Creates and persists a new data provider entity.
     *
     * @param dataProviderName the data provider name
     *
     * @return the newly created data provider entity.
     */
    public DataProviderEntity createDataProviderEntity(String dataProviderName)
    {
        DataProviderEntity dataProviderEntity = new DataProviderEntity();
        dataProviderEntity.setName(dataProviderName);
        return dataProviderDao.saveAndRefresh(dataProviderEntity);
    }

    /**
     * Creates and persists a new data provider entity.
     *
     * @return the newly created data provider entity.
     */
    public DataProviderEntity createDataProviderEntity()
    {
        return createDataProviderEntity("DataProviderTest" + AbstractDaoTest.getRandomSuffix());
    }
}
