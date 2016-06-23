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
package org.finra.herd.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.DataProviderDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.DataProvider;
import org.finra.herd.model.api.xml.DataProviderCreateRequest;
import org.finra.herd.model.api.xml.DataProviderKey;
import org.finra.herd.model.api.xml.DataProviderKeys;
import org.finra.herd.model.jpa.DataProviderEntity;
import org.finra.herd.service.DataProviderService;
import org.finra.herd.service.helper.DataProviderDaoHelper;

/**
 * The data provider service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class DataProviderServiceImpl implements DataProviderService
{
    @Autowired
    private DataProviderDao dataProviderDao;

    @Autowired
    private DataProviderDaoHelper dataProviderDaoHelper;

    @Override
    public DataProvider createDataProvider(DataProviderCreateRequest request)
    {
        // Perform the validation.
        validateDataProviderCreateRequest(request);

        // Get the data provider key.
        DataProviderKey dataProviderKey = new DataProviderKey(request.getDataProviderName());

        // Ensure a data provider with the specified data provider key doesn't already exist.
        DataProviderEntity dataProviderEntity = dataProviderDao.getDataProviderByKey(dataProviderKey);
        if (dataProviderEntity != null)
        {
            throw new AlreadyExistsException(
                String.format("Unable to create data provider \"%s\" because it already exists.", dataProviderKey.getDataProviderName()));
        }

        // Create a data provider entity from the request information.
        dataProviderEntity = createDataProviderEntity(request);

        // Persist the new entity.
        dataProviderEntity = dataProviderDao.saveAndRefresh(dataProviderEntity);

        // Create and return the data provider object from the persisted entity.
        return createDataProviderFromEntity(dataProviderEntity);
    }

    @Override
    public DataProvider getDataProvider(DataProviderKey dataProviderKey)
    {
        // Perform validation and trim.
        validateDataProviderKey(dataProviderKey);

        // Retrieve and ensure that a data provider already exists with the specified key.
        DataProviderEntity dataProviderEntity = dataProviderDaoHelper.getDataProviderEntity(dataProviderKey.getDataProviderName());

        // Create and return the data provider object from the persisted entity.
        return createDataProviderFromEntity(dataProviderEntity);
    }

    @Override
    public DataProvider deleteDataProvider(DataProviderKey dataProviderKey)
    {
        // Perform validation and trim.
        validateDataProviderKey(dataProviderKey);

        // Retrieve and ensure that a data provider already exists with the specified key.
        DataProviderEntity dataProviderEntity = dataProviderDaoHelper.getDataProviderEntity(dataProviderKey.getDataProviderName());

        // Delete the data provider.
        dataProviderDao.delete(dataProviderEntity);

        // Create and return the data provider object from the deleted entity.
        return createDataProviderFromEntity(dataProviderEntity);
    }

    @Override
    public DataProviderKeys getDataProviders()
    {
        DataProviderKeys dataProviderKeys = new DataProviderKeys();
        dataProviderKeys.getDataProviderKeys().addAll(dataProviderDao.getDataProviders());
        return dataProviderKeys;
    }

    /**
     * Validates the data provider create request. This method also trims request parameters.
     *
     * @param request the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateDataProviderCreateRequest(DataProviderCreateRequest request) throws IllegalArgumentException
    {
        // Validate.
        Assert.hasText(request.getDataProviderName(), "A data provider name must be specified.");

        // Remove leading and trailing spaces.
        request.setDataProviderName(request.getDataProviderName().trim());
    }

    /**
     * Validates a data provider key. This method also trims the key parameters.
     *
     * @param dataProviderKey the data provider key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateDataProviderKey(DataProviderKey dataProviderKey) throws IllegalArgumentException
    {
        // Validate.
        Assert.notNull(dataProviderKey, "A data provider key must be specified.");
        Assert.hasText(dataProviderKey.getDataProviderName(), "A data provider name must be specified.");

        // Remove leading and trailing spaces.
        dataProviderKey.setDataProviderName(dataProviderKey.getDataProviderName().trim());
    }

    /**
     * Creates a new data provider entity from the request information.
     *
     * @param request the request
     *
     * @return the newly created data provider entity
     */
    private DataProviderEntity createDataProviderEntity(DataProviderCreateRequest request)
    {
        // Create a new entity.
        DataProviderEntity dataProviderEntity = new DataProviderEntity();
        dataProviderEntity.setName(request.getDataProviderName());
        return dataProviderEntity;
    }

    /**
     * Creates the data provider from the persisted entity.
     *
     * @param dataProviderEntity the newly persisted data provider entity.
     *
     * @return the data provider.
     */
    private DataProvider createDataProviderFromEntity(DataProviderEntity dataProviderEntity)
    {
        // Create the data provider information.
        DataProvider dataProvider = new DataProvider();
        dataProvider.setDataProviderName(dataProviderEntity.getName());
        return dataProvider;
    }
}
