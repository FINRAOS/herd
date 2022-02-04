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
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailability;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDdl;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataPartitions;
import org.finra.herd.model.api.xml.BusinessObjectDataPartitionsRequest;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;

/**
 * This is a Business Object Data service implementation for testing.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
@Primary
public class TestBusinessObjectDataServiceImpl extends BusinessObjectDataServiceImpl
{
    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public BusinessObjectDataAvailability checkBusinessObjectDataAvailability(BusinessObjectDataAvailabilityRequest request)
    {
        return checkBusinessObjectDataAvailabilityImpl(request);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public BusinessObjectDataAvailabilityCollectionResponse checkBusinessObjectDataAvailabilityCollection(
        BusinessObjectDataAvailabilityCollectionRequest request)
    {
        return checkBusinessObjectDataAvailabilityCollectionImpl(request);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @PublishNotificationMessages
    @Override
    public BusinessObjectData createBusinessObjectData(BusinessObjectDataCreateRequest request)
    {
        return businessObjectDataDaoHelper.createBusinessObjectData(request);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public BusinessObjectData destroyBusinessObjectData(BusinessObjectDataKey businessObjectDataKey, Boolean batchMode)
    {
        return destroyBusinessObjectDataImpl(businessObjectDataKey, false);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public BusinessObjectDataDdl generateBusinessObjectDataDdl(BusinessObjectDataDdlRequest request)
    {
        return generateBusinessObjectDataDdlImpl(request, false);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public BusinessObjectDataPartitions generateBusinessObjectDataPartitions(BusinessObjectDataPartitionsRequest request)
    {
        return generateBusinessObjectDataPartitionsImpl(request);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public BusinessObjectDataDdlCollectionResponse generateBusinessObjectDataDdlCollection(BusinessObjectDataDdlCollectionRequest request)
    {
        return generateBusinessObjectDataDdlCollectionImpl(request);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public BusinessObjectData getBusinessObjectData(BusinessObjectDataKey businessObjectDataKey, String businessObjectFormatPartitionKey,
        String businessObjectDataStatus, Boolean includeBusinessObjectDataStatusHistory, Boolean includeStorageUnitStatusHistory,
        Boolean excludeBusinessObjectDataStorageFiles)
    {
        return getBusinessObjectDataImpl(businessObjectDataKey, businessObjectFormatPartitionKey, businessObjectDataStatus,
            includeBusinessObjectDataStatusHistory, includeStorageUnitStatusHistory, excludeBusinessObjectDataStorageFiles);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @PublishNotificationMessages
    @Override
    public BusinessObjectDataInvalidateUnregisteredResponse invalidateUnregisteredBusinessObjectData(
        BusinessObjectDataInvalidateUnregisteredRequest businessObjectDataInvalidateUnregisteredRequest)
    {
        return invalidateUnregisteredBusinessObjectDataImpl(businessObjectDataInvalidateUnregisteredRequest);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation keeps the current transaction context.
     */
    @Override
    public BusinessObjectData restoreBusinessObjectData(BusinessObjectDataKey businessObjectDataKey, Integer expirationInDays, String archiveRetrievalOption,
        Boolean batchMode)
    {
        return restoreBusinessObjectDataImpl(businessObjectDataKey, expirationInDays, archiveRetrievalOption, batchMode);
    }
}
