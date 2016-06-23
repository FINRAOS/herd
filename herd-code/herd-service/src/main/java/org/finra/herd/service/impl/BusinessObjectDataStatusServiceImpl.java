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

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.annotation.PublishJmsMessages;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusInformation;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStatusUpdateResponse;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.service.BusinessObjectDataStatusService;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;

/**
 * The business object data status service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectDataStatusServiceImpl implements BusinessObjectDataStatusService
{
    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @NamespacePermission(fields = "#businessObjectDataKey.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataStatusInformation getBusinessObjectDataStatus(BusinessObjectDataKey businessObjectDataKey, String businessObjectFormatPartitionKey)
    {
        return getBusinessObjectDataStatusImpl(businessObjectDataKey, businessObjectFormatPartitionKey);
    }

    /**
     * Retrieves status information for an existing business object data.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectFormatPartitionKey the business object format partition key
     *
     * @return the retrieved business object data status information
     */
    protected BusinessObjectDataStatusInformation getBusinessObjectDataStatusImpl(BusinessObjectDataKey businessObjectDataKey,
        String businessObjectFormatPartitionKey)
    {
        String businessObjectFormatPartitionKeyLocal = businessObjectFormatPartitionKey;

        // Validate and trim the business object data key.
        businessObjectDataHelper.validateBusinessObjectDataKey(businessObjectDataKey, false, false);

        // If specified, trim the partition key parameter.
        if (businessObjectFormatPartitionKeyLocal != null)
        {
            businessObjectFormatPartitionKeyLocal = businessObjectFormatPartitionKeyLocal.trim();
        }

        // Get the business object data based on the specified parameters.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // If specified, ensure that partition key matches what's configured within the business object format.
        if (StringUtils.isNotBlank(businessObjectFormatPartitionKeyLocal))
        {
            String configuredPartitionKey = businessObjectDataEntity.getBusinessObjectFormat().getPartitionKey();
            Assert.isTrue(configuredPartitionKey.equalsIgnoreCase(businessObjectFormatPartitionKeyLocal), String
                .format("Partition key \"%s\" doesn't match configured business object format partition key \"%s\".", businessObjectFormatPartitionKeyLocal,
                    configuredPartitionKey));
        }

        // Create and return the business object data status information object.
        BusinessObjectDataStatusInformation businessObjectDataStatusInformation = new BusinessObjectDataStatusInformation();
        businessObjectDataStatusInformation.setBusinessObjectDataKey(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity));
        businessObjectDataStatusInformation.setStatus(businessObjectDataEntity.getStatus().getCode());

        return businessObjectDataStatusInformation;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @PublishJmsMessages
    @NamespacePermission(fields = "#businessObjectDataKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataStatusUpdateResponse updateBusinessObjectDataStatus(BusinessObjectDataKey businessObjectDataKey,
        BusinessObjectDataStatusUpdateRequest request)
    {
        return updateBusinessObjectDataStatusImpl(businessObjectDataKey, request);
    }

    /**
     * Updates status of the business object data.
     *
     * @param businessObjectDataKey the business object data key
     * @param request the business object data status update request
     *
     * @return the business object data status update response
     */
    protected BusinessObjectDataStatusUpdateResponse updateBusinessObjectDataStatusImpl(BusinessObjectDataKey businessObjectDataKey,
        BusinessObjectDataStatusUpdateRequest request)
    {
        // Validate and trim the business object data key.
        businessObjectDataHelper.validateBusinessObjectDataKey(businessObjectDataKey, true, true);

        // Validate status
        Assert.hasText(request.getStatus(), "A business object data status must be specified.");
        request.setStatus(request.getStatus().trim());

        // Retrieve and ensure that a business object data exists with the specified key.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Get the current status value.
        String previousBusinessObjectDataStatus = businessObjectDataEntity.getStatus().getCode();

        // Update the entity with the new values.
        businessObjectDataDaoHelper.updateBusinessObjectDataStatus(businessObjectDataEntity, request.getStatus());

        // Create and return the business object data status response object.
        BusinessObjectDataStatusUpdateResponse response = new BusinessObjectDataStatusUpdateResponse();
        response.setBusinessObjectDataKey(businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity));
        response.setStatus(businessObjectDataEntity.getStatus().getCode());
        response.setPreviousStatus(previousBusinessObjectDataStatus);

        return response;
    }
}
