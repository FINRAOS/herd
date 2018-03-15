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
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitStatusUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitStatusUpdateResponse;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.jpa.StorageUnitEntity;
import org.finra.herd.model.jpa.StorageUnitStatusEntity;
import org.finra.herd.service.BusinessObjectDataStorageUnitStatusService;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.StorageUnitDaoHelper;
import org.finra.herd.service.helper.StorageUnitHelper;
import org.finra.herd.service.helper.StorageUnitStatusDaoHelper;

/**
 * The business object data storage unit status service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectDataStorageUnitStatusServiceImpl implements BusinessObjectDataStorageUnitStatusService
{
    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private StorageUnitDaoHelper storageUnitDaoHelper;

    @Autowired
    private StorageUnitHelper storageUnitHelper;

    @Autowired
    private StorageUnitStatusDaoHelper storageUnitStatusDaoHelper;

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation starts a new transaction.
     */
    @PublishNotificationMessages
    @NamespacePermission(fields = "#businessObjectDataStorageUnitKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataStorageUnitStatusUpdateResponse updateBusinessObjectDataStorageUnitStatus(
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey, BusinessObjectDataStorageUnitStatusUpdateRequest request)
    {
        return updateBusinessObjectDataStorageUnitStatusImpl(businessObjectDataStorageUnitKey, request);
    }

    /**
     * Updates status of a business object data storage unit.
     *
     * @param businessObjectDataStorageUnitKey the business object data storage unit key
     * @param request the business object data storage unit status update request
     *
     * @return the business object data storage unit status update response
     */
    protected BusinessObjectDataStorageUnitStatusUpdateResponse updateBusinessObjectDataStorageUnitStatusImpl(
        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey, BusinessObjectDataStorageUnitStatusUpdateRequest request)
    {
        // Validate and trim the business object data storage unit key.
        storageUnitHelper.validateBusinessObjectDataStorageUnitKey(businessObjectDataStorageUnitKey);

        // Validate status.
        Assert.hasText(request.getStatus(), "A business object data storage unit status must be specified.");
        request.setStatus(request.getStatus().trim());

        // Retrieve and ensure that a business object data storage unit exists with the specified key.
        StorageUnitEntity storageUnitEntity = storageUnitDaoHelper.getStorageUnitEntityByKey(businessObjectDataStorageUnitKey);

        // Retrieve and ensure the new storage unit status entity exists.
        StorageUnitStatusEntity newStorageUnitStatusEntity = storageUnitStatusDaoHelper.getStorageUnitStatusEntity(request.getStatus());

        // Save the old storage unit status value.
        String oldStorageUnitStatus = storageUnitEntity.getStatus().getCode();

        // Update the storage unit status.
        storageUnitDaoHelper.updateStorageUnitStatus(storageUnitEntity, newStorageUnitStatusEntity, request.getStatus());

        // Get business object data key from the business object data entity.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.createBusinessObjectDataKeyFromEntity(storageUnitEntity.getBusinessObjectData());

        // Create and return the business object data storage unit status response object.
        BusinessObjectDataStorageUnitStatusUpdateResponse response = new BusinessObjectDataStorageUnitStatusUpdateResponse();
        response.setBusinessObjectDataStorageUnitKey(
            storageUnitHelper.createBusinessObjectDataStorageUnitKey(businessObjectDataKey, storageUnitEntity.getStorage().getName()));
        response.setStatus(storageUnitEntity.getStatus().getCode());
        response.setPreviousStatus(oldStorageUnitStatus);

        return response;
    }
}
