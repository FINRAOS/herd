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

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.model.dto.StoragePolicyTransitionParamsDto;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.StoragePolicyProcessorHelperService;
import org.finra.herd.service.StoragePolicyProcessorService;

/**
 * An implementation of the storage policy processor service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class StoragePolicyProcessorServiceImpl implements StoragePolicyProcessorService
{
    @Autowired
    private StoragePolicyProcessorHelperService storagePolicyProcessorHelperService;

    @Autowired
    private NotificationEventService notificationEventService;

    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void processStoragePolicySelectionMessage(StoragePolicySelection storagePolicySelection)
    {
        processStoragePolicySelectionMessageImpl(storagePolicySelection);
    }

    /**
     * Performs a storage policy transition as specified by the storage policy selection message.
     *
     * @param storagePolicySelection the storage policy selection message
     */
    protected void processStoragePolicySelectionMessageImpl(StoragePolicySelection storagePolicySelection)
    {
        // Create a storage policy transition parameters DTO.
        StoragePolicyTransitionParamsDto storagePolicyTransitionParamsDto = new StoragePolicyTransitionParamsDto();

        try
        {
            // Initiate the storage policy transition.
            storagePolicyProcessorHelperService.initiateStoragePolicyTransition(storagePolicyTransitionParamsDto, storagePolicySelection);

            // Create a storage unit notification for the source storage unit.
            notificationEventService.processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG,
                storagePolicyTransitionParamsDto.getBusinessObjectDataKey(), storagePolicyTransitionParamsDto.getStorageName(),
                storagePolicyTransitionParamsDto.getNewStorageUnitStatus(), storagePolicyTransitionParamsDto.getOldStorageUnitStatus());

            // Execute the actual transition using the DAO tier.
            storagePolicyProcessorHelperService.executeStoragePolicyTransition(storagePolicyTransitionParamsDto);

            // Complete the storage policy transition.
            storagePolicyProcessorHelperService.completeStoragePolicyTransition(storagePolicyTransitionParamsDto);

            // Create a storage unit notification for the source storage unit.
            notificationEventService.processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG,
                storagePolicyTransitionParamsDto.getBusinessObjectDataKey(), storagePolicyTransitionParamsDto.getStorageName(),
                storagePolicyTransitionParamsDto.getNewStorageUnitStatus(), storagePolicyTransitionParamsDto.getOldStorageUnitStatus());
        }
        catch (RuntimeException e)
        {
            // Try to increment the count for failed storage policy transition attempts for the specified storage unit.
            storagePolicyProcessorHelperService.updateStoragePolicyTransitionFailedAttemptsIgnoreException(storagePolicyTransitionParamsDto, e);

            // Rethrow the original exception.
            throw e;
        }
    }
}
