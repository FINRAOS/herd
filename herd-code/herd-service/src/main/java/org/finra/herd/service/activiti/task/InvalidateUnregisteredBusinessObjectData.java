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
package org.finra.herd.service.activiti.task;

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.service.BusinessObjectDataService;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.helper.BusinessObjectDataHelper;

/**
 * An Activiti task wrapper for {@link org.finra.herd.service.BusinessObjectDataService#invalidateUnregisteredBusinessObjectData
 * (BusinessObjectDataInvalidateUnregisteredRequest)}.
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="contentType" stringValue="" />
 *   <activiti:field name="businessObjectDataInvalidateUnregisteredRequest" stringValue="" />
 * </extensionElements>
 * </pre>
 */
@Component
public class InvalidateUnregisteredBusinessObjectData extends BaseJavaDelegate
{
    private Expression contentType;

    private Expression businessObjectDataInvalidateUnregisteredRequest;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private BusinessObjectDataService businessObjectDataService;

    @Autowired
    private NotificationEventService notificationEventService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        String contentTypeString = activitiHelper.getRequiredExpressionVariableAsString(this.contentType, execution, "contentType");
        String businessObjectDataInvalidateUnregisteredRequestString = activitiHelper
            .getRequiredExpressionVariableAsString(this.businessObjectDataInvalidateUnregisteredRequest, execution,
                "businessObjectDataInvalidateUnregisteredRequest");

        BusinessObjectDataInvalidateUnregisteredRequest businessObjectDataInvalidateUnregisteredRequest =
            getRequestObject(contentTypeString, businessObjectDataInvalidateUnregisteredRequestString, BusinessObjectDataInvalidateUnregisteredRequest.class);

        BusinessObjectDataInvalidateUnregisteredResponse businessObjectDataInvalidateUnregisteredResponse =
            businessObjectDataService.invalidateUnregisteredBusinessObjectData(businessObjectDataInvalidateUnregisteredRequest);

        // Trigger notifications.
        for (BusinessObjectData businessObjectData : businessObjectDataInvalidateUnregisteredResponse.getRegisteredBusinessObjectDataList())
        {
            BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectData);

            // Create business object data notifications.
            notificationEventService
                .processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG, businessObjectDataKey,
                    businessObjectData.getStatus(), null);

            // Create storage unit notifications.
            if (!CollectionUtils.isEmpty(businessObjectData.getStorageUnits()))
            {
                for (StorageUnit storageUnit : businessObjectData.getStorageUnits())
                {
                    notificationEventService
                        .processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG, businessObjectDataKey,
                            storageUnit.getStorage().getName(), storageUnit.getStorageUnitStatus(), null);
                }
            }
        }

        setJsonResponseAsWorkflowVariable(businessObjectDataInvalidateUnregisteredResponse, execution);
    }
}
