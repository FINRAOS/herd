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

import java.util.List;

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitStatusUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitStatusUpdateResponse;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.service.BusinessObjectDataStorageUnitStatusService;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.helper.StorageUnitHelper;

/**
 * An Activiti task that updates status for a business object data storage unit.
 * <p/>
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="namespace" stringValue=""/>
 *   <activiti:field name="businessObjectDefinitionName" stringValue=""/>
 *   <activiti:field name="businessObjectFormatUsage" stringValue=""/>
 *   <activiti:field name="businessObjectFormatFileType" stringValue=""/>
 *   <activiti:field name="businessObjectFormatVersion" stringValue=""/>
 *   <activiti:field name="partitionValue" stringValue=""/>
 *   <activiti:field name="subPartitionValues" stringValue=""/>
 *   <activiti:field name="businessObjectDataVersion" stringValue=""/>
 *   <activiti:field name="storageName" stringValue=""/>
 *   <activiti:field name="businessObjectDataStorageUnitStatus" stringValue=""/>
 * </extensionElements>
 * </pre>
 */
@Component
public class UpdateBusinessObjectDataStorageUnitStatus extends BaseJavaDelegate
{
    private Expression businessObjectDataStorageUnitStatus;

    @Autowired
    private BusinessObjectDataStorageUnitStatusService businessObjectDataStorageUnitStatusService;

    private Expression businessObjectDataVersion;

    private Expression businessObjectDefinitionName;

    private Expression businessObjectFormatFileType;

    private Expression businessObjectFormatUsage;

    private Expression businessObjectFormatVersion;

    private Expression namespace;

    @Autowired
    private NotificationEventService notificationEventService;

    private Expression partitionValue;

    private Expression storageName;

    @Autowired
    private StorageUnitHelper storageUnitHelper;

    private Expression subPartitionValues;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        String namespace = activitiHelper.getExpressionVariableAsString(this.namespace, execution);
        String businessObjectDefinitionName = activitiHelper.getExpressionVariableAsString(this.businessObjectDefinitionName, execution);
        String businessObjectFormatUsage = activitiHelper.getExpressionVariableAsString(this.businessObjectFormatUsage, execution);
        String businessObjectFormatFileType = activitiHelper.getExpressionVariableAsString(this.businessObjectFormatFileType, execution);
        Integer businessObjectFormatVersion =
            activitiHelper.getExpressionVariableAsInteger(this.businessObjectFormatVersion, execution, "businessObjectFormatVersion", true);
        String partitionValue = activitiHelper.getExpressionVariableAsString(this.partitionValue, execution);
        String subPartitionValuesString = activitiHelper.getExpressionVariableAsString(this.subPartitionValues, execution);
        List<String> subPartitionValues = daoHelper.splitStringWithDefaultDelimiterEscaped(subPartitionValuesString);
        String storageName = activitiHelper.getExpressionVariableAsString(this.storageName, execution);
        Integer businessObjectDataVersion =
            activitiHelper.getExpressionVariableAsInteger(this.businessObjectDataVersion, execution, "businessObjectDataVersion", true);
        String businessObjectDataStorageUnitStatus = activitiHelper.getExpressionVariableAsString(this.businessObjectDataStorageUnitStatus, execution);

        BusinessObjectDataStorageUnitKey businessObjectDataStorageUnitKey = new BusinessObjectDataStorageUnitKey();
        businessObjectDataStorageUnitKey.setNamespace(namespace);
        businessObjectDataStorageUnitKey.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        businessObjectDataStorageUnitKey.setBusinessObjectFormatUsage(businessObjectFormatUsage);
        businessObjectDataStorageUnitKey.setBusinessObjectFormatFileType(businessObjectFormatFileType);
        businessObjectDataStorageUnitKey.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        businessObjectDataStorageUnitKey.setPartitionValue(partitionValue);
        businessObjectDataStorageUnitKey.setSubPartitionValues(subPartitionValues);
        businessObjectDataStorageUnitKey.setBusinessObjectDataVersion(businessObjectDataVersion);
        businessObjectDataStorageUnitKey.setStorageName(storageName);

        BusinessObjectDataStorageUnitStatusUpdateResponse businessObjectDataStorageUnitStatusUpdateResponse = businessObjectDataStorageUnitStatusService
            .updateBusinessObjectDataStorageUnitStatus(businessObjectDataStorageUnitKey,
                new BusinessObjectDataStorageUnitStatusUpdateRequest(businessObjectDataStorageUnitStatus));

        // Create a storage unit notification.
        notificationEventService.processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG,
            storageUnitHelper.getBusinessObjectDataKey(businessObjectDataStorageUnitStatusUpdateResponse.getBusinessObjectDataStorageUnitKey()),
            businessObjectDataStorageUnitStatusUpdateResponse.getBusinessObjectDataStorageUnitKey().getStorageName(),
            businessObjectDataStorageUnitStatusUpdateResponse.getStatus(), businessObjectDataStorageUnitStatusUpdateResponse.getPreviousStatus());

        setJsonResponseAsWorkflowVariable(businessObjectDataStorageUnitStatusUpdateResponse, execution);
    }
}
