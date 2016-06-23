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

import java.util.Arrays;

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.service.BusinessObjectDataService;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.helper.BusinessObjectDataHelper;

/**
 * An Activiti task that registers the business object data.
 * <p/>
 * <p/>
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="contentType" stringValue=""/>
 *   <activiti:field name="businessObjectDataCreateRequest" stringValue=""/>
 * </extensionElements>
 * </pre>
 */
@Component
public class RegisterBusinessObjectData extends BaseJavaDelegate
{
    public static final String VARIABLE_ID = "id";
    public static final String VARIABLE_VERSION = "version";
    public static final String VARIABLE_LATEST_VERSION = "isLatestVersion";

    private Expression contentType;
    private Expression businessObjectDataCreateRequest;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private BusinessObjectDataService businessObjectDataService;

    @Autowired
    private NotificationEventService notificationEventService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        String contentTypeString = activitiHelper.getRequiredExpressionVariableAsString(contentType, execution, "ContentType").trim();
        String requestString =
            activitiHelper.getRequiredExpressionVariableAsString(businessObjectDataCreateRequest, execution, "BusinessObjectDataCreateRequest").trim();

        BusinessObjectDataCreateRequest request = getRequestObject(contentTypeString, requestString, BusinessObjectDataCreateRequest.class);

        // Register the data.
        BusinessObjectData businessObjectData = businessObjectDataService.createBusinessObjectData(request);

        // Trigger notifications.
        BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectData);

        // Create business object data notifications.
        for (NotificationEventTypeEntity.EventTypesBdata eventType : Arrays
            .asList(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN, NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG))
        {
            notificationEventService.processBusinessObjectDataNotificationEventAsync(eventType, businessObjectDataKey, businessObjectData.getStatus(), null);
        }

        // Set the JSON response as a workflow variable.
        setJsonResponseAsWorkflowVariable(businessObjectData, execution);

        setTaskWorkflowVariable(execution, VARIABLE_ID, businessObjectData.getId());
        setTaskWorkflowVariable(execution, VARIABLE_VERSION, businessObjectData.getVersion());
        setTaskWorkflowVariable(execution, VARIABLE_LATEST_VERSION, businessObjectData.isLatestVersion());
    }
}
