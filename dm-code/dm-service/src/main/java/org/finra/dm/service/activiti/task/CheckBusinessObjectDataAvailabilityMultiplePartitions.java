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
package org.finra.dm.service.activiti.task;

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.dm.model.api.xml.BusinessObjectDataAvailability;
import org.finra.dm.model.api.xml.BusinessObjectDataAvailabilityRequest;
import org.finra.dm.service.BusinessObjectDataService;

/**
 * An Activiti task that performs a search and returns a list of business object data status information for a range of requested business object data in the
 * specified storage.
 * <p/>
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="contentType" stringValue=""/>
 *   <activiti:field name="businessObjectDataAvailabilityRequest" stringValue=""/>
 * </extensionElements>
 * </pre>
 */
@Component
public class CheckBusinessObjectDataAvailabilityMultiplePartitions extends BaseJavaDelegate
{
    public static final String VARIABLE_IS_ALL_DATA_AVAILABLE = "isAllDataAvailable";

    private Expression contentType;
    private Expression businessObjectDataAvailabilityRequest;

    @Autowired
    private BusinessObjectDataService businessObjectDataService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        String contentTypeString = activitiHelper.getRequiredExpressionVariableAsString(contentType, execution, "ContentType").trim();
        String requestString =
            activitiHelper.getRequiredExpressionVariableAsString(businessObjectDataAvailabilityRequest, execution, "BusinessObjectDataAvailabilityRequest")
                .trim();

        BusinessObjectDataAvailabilityRequest request = getRequestObject(contentTypeString, requestString, BusinessObjectDataAvailabilityRequest.class);

        // Call the business object data availability service.
        BusinessObjectDataAvailability businessObjectDataAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Set the JSON response as a workflow variable.
        setJsonResponseAsWorkflowVariable(businessObjectDataAvailability, execution);

        // Set a workflow variable for whether all the data is available or not for easy access.
        boolean allAvailable = businessObjectDataAvailability.getNotAvailableStatuses().size() == 0;
        setTaskWorkflowVariable(execution, VARIABLE_IS_ALL_DATA_AVAILABLE, allAvailable);
    }
}
