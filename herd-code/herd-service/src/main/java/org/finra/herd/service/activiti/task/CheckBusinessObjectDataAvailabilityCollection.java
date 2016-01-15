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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionResponse;
import org.finra.herd.service.BusinessObjectDataService;

/**
 * An Activiti task that performs an availability check for a collection of business object data.
 * <p/>
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="contentType" stringValue=""/>
 *   <activiti:field name="businessObjectDataAvailabilityCollectionRequest" stringValue=""/>
 * </extensionElements>
 * </pre>
 */
@Component
public class CheckBusinessObjectDataAvailabilityCollection extends BaseJavaDelegate
{
    public static final String VARIABLE_IS_ALL_DATA_AVAILABLE = "isAllDataAvailable";
    public static final String VARIABLE_IS_ALL_DATA_NOT_AVAILABLE = "isAllDataNotAvailable";

    private Expression contentType;
    private Expression businessObjectDataAvailabilityCollectionRequest;

    @Autowired
    private BusinessObjectDataService businessObjectDataService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        String contentTypeString = activitiHelper.getRequiredExpressionVariableAsString(contentType, execution, "ContentType").trim();
        String requestString = activitiHelper.getRequiredExpressionVariableAsString(businessObjectDataAvailabilityCollectionRequest, execution,
            "BusinessObjectDataAvailabilityCollectionRequest").trim();

        BusinessObjectDataAvailabilityCollectionRequest request =
            getRequestObject(contentTypeString, requestString, BusinessObjectDataAvailabilityCollectionRequest.class);

        // Call the business object data availability service.
        BusinessObjectDataAvailabilityCollectionResponse businessObjectDataAvailabilityCollectionResponse =
            businessObjectDataService.checkBusinessObjectDataAvailabilityCollection(request);

        // Set the JSON response as a workflow variable.
        setJsonResponseAsWorkflowVariable(businessObjectDataAvailabilityCollectionResponse, execution);

        // Set workflow variables for whether all the data is available or not available for easy access.
        setTaskWorkflowVariable(execution, VARIABLE_IS_ALL_DATA_AVAILABLE, businessObjectDataAvailabilityCollectionResponse.isIsAllDataAvailable());
        setTaskWorkflowVariable(execution, VARIABLE_IS_ALL_DATA_NOT_AVAILABLE, businessObjectDataAvailabilityCollectionResponse.isIsAllDataNotAvailable());
    }
}
