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

import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchResult;
import org.finra.herd.service.BusinessObjectDataService;


/**
 * An Activiti task that performs a search of business object data.
 * <p/>
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="contentType" stringValue=""/>
 *   <activiti:field name="businessObjectDataSearchRequest" stringValue=""/>
 * </extensionElements>
 * </pre>
 */

@Component
public class SearchBusinessObjectData extends BaseJavaDelegate
{
    private Expression contentType;

    private Expression businessObjectDataSearchRequest;

    private Expression pageNum;

    private Expression pageSize;

    @Autowired
    private BusinessObjectDataService businessObjectDataService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        String contentTypeString = activitiHelper.getRequiredExpressionVariableAsString(contentType, execution, "ContentType").trim();
        String requestString =
            activitiHelper.getRequiredExpressionVariableAsString(businessObjectDataSearchRequest, execution, "BusinessObjectDataSearchRequest").trim();
        Integer pageNum = activitiHelper.getExpressionVariableAsInteger(this.pageNum, execution, "pageNum", false);
        Integer pageSize = activitiHelper.getExpressionVariableAsInteger(this.pageSize, execution, "pageSize", false);

        BusinessObjectDataSearchRequest request = getRequestObject(contentTypeString, requestString, BusinessObjectDataSearchRequest.class);

        // Call the business object data search service
        BusinessObjectDataSearchResult businessObjectDataSearchResult = businessObjectDataService.searchBusinessObjectData(pageNum, pageSize, request);

        // Set the JSON response as a workflow variable.
        setJsonResponseAsWorkflowVariable(businessObjectDataSearchResult, execution);
    }
}
