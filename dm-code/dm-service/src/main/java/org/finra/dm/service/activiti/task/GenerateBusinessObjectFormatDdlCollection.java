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

import org.finra.dm.model.api.xml.BusinessObjectFormatDdlCollectionRequest;
import org.finra.dm.model.api.xml.BusinessObjectFormatDdlCollectionResponse;
import org.finra.dm.service.BusinessObjectFormatService;

/**
 * An Activiti task that generates DDL for a collection of business object formats.
 * <p/>
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="contentType" stringValue=""/>
 *   <activiti:field name="businessObjectFormatDdlCollectionRequest" stringValue=""/>
 * </extensionElements>
 * </pre>
 */
@Component
public class GenerateBusinessObjectFormatDdlCollection extends BaseJavaDelegate
{
    public static final String VARIABLE_DDL_COLLECTION = "businessObjectFormatDdlCollection";

    private Expression contentType;
    private Expression businessObjectFormatDdlCollectionRequest;

    @Autowired
    private BusinessObjectFormatService businessObjectFormatService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        String contentTypeString = activitiHelper.getRequiredExpressionVariableAsString(contentType, execution, "ContentType").trim();
        String requestString = activitiHelper
            .getRequiredExpressionVariableAsString(businessObjectFormatDdlCollectionRequest, execution, "BusinessObjectFormatDdlCollectionRequest").trim();

        BusinessObjectFormatDdlCollectionRequest request = getRequestObject(contentTypeString, requestString, BusinessObjectFormatDdlCollectionRequest.class);

        // Call the business object format service to generate DDL for a collection of business object formats.
        BusinessObjectFormatDdlCollectionResponse businessObjectFormatDdlCollectionResponse =
            businessObjectFormatService.generateBusinessObjectFormatDdlCollection(request);

        // Set workflow variable for generated DDL collection.
        setTaskWorkflowVariable(execution, VARIABLE_DDL_COLLECTION, businessObjectFormatDdlCollectionResponse.getDdlCollection());
    }
}
