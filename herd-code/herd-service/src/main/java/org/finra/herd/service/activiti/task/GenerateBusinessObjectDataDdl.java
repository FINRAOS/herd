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

import org.finra.herd.model.api.xml.BusinessObjectDataDdl;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlRequest;
import org.finra.herd.service.BusinessObjectDataService;

/**
 * An Activiti task that generates the business object data DDL.
 * <p/>
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="contentType" stringValue=""/>
 *   <activiti:field name="businessObjectDataDdlRequest" stringValue=""/>
 * </extensionElements>
 * </pre>
 */
@Component
public class GenerateBusinessObjectDataDdl extends BaseJavaDelegate
{
    public static final String VARIABLE_DDL = "businessObjectDataDdl";

    private Expression contentType;
    private Expression businessObjectDataDdlRequest;

    @Autowired
    private BusinessObjectDataService businessObjectDataService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        String contentTypeString = activitiHelper.getRequiredExpressionVariableAsString(contentType, execution, "ContentType").trim();
        String requestString =
            activitiHelper.getRequiredExpressionVariableAsString(businessObjectDataDdlRequest, execution, "businessObjectDataDdlRequest").trim();

        BusinessObjectDataDdlRequest request = getRequestObject(contentTypeString, requestString, BusinessObjectDataDdlRequest.class);

        // Generate Ddl.
        BusinessObjectDataDdl businessObjectDataDdl = businessObjectDataService.generateBusinessObjectDataDdl(request);

        setTaskWorkflowVariable(execution, VARIABLE_DDL, businessObjectDataDdl.getDdl());
    }
}