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

import org.finra.dm.model.api.xml.BusinessObjectDataStorageFilesCreateRequest;
import org.finra.dm.model.api.xml.BusinessObjectDataStorageFilesCreateResponse;
import org.finra.dm.service.BusinessObjectDataStorageFileService;

/**
 * An Activiti task that adds storage files to an existing storage unit.
 * <p/>
 * <p/>
 * 
 * <pre>
 * <extensionElements>
 *   <activiti:field name="contentType" stringValue=""/>
 *   <activiti:field name="businessObjectDataStorageFilesCreateRequest" stringValue=""/>
 * </extensionElements>
 * </pre>
 */
@Component
public class AddBusinessObjectDataStorageFiles extends BaseJavaDelegate
{
    private Expression contentType;
    private Expression businessObjectDataStorageFilesCreateRequest;

    @Autowired
    private BusinessObjectDataStorageFileService businessObjectDataStorageFileService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        String contentTypeString = activitiHelper.getRequiredExpressionVariableAsString(contentType, execution, "ContentType").trim();
        String requestString =
                activitiHelper.getRequiredExpressionVariableAsString(businessObjectDataStorageFilesCreateRequest, execution, "BusinessObjectDataCreateRequest")
                        .trim();

        BusinessObjectDataStorageFilesCreateRequest request =
                getRequestObject(contentTypeString, requestString, BusinessObjectDataStorageFilesCreateRequest.class);

        // Call the BusinessObjectDataStorageFiles service.
        BusinessObjectDataStorageFilesCreateResponse businessObjectDataStorageFilesCreateResponse =
                businessObjectDataStorageFileService.createBusinessObjectDataStorageFiles(request);

        // Set the JSON response as a workflow variable.
        setJsonResponseAsWorkflowVariable(businessObjectDataStorageFilesCreateResponse, execution);
    }
}
