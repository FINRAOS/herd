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

import java.util.List;

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.dm.model.api.xml.BusinessObjectDataAvailability;
import org.finra.dm.model.api.xml.BusinessObjectDataAvailabilityRequest;
import org.finra.dm.model.api.xml.PartitionValueFilter;
import org.finra.dm.model.api.xml.PartitionValueRange;
import org.finra.dm.service.BusinessObjectDataService;

/**
 * An Activiti task that Performs a search and returns a list of business object data key values and relative statuses for the requested business object data.
 * <p/>
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="namespace" stringValue=""/>
 *   <activiti:field name="businessObjectDefinitionName" stringValue=""/>
 *   <activiti:field name="businessObjectFormatUsage" stringValue=""/>
 *   <activiti:field name="businessObjectFormatFileType" stringValue=""/>
 *   <activiti:field name="businessObjectFormatVersion" stringValue=""/>
 *   <activiti:field name="partitionValues" stringValue=""/>
 *   <activiti:field name="startPartitionValue" stringValue=""/>
 *   <activiti:field name="endPartitionValue" stringValue=""/>
 *   <activiti:field name="businessObjectDataVersion" stringValue=""/>
 *   <activiti:field name="storageName" stringValue=""/>
 * </extensionElements>
 * </pre>
 */
@Component
public class CheckBusinessObjectDataAvailability extends BaseJavaDelegate
{
    public static final String VARIABLE_IS_ALL_DATA_AVAILABLE = "isAllDataAvailable";

    private Expression namespace;
    private Expression businessObjectDefinitionName;
    private Expression businessObjectFormatUsage;
    private Expression businessObjectFormatFileType;
    private Expression businessObjectFormatVersion;
    private Expression partitionKey;
    private Expression partitionValues;
    private Expression startPartitionValue;
    private Expression endPartitionValue;
    private Expression businessObjectDataVersion;
    private Expression storageName;

    @Autowired
    private BusinessObjectDataService businessObjectDataService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        // Create the request.
        BusinessObjectDataAvailabilityRequest request = new BusinessObjectDataAvailabilityRequest();
        request.setNamespace(activitiHelper.getExpressionVariableAsString(namespace, execution));
        request.setBusinessObjectDefinitionName(activitiHelper.getExpressionVariableAsString(businessObjectDefinitionName, execution));
        request.setBusinessObjectFormatUsage(activitiHelper.getExpressionVariableAsString(businessObjectFormatUsage, execution));
        request.setBusinessObjectFormatFileType(activitiHelper.getExpressionVariableAsString(businessObjectFormatFileType, execution));
        request.setBusinessObjectFormatVersion(
            activitiHelper.getExpressionVariableAsInteger(businessObjectFormatVersion, execution, "BusinessObjectFormatVersion", false));

        // Build the partition value filter.
        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        request.setPartitionValueFilter(partitionValueFilter);

        // Set the partition key if present.
        String partitionKeyString = activitiHelper.getExpressionVariableAsString(partitionKey, execution);
        if (partitionKeyString != null)
        {
            partitionValueFilter.setPartitionKey(partitionKeyString);
        }

        // Set the partition values if present.
        String partitionValuesString = activitiHelper.getExpressionVariableAsString(partitionValues, execution);
        List<String> partitionValueList = daoHelper.splitStringWithDefaultDelimiterEscaped(partitionValuesString);
        if (!CollectionUtils.isEmpty(partitionValueList))
        {
            partitionValueFilter.setPartitionValues(partitionValueList);
        }

        // Build the partition value range and set it on the filter if present.
        PartitionValueRange partitionValueRange = new PartitionValueRange();
        partitionValueRange.setStartPartitionValue(activitiHelper.getExpressionVariableAsString(startPartitionValue, execution));
        partitionValueRange.setEndPartitionValue(activitiHelper.getExpressionVariableAsString(endPartitionValue, execution));
        if (StringUtils.isNotBlank(partitionValueRange.getStartPartitionValue()) || StringUtils.isNotBlank(partitionValueRange.getEndPartitionValue()))
        {
            partitionValueFilter.setPartitionValueRange(partitionValueRange);
        }

        // Set the business object data version if present.
        request.setBusinessObjectDataVersion(
            activitiHelper.getExpressionVariableAsInteger(businessObjectDataVersion, execution, "BusinessObjectDataVersion", false));

        // Set the storage.
        request.setStorageName(activitiHelper.getExpressionVariableAsString(storageName, execution));

        // Call the availability service.
        BusinessObjectDataAvailability businessObjectDataAvailability = businessObjectDataService.checkBusinessObjectDataAvailability(request);

        // Set the JSON response as a workflow variable.
        setJsonResponseAsWorkflowVariable(businessObjectDataAvailability, execution);

        // Set a workflow variable for whether all the data is available or not for easy access.
        boolean allAvailable = businessObjectDataAvailability.getNotAvailableStatuses().size() == 0;
        setTaskWorkflowVariable(execution, VARIABLE_IS_ALL_DATA_AVAILABLE, allAvailable);
    }
}