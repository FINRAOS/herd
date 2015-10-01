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
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.finra.dm.model.api.xml.EmrCluster;
import org.finra.dm.model.api.xml.EmrClusterCreateRequest;
import org.finra.dm.model.api.xml.EmrClusterDefinition;
import org.springframework.stereotype.Component;

/**
 * An Activiti task that creates the EMR cluster
 * <p/>
 * 
 * <pre>
 * <extensionElements>
 *   <activiti:field name="namespaceCode" stringValue="" />
 *   <activiti:field name="emrClusterDefinitionName" stringValue="" />
 *   <activiti:field name="emrClusterName" stringValue="" />
 *   <activiti:field name="dryRun" stringValue="" />
 *   <activiti:field name="contentType" stringValue="" />
 *   <activiti:field name="emrClusterDefinitionOverride" stringValue="" />
 * </extensionElements>
 * </pre>
 */
@Component
public class CreateEmrCluster extends BaseEmrCluster
{
    private static final Logger LOGGER = Logger.getLogger(CreateEmrCluster.class);

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        // Create the request.
        EmrClusterCreateRequest request = new EmrClusterCreateRequest();
        request.setNamespace(activitiHelper.getExpressionVariableAsString(namespace, execution));
        request.setEmrClusterDefinitionName(activitiHelper.getExpressionVariableAsString(emrClusterDefinitionName, execution));
        request.setEmrClusterName(activitiHelper.getExpressionVariableAsString(emrClusterName, execution));
        request.setDryRun(activitiHelper.getExpressionVariableAsBoolean(dryRun, execution, "dryRun", false, false));

        String contentTypeString = activitiHelper.getExpressionVariableAsString(contentType, execution);
        String emrClusterDefinitionOverrideString = activitiHelper.getExpressionVariableAsString(emrClusterDefinitionOverride, execution);

        if (StringUtils.isNotBlank(contentTypeString) && StringUtils.isBlank(emrClusterDefinitionOverrideString))
        {
            throw new IllegalArgumentException("emrClusterDefinitionOverride is required when contentType is specified");
        }
        else if (StringUtils.isBlank(contentTypeString) && StringUtils.isNotBlank(emrClusterDefinitionOverrideString))
        {
            throw new IllegalArgumentException("contentType is required when emrClusterDefinitionOverride is specified");
        }
        else if (StringUtils.isNotBlank(contentTypeString) && StringUtils.isNotBlank(emrClusterDefinitionOverrideString))
        {
            EmrClusterDefinition emrClusterDefinitionOverride =
                    getRequestObject(contentTypeString, emrClusterDefinitionOverrideString, EmrClusterDefinition.class);
            request.setEmrClusterDefinitionOverride(emrClusterDefinitionOverride);
        }

        // Create the cluster in a new transaction.
        EmrCluster emrCluster = emrService.createCluster(request);

        // Set workflow variables based on the result EMR cluster that was created.
        setResultWorkflowVariables(execution, emrCluster);
        LOGGER.info(activitiHelper.getProcessIdentifyingInformation(execution) + " EMR cluster started with cluster Id: " + emrCluster.getId());
    }
}