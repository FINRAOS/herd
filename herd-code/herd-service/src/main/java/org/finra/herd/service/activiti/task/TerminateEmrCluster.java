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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.EmrCluster;
import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;

/**
 * An Activiti task that terminates the EMR cluster
 * <p/>
 * <pre>
 * <extensionElements>
 *   <activiti:field name="namespaceCode" stringValue="" />
 *   <activiti:field name="emrClusterDefinitionName" stringValue="" />
 *   <activiti:field name="emrClusterName" stringValue="" />
 *   <activiti:field name="overrideTerminationProtection" stringValue="" />
 * </extensionElements>
 * </pre>
 */
@Component
public class TerminateEmrCluster extends BaseEmrCluster
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TerminateEmrCluster.class);

    private Expression overrideTerminationProtection;
    private Expression emrClusterId;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = getClusterAlternateKey(execution);

        boolean overrideTerminationProtectionBoolean =
            activitiHelper.getExpressionVariableAsBoolean(overrideTerminationProtection, execution, "overrideTerminationProtection", false, false);
        String emrClusterIdString = activitiHelper.getExpressionVariableAsString(emrClusterId, execution);

        // Terminate the EMR cluster.
        EmrCluster emrCluster = emrService.terminateCluster(emrClusterAlternateKeyDto, overrideTerminationProtectionBoolean, emrClusterIdString);

        // Set workflow variables based on the result EMR cluster that was terminated.
        setIdStatusWorkflowVariables(execution, emrCluster);
        LOGGER.info("{} EMR cluster terminated. emrClusterId=\"{}\"", activitiHelper.getProcessIdentifyingInformation(execution), emrCluster.getId());
    }
}