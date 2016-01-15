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

import org.activiti.engine.delegate.Expression;

import org.activiti.engine.delegate.DelegateExecution;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;
import org.finra.herd.model.api.xml.EmrCluster;

/**
 * An Activiti task that terminates the EMR cluster
 * <p/>
 * 
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
    private static final Logger LOGGER = Logger.getLogger(TerminateEmrCluster.class);

    private Expression overrideTerminationProtection;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = getClusterAlternateKey(execution);

        boolean overrideTerminationProtectionBoolean =
                activitiHelper.getExpressionVariableAsBoolean(overrideTerminationProtection, execution, "overrideTerminationProtection", false, false);
        
        // Terminate the EMR cluster.
        EmrCluster emrCluster = emrService.terminateCluster(emrClusterAlternateKeyDto, overrideTerminationProtectionBoolean);

        // Set workflow variables based on the result EMR cluster that was terminated.
        setIdStatusWorkflowVariables(execution, emrCluster);
        LOGGER.info(activitiHelper.getProcessIdentifyingInformation(execution) + " EMR cluster terminated with cluster Id: " + emrCluster.getId());
    }
}