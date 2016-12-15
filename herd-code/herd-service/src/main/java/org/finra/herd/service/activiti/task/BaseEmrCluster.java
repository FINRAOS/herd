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

import java.io.IOException;

import javax.xml.bind.JAXBException;

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.model.dto.EmrClusterAlternateKeyDto;
import org.finra.herd.model.api.xml.EmrCluster;
import org.finra.herd.service.EmrService;

/**
 * Base class for EMR cluster Activiti tasks
 */
public abstract class BaseEmrCluster extends BaseJavaDelegate
{
    protected static final String VARIABLE_EMR_CLUSTER_ID = "emrClusterId";
    protected static final String VARIABLE_EMR_CLUSTER_STATUS = "emrClusterStatus";
    protected static final String VARIABLE_EMR_CLUSTER_CREATED = "emrClusterCreated";
    protected static final String VARIABLE_EMR_CLUSTER_DEFINITION = "emrClusterDefinition";

    protected Expression namespace;
    protected Expression emrClusterDefinitionName;
    protected Expression emrClusterName;
    protected Expression dryRun;
    protected Expression contentType;
    protected Expression emrClusterDefinitionOverride;

    @Autowired
    protected EmrService emrService;

    /**
     * Builds the cluster alternate key and returns.
     *
     * @param execution the DelegateExecution
     *
     * @return the cluster alternate key.
     */
    protected EmrClusterAlternateKeyDto getClusterAlternateKey(DelegateExecution execution)
    {
        // Create the alternate key.
        EmrClusterAlternateKeyDto emrClusterAlternateKeyDto = new EmrClusterAlternateKeyDto();
        emrClusterAlternateKeyDto.setNamespace(activitiHelper.getExpressionVariableAsString(namespace, execution));
        emrClusterAlternateKeyDto.setEmrClusterDefinitionName(activitiHelper.getExpressionVariableAsString(emrClusterDefinitionName, execution));
        emrClusterAlternateKeyDto.setEmrClusterName(activitiHelper.getExpressionVariableAsString(emrClusterName, execution));

        return emrClusterAlternateKeyDto;
    }

    /**
     * Sets the EMR cluster id and status as activiti workflow variables.
     *
     * @param execution the DelegateExecution
     * @param emrCluster the EmrCluster
     */
    protected void setIdStatusWorkflowVariables(DelegateExecution execution, EmrCluster emrCluster)
    {
        // Set workflow variables based on the result EMR cluster that was created.
        setTaskWorkflowVariable(execution, VARIABLE_EMR_CLUSTER_ID, emrCluster.getId());
        setTaskWorkflowVariable(execution, VARIABLE_EMR_CLUSTER_STATUS, emrCluster.getStatus());
    }

    /**
     * Sets the result variables to Activiti workflow.
     *
     * @param execution the DelegateExecution
     * @param emrCluster the EmrCluster
     *
     * @throws IOException if an I/O exception occurred.
     * @throws JAXBException if a JAXB exception occurred.
     */
    protected void setResultWorkflowVariables(DelegateExecution execution, EmrCluster emrCluster) throws JAXBException, IOException
    {
        // Set workflow variables based on the result EMR cluster that was created.
        setIdStatusWorkflowVariables(execution, emrCluster);
        setTaskWorkflowVariable(execution, VARIABLE_EMR_CLUSTER_CREATED, emrCluster.isEmrClusterCreated());
        if (emrCluster.getEmrClusterDefinition() != null)
        {
            setTaskWorkflowVariable(execution, VARIABLE_EMR_CLUSTER_DEFINITION, jsonHelper.objectToJson(emrCluster.getEmrClusterDefinition()));
        }
    }
}
