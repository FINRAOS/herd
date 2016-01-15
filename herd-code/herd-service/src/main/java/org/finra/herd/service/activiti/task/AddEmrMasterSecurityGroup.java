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
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.EmrMasterSecurityGroup;
import org.finra.herd.model.api.xml.EmrMasterSecurityGroupAddRequest;
import org.finra.herd.service.EmrService;

/**
 * An Activiti task that adds security groups to master nodes of EMR cluster
 * <p/>
 * 
 * <pre>
 * <extensionElements>
 *   <activiti:field name="namespaceCode" stringValue="" />
 *   <activiti:field name="emrClusterDefinitionName" stringValue="" />
 *   <activiti:field name="emrClusterName" stringValue="" />
 *   <activiti:field name="securityGroupIds" stringValue="" />
 * </extensionElements>
 * </pre>
 */
@Component
public class AddEmrMasterSecurityGroup extends BaseJavaDelegate
{
    public static final String VARIABLE_EMR_MASTER_SECURITY_GROUPS = "emrMasterSecurityGroupIds";

    private Expression namespace;
    private Expression emrClusterDefinitionName;
    private Expression emrClusterName;
    private Expression securityGroupIds;

    @Autowired
    private EmrService emrService;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        // Create the request.
        EmrMasterSecurityGroupAddRequest request = new EmrMasterSecurityGroupAddRequest();
        request.setNamespace(activitiHelper.getExpressionVariableAsString(namespace, execution));
        request.setEmrClusterDefinitionName(activitiHelper.getExpressionVariableAsString(emrClusterDefinitionName, execution));
        request.setEmrClusterName(activitiHelper.getExpressionVariableAsString(emrClusterName, execution));

        String groupIdStr = activitiHelper.getExpressionVariableAsString(securityGroupIds, execution);
        if (StringUtils.isBlank(groupIdStr))
        {
            throw new IllegalArgumentException("At least one security group must be specified.");
        }

        request.setSecurityGroupIds(daoHelper.splitStringWithDefaultDelimiter(groupIdStr.trim()));
        // Create the cluster in a new transaction.
        EmrMasterSecurityGroup emrMasterSecurityGroup = emrService.addSecurityGroupsToClusterMaster(request);

        // Set workflow variables based on the security groups.
        setTaskWorkflowVariable(execution, VARIABLE_EMR_MASTER_SECURITY_GROUPS, 
                herdHelper.buildStringWithDefaultDelimiter(emrMasterSecurityGroup.getSecurityGroupIds()));
    }
}