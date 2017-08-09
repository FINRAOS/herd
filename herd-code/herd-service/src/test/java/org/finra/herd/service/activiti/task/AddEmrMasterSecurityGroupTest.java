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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.activiti.bpmn.model.FieldExtension;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import org.finra.herd.model.api.xml.EmrClusterCreateRequest;
import org.finra.herd.model.api.xml.EmrClusterDefinition;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

/**
 * Tests the AddEmrMasterSecurityGroup Activiti task wrapper.
 */
public class AddEmrMasterSecurityGroupTest extends HerdActivitiServiceTaskTest
{
    @Test
    public void testAddEmrMasterSecurityGroup() throws Exception
    {
        // Create a pipe-separated list of security group IDs.
        final String securityGroupIds = EC2_SECURITY_GROUP_1 + "|" + EC2_SECURITY_GROUP_2;

        // Create EC2 on-demand pricing entities required for testing.
        ec2OnDemandPricingDaoTestHelper.createEc2OnDemandPricingEntities();

        // Create the namespace entity.
        NamespaceEntity namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);

        // Create a trusting AWS account.
        trustingAccountDaoTestHelper.createTrustingAccountEntity(AWS_ACCOUNT_ID, AWS_ROLE_ARN);

        // Create an EMR cluster definition.
        emrClusterDefinitionDaoTestHelper.createEmrClusterDefinitionEntity(namespaceEntity, EMR_CLUSTER_DEFINITION_NAME,
            IOUtils.toString(resourceLoader.getResource(EMR_CLUSTER_DEFINITION_XML_FILE_WITH_CLASSPATH).getInputStream()));

        // Create an EMR cluster definition override with an AWS account ID.
        EmrClusterDefinition emrClusterDefinitionOverride = new EmrClusterDefinition();
        emrClusterDefinitionOverride.setAccountId(AWS_ACCOUNT_ID);

        // Create an EMR cluster.
        emrService
            .createCluster(new EmrClusterCreateRequest(NAMESPACE, EMR_CLUSTER_DEFINITION_NAME, EMR_CLUSTER_NAME, NO_DRY_RUN, emrClusterDefinitionOverride));

        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("namespace", "${namespace}"));
        fieldExtensionList.add(buildFieldExtension("emrClusterDefinitionName", "${emrClusterDefinitionName}"));
        fieldExtensionList.add(buildFieldExtension("emrClusterName", "${emrClusterName}"));
        fieldExtensionList.add(buildFieldExtension("securityGroupIds", "${securityGroupIds}"));
        fieldExtensionList.add(buildFieldExtension("accountId", "${accountId}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("namespace", NAMESPACE));
        parameters.add(buildParameter("emrClusterDefinitionName", EMR_CLUSTER_DEFINITION_NAME));
        parameters.add(buildParameter("emrClusterName", EMR_CLUSTER_NAME));
        parameters.add(buildParameter("securityGroupIds", securityGroupIds));
        parameters.add(buildParameter("accountId", AWS_ACCOUNT_ID));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(AddEmrMasterSecurityGroup.VARIABLE_EMR_MASTER_SECURITY_GROUPS, securityGroupIds);

        testActivitiServiceTaskSuccess(AddEmrMasterSecurityGroup.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
    }

    @Test
    public void testAddEmrMasterSecurityGroupMissingSecurityGroupIds() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("namespace", "${namespace}"));
        fieldExtensionList.add(buildFieldExtension("emrClusterDefinitionName", "${emrClusterDefinitionName}"));
        fieldExtensionList.add(buildFieldExtension("emrClusterName", "${emrClusterName}"));
        fieldExtensionList.add(buildFieldExtension("securityGroupIds", "${securityGroupIds}"));
        fieldExtensionList.add(buildFieldExtension("accountId", "${accountId}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("namespace", NAMESPACE));
        parameters.add(buildParameter("emrClusterDefinitionName", EMR_CLUSTER_DEFINITION_NAME));
        parameters.add(buildParameter("emrClusterName", EMR_CLUSTER_NAME));
        parameters.add(buildParameter("securityGroupIds", BLANK_TEXT));
        parameters.add(buildParameter("accountId", AWS_ACCOUNT_ID));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "At least one security group must be specified.");

        executeWithoutLogging(ActivitiRuntimeHelper.class, () -> {
            testActivitiServiceTaskFailure(AddEmrMasterSecurityGroup.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate);
        });
    }
}
