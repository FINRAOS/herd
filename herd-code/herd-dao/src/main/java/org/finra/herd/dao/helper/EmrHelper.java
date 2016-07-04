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
package org.finra.herd.dao.helper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.dao.EmrDao;
import org.finra.herd.dao.impl.OozieDaoImpl;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * A helper class that provides EMR functions.
 */
@Component
public class EmrHelper extends AwsHelper
{
    @Autowired
    private EmrDao emrDao;

    /**
     * Returns EMR cluster name constructed according to the template defined.
     *
     * @param namespaceCd the namespace code value.
     * @param emrDefinitionName the EMR definition name value.
     * @param clusterName the cluster name value.
     *
     * @return the cluster name.
     */
    public String buildEmrClusterName(String namespaceCd, String emrDefinitionName, String clusterName)
    {
        // Set the token delimiter based on the environment configuration.
        String tokenDelimiter = configurationHelper.getProperty(ConfigurationValue.TEMPLATE_TOKEN_DELIMITER);

        // Setup the individual token names (using the configured delimiter).
        String namespaceToken = tokenDelimiter + "namespace" + tokenDelimiter;
        String emrDefinitionToken = tokenDelimiter + "emrDefinitionName" + tokenDelimiter;
        String clusterNameToken = tokenDelimiter + "clusterName" + tokenDelimiter;

        // Populate a map with the tokens mapped to actual database values.
        Map<String, String> pathToTokenValueMap = new HashMap<>();
        pathToTokenValueMap.put(namespaceToken, namespaceCd);
        pathToTokenValueMap.put(emrDefinitionToken, emrDefinitionName);
        pathToTokenValueMap.put(clusterNameToken, clusterName);

        // Set the default EMR cluster name tokenized template.
        // ~namespace~.~emrDefinitionName~.clusterName
        String defaultClusterNameTemplate = namespaceToken + "." + emrDefinitionToken + "." + clusterNameToken;

        // Get the EMR cluster name template from the environment, but use the default if one isn't configured.
        // This gives us the ability to customize/change the format post deployment.
        String emrClusterName = configurationHelper.getProperty(ConfigurationValue.EMR_CLUSTER_NAME_TEMPLATE);

        if (emrClusterName == null)
        {
            emrClusterName = defaultClusterNameTemplate;
        }

        // Substitute the tokens with the actual database values.
        for (Map.Entry<String, String> mapEntry : pathToTokenValueMap.entrySet())
        {
            emrClusterName = emrClusterName.replaceAll(mapEntry.getKey(), mapEntry.getValue());
        }

        return emrClusterName;
    }

    /**
     * Get the S3_STAGING_RESOURCE full path from the bucket name as well as other details.
     *
     * @return the s3 managed location.
     */
    public String getS3StagingLocation()
    {
        return configurationHelper.getProperty(ConfigurationValue.S3_URL_PROTOCOL) +
            configurationHelper.getProperty(ConfigurationValue.S3_STAGING_BUCKET_NAME) +
            configurationHelper.getProperty(ConfigurationValue.S3_URL_PATH_DELIMITER) +
            configurationHelper.getProperty(ConfigurationValue.S3_STAGING_RESOURCE_BASE);
    }

    /**
     * Gets the S3 to HDFS copy script name.
     *
     * @return the S3 to HDFS copy script name.
     * @throws IllegalStateException if the S3 to HDGFS copy script name is not configured.
     */
    public String getS3HdfsCopyScriptName() throws IllegalStateException
    {
        String s3HdfsCopyScript = configurationHelper.getProperty(ConfigurationValue.EMR_S3_HDFS_COPY_SCRIPT);
        if (StringUtils.isBlank(s3HdfsCopyScript))
        {
            throw new IllegalStateException(String.format("No S3 to HDFS copy script name found. Ensure the \"%s\" configuration entry is configured.",
                ConfigurationValue.EMR_S3_HDFS_COPY_SCRIPT.getKey()));
        }

        return s3HdfsCopyScript;
    }

    /**
     * Gets the Oozie herd wrapper Workflow S3 Location ConfigurationValue.
     *
     * @return the ConfigurationValue of S3 location of herd oozie wrapper workflow.
     * @throws IllegalStateException if the S3 location of herd wrapper workflow is not configured.
     */
    public ConfigurationValue getEmrOozieHerdWorkflowS3LocationConfiguration() throws IllegalStateException
    {
        String s3HdfsCopyScript = configurationHelper.getProperty(ConfigurationValue.EMR_OOZIE_HERD_WRAPPER_WORKFLOW_S3_LOCATION);
        if (StringUtils.isBlank(s3HdfsCopyScript))
        {
            throw new IllegalStateException(String
                .format("No herd wrapper oozie workflow S3 locaton found. Ensure the \"%s\" configuration entry is configured.",
                    ConfigurationValue.EMR_OOZIE_HERD_WRAPPER_WORKFLOW_S3_LOCATION.getKey()));
        }

        return ConfigurationValue.EMR_OOZIE_HERD_WRAPPER_WORKFLOW_S3_LOCATION;
    }

    /**
     * Retrieves the workflow action for the client workflow. This is the sub workflow with the name OozieDaoImpl.ACTION_NAME_CLIENT_WORKFLOW. Returns null if
     * not found.
     *
     * @param wrapperWorkflowJob the herd wrapper workflow job.
     *
     * @return the client workflow action.
     */
    public WorkflowAction getClientWorkflowAction(WorkflowJob wrapperWorkflowJob)
    {
        WorkflowAction clientWorkflowAction = null;
        if (wrapperWorkflowJob.getActions() != null)
        {
            for (WorkflowAction workflowAction : wrapperWorkflowJob.getActions())
            {
                if (OozieDaoImpl.ACTION_NAME_CLIENT_WORKFLOW.equals(workflowAction.getName()))
                {
                    clientWorkflowAction = workflowAction;
                    break;
                }
            }
        }

        return clientWorkflowAction;
    }

    /**
     * Retrieves the first workflow action that is in error. Returns null if not found.
     *
     * @param workflowJob the oozie workflow job.
     *
     * @return the workflow action that has errors.
     */
    public WorkflowAction getFirstWorkflowActionInError(WorkflowJob workflowJob)
    {
        WorkflowAction errorWorkflowAction = null;
        if (workflowJob.getActions() != null)
        {
            for (WorkflowAction workflowAction : workflowJob.getActions())
            {
                if (workflowAction.getStatus().equals(WorkflowAction.Status.ERROR))
                {
                    errorWorkflowAction = workflowAction;
                    break;
                }
            }
        }

        return errorWorkflowAction;
    }

    public boolean isActiveEmrState(String status)
    {
        return Arrays.asList(getActiveEmrClusterStates()).contains(status);
    }

    /**
     * Builds the StepConfig for the Hadoop jar step.
     *
     * @param stepName the step name.
     * @param jarLocation the location of jar.
     * @param mainClass the main class.
     * @param scriptArguments the arguments.
     * @param isContinueOnError indicate what to do on error.
     *
     * @return the stepConfig.
     */
    public StepConfig getEmrHadoopJarStepConfig(String stepName, String jarLocation, String mainClass, List<String> scriptArguments, Boolean isContinueOnError)
    {
        // Default ActionOnFailure is to cancel the execution and wait
        ActionOnFailure actionOnFailure = ActionOnFailure.CANCEL_AND_WAIT;

        if (isContinueOnError != null && isContinueOnError)
        {
            // Override based on user input
            actionOnFailure = ActionOnFailure.CONTINUE;
        }

        // If there are no arguments
        if (CollectionUtils.isEmpty(scriptArguments))
        {
            // Build the StepConfig object and return
            return new StepConfig().withName(stepName.trim()).withActionOnFailure(actionOnFailure)
                .withHadoopJarStep(new HadoopJarStepConfig().withJar(jarLocation.trim()).withMainClass(mainClass));
        }
        else
        {
            // If there are arguments, include the arguments in the StepConfig object
            return new StepConfig().withName(stepName.trim()).withActionOnFailure(actionOnFailure).withHadoopJarStep(
                new HadoopJarStepConfig().withJar(jarLocation.trim()).withMainClass(mainClass)
                    .withArgs(scriptArguments.toArray(new String[scriptArguments.size()])));
        }
    }

    private String[] getActiveEmrClusterStates()
    {
        String emrStatesString = configurationHelper.getProperty(ConfigurationValue.EMR_VALID_STATES);
        return emrStatesString.split("\\" + configurationHelper.getProperty(ConfigurationValue.FIELD_DATA_DELIMITER));
    }

    /**
     * Gets the ID of an active EMR cluster which matches the given criteria. If both cluster ID and cluster name is specified, the name of the actual cluster
     * with the given ID must match the specified name. For cases where the cluster is not found (does not exists or not active), the method fails. All
     * parameters are case-insensitive and whitespace trimmed. Blank parameters are equal to null.
     *
     * @param emrClusterId EMR cluster ID
     * @param emrClusterName EMR cluster name
     *
     * @return The cluster ID
     */
    public String getActiveEmrClusterId(String emrClusterId, String emrClusterName)
    {
        boolean emrClusterIdSpecified = StringUtils.isNotBlank(emrClusterId);
        boolean emrClusterNameSpecified = StringUtils.isNotBlank(emrClusterName);

        Assert.isTrue(emrClusterIdSpecified || emrClusterNameSpecified, "One of EMR cluster ID or EMR cluster name must be specified.");

        // Get cluster by ID first
        if (emrClusterIdSpecified)
        {
            String emrClusterIdTrimmed = emrClusterId.trim();

            // Assert cluster exists
            Cluster cluster = emrDao.getEmrClusterById(emrClusterIdTrimmed, getAwsParamsDto());
            Assert.notNull(cluster, String.format("The cluster with ID \"%s\" does not exist.", emrClusterIdTrimmed));

            // Assert the cluster's state is active
            String emrClusterState = cluster.getStatus().getState();
            Assert.isTrue(isActiveEmrState(emrClusterState), String
                .format("The cluster with ID \"%s\" is not active. The cluster state must be in one of %s. Current state is \"%s\"", emrClusterIdTrimmed,
                    Arrays.toString(getActiveEmrClusterStates()), emrClusterState));

            // Assert cluster name equals if cluster name was specified
            if (emrClusterNameSpecified)
            {
                String emrClusterNameTrimmed = emrClusterName.trim();
                Assert.isTrue(cluster.getName().equalsIgnoreCase(emrClusterNameTrimmed), String
                    .format("The cluster with ID \"%s\" does not match the expected name \"%s\". The actual name is \"%s\".", cluster.getId(),
                        emrClusterNameTrimmed, cluster.getName()));
            }

            return cluster.getId();
        }
        else
        {
            String emrClusterNameTrimmed = emrClusterName.trim();
            ClusterSummary clusterSummary = emrDao.getActiveEmrClusterByName(emrClusterNameTrimmed, getAwsParamsDto());
            Assert.notNull(clusterSummary, String.format("The cluster with name \"%s\" does not exist.", emrClusterNameTrimmed));
            return clusterSummary.getId();
        }
    }

    public EmrDao getEmrDao()
    {
        return emrDao;
    }

    public void setEmrDao(EmrDao emrDao)
    {
        this.emrDao = emrDao;
    }
}
