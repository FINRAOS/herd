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
package org.finra.dm.dao.impl;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import org.finra.dm.core.helper.ConfigurationHelper;
import org.finra.dm.dao.OozieDao;
import org.finra.dm.dao.OozieOperations;
import org.finra.dm.dao.helper.EmrHelper;
import org.finra.dm.model.ObjectNotFoundException;
import org.finra.dm.model.dto.ConfigurationValue;
import org.finra.dm.model.api.xml.Parameter;

/**
 * The Oozie DAO implementation.
 */
@Repository
public class OozieDaoImpl implements OozieDao
{
    /*
     * The status that will be returned, when DM wrapper workflow has not yet started to run the client workflow. 
     */
    public static final String OOZIE_WORKFLOW_JOB_STATUS_DM_PREP = "DM_PREP";
    public static final String OOZIE_WORKFLOW_JOB_STATUS_DM_FAILED = "DM_FAILED";
    
    public static final String OOZIE_ERROR_CODE_JOB_DOES_NOT_EXIST = "E0604";
    public static final String DM_OOZIE_WRAPPER_WORKFLOW_NAME = "dm_wrapper_workflow";
    public static final String ACTION_NAME_CLIENT_WORKFLOW = "client_workflow";

    private static final String MAP_REDUCE_QUEUE_NAME_VARIABLE = "queueName";
    private static final String MAP_REDUCE_QUEUE_NAME_VALUE = "default";

    private static final String MASTER_IP_VARIABLE = "masterIp";

    private static final String OOZIE_USE_SYSTEM_LIBPATH_VARIABLE = "oozie.use.system.libpath";
    private static final String OOZIE_USE_SYSTEM_LIBPATH_VALUE = "true";

    private static final String NAMENODE_VARIABLE = "nameNode";
    private static final String NAMENODE_VALUE = "hdfs://${masterIp}:9000";

    private static final String JOB_TRACKER_VARIABLE = "jobTracker";
    private static final String JOB_TRACKER_VALUE = "${masterIp}:9022";

    private static final String USER_NAME_VARIABLE = "user.name";
    private static final String USER_NAME_VALUE = "hadoop";

    private static final String OOZIE_WF_APPLICATION_PATH_VARIABLE = "oozie.wf.application.path";
    private static final String OOZIE_WF_APPLICATION_PATH_VALUE = "${nameNode}/user/hadoop/datamgmt/oozie_wrapper/";

    private static final String S3_HDFS_COPY_SCRIPT_VARIABLE = "s3_hdfs_copy_script";

    private static final String TEMP_FOLDER_SUFFIX_VARIABLE = "temp_folder_suffix";

    private static final String CLIENT_WORKFLOW_HDFS_LOCATION_VARIABLE = "client_workflow_hdfs_location";
    private static final String CLIENT_WORKFLOW_HDFS_LOCATION_VALUE = "/user/hadoop/datamgmt/${temp_folder_suffix}/";

    private static final String CLIENT_WORKFLOW_S3_LOCATION_VARIABLE = "client_workflow_s3_location";

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private OozieOperations oozieOperations;

    @Autowired
    private EmrHelper emrHelper;

    /**
     * {@inheritDoc}
     */
    @Override
    public String runOozieWorkflow(String masterIpAddress, String workflowLocation, List<Parameter> parameters) throws Exception
    {
        OozieClient oozieClient = getOozieClient(masterIpAddress);

        Properties conf = new Properties();

        // Add client properties.
        if (!CollectionUtils.isEmpty(parameters))
        {
            for (Parameter parameter : parameters)
            {
                conf.put(parameter.getName(), parameter.getValue());
            }
        }

        conf.put(MAP_REDUCE_QUEUE_NAME_VARIABLE, MAP_REDUCE_QUEUE_NAME_VALUE);
        conf.put(MASTER_IP_VARIABLE, masterIpAddress);
        conf.put(OOZIE_USE_SYSTEM_LIBPATH_VARIABLE, OOZIE_USE_SYSTEM_LIBPATH_VALUE);
        conf.put(NAMENODE_VARIABLE, NAMENODE_VALUE);
        conf.put(JOB_TRACKER_VARIABLE, JOB_TRACKER_VALUE);
        conf.put(USER_NAME_VARIABLE, USER_NAME_VALUE);

        // Properties needed for the DM wrapper workflow.
        conf.put(OOZIE_WF_APPLICATION_PATH_VARIABLE, OOZIE_WF_APPLICATION_PATH_VALUE);
        conf.put(S3_HDFS_COPY_SCRIPT_VARIABLE, emrHelper.getS3HdfsCopyScriptName());
        conf.put(TEMP_FOLDER_SUFFIX_VARIABLE, UUID.randomUUID().toString());
        conf.put(CLIENT_WORKFLOW_HDFS_LOCATION_VARIABLE, CLIENT_WORKFLOW_HDFS_LOCATION_VALUE);
        conf.put(CLIENT_WORKFLOW_S3_LOCATION_VARIABLE, workflowLocation);

        return oozieOperations.runOozieWorkflow(oozieClient, conf);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public WorkflowJob getEmrOozieWorkflow(String masterIpAddress, String emrOozieWorkflowId) throws OozieClientException
    {
        OozieClient oozieClient = getOozieClient(masterIpAddress);

        WorkflowJob workflowJob;
        try
        {
            workflowJob = oozieOperations.getJobInfo(oozieClient, emrOozieWorkflowId);
        }
        catch (OozieClientException oozieClientException)
        {
            /*
             * We will do our best effort to handle exceptions with known error codes.
             */
            if (OOZIE_ERROR_CODE_JOB_DOES_NOT_EXIST.equals(oozieClientException.getErrorCode()))
            {
                throw new ObjectNotFoundException("The oozie workflow with job ID '" + emrOozieWorkflowId + "' does not exist.", oozieClientException);
            }

            // If unhandlable, throw the exception as-is
            throw oozieClientException;
        }

        return workflowJob;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<WorkflowJob> getRunningEmrOozieJobsByName(String masterIpAddress, String appName, int start, int len) throws OozieClientException
    {
        OozieClient oozieClient = getOozieClient(masterIpAddress);
        
        String filter = "status=" + WorkflowJob.Status.RUNNING.toString();
        
        if(StringUtils.isNotEmpty(appName))
        {
            filter += ";name=" + appName;
        }
        
        return oozieOperations.getJobsInfo(oozieClient, filter, start, len);
    }
    
    /**
     * Builds the Oozie client for the given master IP address.
     * 
     * @param masterIpAddress the IP address of oozie master server.
     * @return the OozieClient.
     */
    private OozieClient getOozieClient(String masterIpAddress)
    {
        ConfigurationValue configurationValue = ConfigurationValue.EMR_OOZIE_URL_TEMPLATE;
        String oozieUrlTemplate = configurationHelper.getProperty(configurationValue);
        String oozieUrl = String.format(oozieUrlTemplate, masterIpAddress);
        return new OozieClient(oozieUrl);
    }
}
