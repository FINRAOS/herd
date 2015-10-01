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
package org.finra.dm.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import org.finra.dm.model.dto.EmrClusterAlternateKeyDto;
import org.finra.dm.model.dto.SecurityFunctions;
import org.finra.dm.model.api.xml.EmrCluster;
import org.finra.dm.model.api.xml.EmrClusterCreateRequest;
import org.finra.dm.model.api.xml.EmrHadoopJarStep;
import org.finra.dm.model.api.xml.EmrHadoopJarStepAddRequest;
import org.finra.dm.model.api.xml.EmrHiveStep;
import org.finra.dm.model.api.xml.EmrHiveStepAddRequest;
import org.finra.dm.model.api.xml.EmrMasterSecurityGroup;
import org.finra.dm.model.api.xml.EmrMasterSecurityGroupAddRequest;
import org.finra.dm.model.api.xml.EmrOozieStep;
import org.finra.dm.model.api.xml.EmrOozieStepAddRequest;
import org.finra.dm.model.api.xml.EmrPigStep;
import org.finra.dm.model.api.xml.EmrPigStepAddRequest;
import org.finra.dm.model.api.xml.EmrShellStep;
import org.finra.dm.model.api.xml.EmrShellStepAddRequest;
import org.finra.dm.model.api.xml.OozieWorkflowJob;
import org.finra.dm.model.api.xml.RunOozieWorkflowRequest;
import org.finra.dm.service.EmrService;
import org.finra.dm.ui.constants.UiConstants;

/**
 * The REST controller that handles EMR REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
public class EmrRestController extends DmBaseController
{
    @Autowired
    private EmrService emrService;

    /**
     * Gets an existing EMR cluster details.
     *
     * @param namespace the namespace
     * @param emrClusterDefinitionName the EMR cluster definition name
     * @param emrClusterName the EMR cluster name
     * @param emrClusterId the cluster id of the cluster to get details
     * @param emrStepId the step id of the step to get details
     * @param verbose parameter for whether to return detailed information
     * @param retrieveOozieJobs parameter for whether to retrieve oozie job information
     *
     * @return the EMR Cluster object with details.
     * @throws Exception if there was an error getting the EMR cluster.
     */
    @RequestMapping(value = "/emrClusters/namespaces/{namespace}/emrClusterDefinitionNames/{emrClusterDefinitionName}/emrClusterNames/{emrClusterName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_EMR_CLUSTERS_GET)
    public EmrCluster getEmrCluster(@PathVariable("namespace") String namespace, @PathVariable("emrClusterDefinitionName") String emrClusterDefinitionName,
        @PathVariable("emrClusterName") String emrClusterName, @RequestParam(value = "emrClusterId", required = false) String emrClusterId,
        @RequestParam(value = "emrStepId", required = false) String emrStepId,
        @RequestParam(value = "verbose", required = false, defaultValue = "false") Boolean verbose,
        @RequestParam(value = "retrieveOozieJobs", required = false, defaultValue = "false") Boolean retrieveOozieJobs) throws Exception
    {
        EmrClusterAlternateKeyDto alternateKey =
            EmrClusterAlternateKeyDto.builder().namespace(namespace).emrClusterDefinitionName(emrClusterDefinitionName).emrClusterName(emrClusterName).build();

        return emrService.getCluster(alternateKey, emrClusterId, emrStepId, verbose, retrieveOozieJobs);
    }

    /**
     * Creates a new EMR cluster.
     *
     * @param request the information needed to create the EMR cluster.
     *
     * @return the created EMR cluster.
     */
    @RequestMapping(value = "/emrClusters", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_EMR_CLUSTERS_POST)
    public EmrCluster createEmrCluster(@RequestBody EmrClusterCreateRequest request) throws Exception
    {
        return emrService.createCluster(request);
    }

    /**
     * Terminates an existing EMR cluster.
     *
     * @param namespace the namespace
     * @param emrClusterDefinitionName the EMR cluster definition name
     * @param emrClusterName the EMR cluster name
     * @param overrideTerminationProtection parameter for whether to override termination protection
     *
     * @return the EMR cluster that was terminated
     * @throws Exception if there was an error terminating the EMR cluster.
     */
    @RequestMapping(value = "/emrClusters/namespaces/{namespace}/emrClusterDefinitionNames/{emrClusterDefinitionName}/emrClusterNames/{emrClusterName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_EMR_CLUSTERS_DELETE)
    public EmrCluster terminateEmrCluster(@PathVariable("namespace") String namespace,
        @PathVariable("emrClusterDefinitionName") String emrClusterDefinitionName, @PathVariable("emrClusterName") String emrClusterName,
        @RequestParam(value = "overrideTerminationProtection", required = false, defaultValue = "false") Boolean overrideTerminationProtection) throws Exception
    {
        EmrClusterAlternateKeyDto alternateKey =
            EmrClusterAlternateKeyDto.builder().namespace(namespace).emrClusterDefinitionName(emrClusterDefinitionName).emrClusterName(emrClusterName).build();

        return emrService.terminateCluster(alternateKey, overrideTerminationProtection);
    }

    /**
     * Adds a shell step to the existing cluster
     *
     * @param request the information needed to add shell step to the EMR cluster.
     *
     * @return the created EMR shell step.
     * @throws Exception if a shell step couldn't be added to the EMR cluster.
     */
    @RequestMapping(value = "/emrShellSteps", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_EMR_SHELL_STEPS_POST)
    public EmrShellStep addShellStepToEmrCluster(@RequestBody EmrShellStepAddRequest request) throws Exception
    {
        return (EmrShellStep) emrService.addStepToCluster(request);
    }

    /**
     * Adds a hive step to the existing cluster
     *
     * @param request the information needed to add hive step to the EMR cluster.
     *
     * @return the created EMR hive step.
     */
    @RequestMapping(value = "/emrHiveSteps", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_EMR_HIVE_STEPS_POST)
    public EmrHiveStep addHiveStepToEmrCluster(@RequestBody EmrHiveStepAddRequest request) throws Exception
    {
        return (EmrHiveStep) emrService.addStepToCluster(request);
    }

    /**
     * Adds a Pig step to the existing cluster
     *
     * @param request the information needed to add Pig step to the EMR cluster.
     *
     * @return the created EMR Pig step.
     */
    @RequestMapping(value = "/emrPigSteps", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_EMR_PIG_STEPS_POST)
    public EmrPigStep addPigStepToEmrCluster(@RequestBody EmrPigStepAddRequest request) throws Exception
    {
        return (EmrPigStep) emrService.addStepToCluster(request);
    }

    /**
     * Adds a oozie step to the existing cluster
     *
     * @param request the information needed to add oozie step to the EMR cluster.
     *
     * @return the created EMR oozie step.
     * @throws Exception if an Oozie step couldn't be added to an EMR cluster.
     */
    @RequestMapping(value = "/emrOozieSteps", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_EMR_OOZIE_STEPS_POST)
    public EmrOozieStep addOozieStepToEmrCluster(@RequestBody EmrOozieStepAddRequest request) throws Exception
    {
        return (EmrOozieStep) emrService.addStepToCluster(request);
    }

    /**
     * Adds a Hadoop Jar step to the existing cluster
     *
     * @param request the information needed to add Hadoop Jar step to the EMR cluster.
     *
     * @return the created EMR Hadoop Jar step.
     */
    @RequestMapping(value = "/emrHadoopJarSteps", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_EMR_HADOOP_JAR_STEPS_POST)
    public EmrHadoopJarStep addHadoopJarStepToEmrCluster(@RequestBody EmrHadoopJarStepAddRequest request) throws Exception
    {
        return (EmrHadoopJarStep) emrService.addStepToCluster(request);
    }

    /**
     * Adds security groups to the master node of an existing cluster
     *
     * @param request the information needed to add security groups to master node of the EMR cluster.
     *
     * @return the created EMR master groups.
     */
    @RequestMapping(value = "/emrMasterSecurityGroups", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_EMR_MASTER_SECURITY_GROUPS_POST)
    public EmrMasterSecurityGroup addGroupsToEmrClusterMaster(@RequestBody EmrMasterSecurityGroupAddRequest request) throws Exception
    {
        return emrService.addSecurityGroupsToClusterMaster(request);
    }
    
    /**
     * Submits an oozie workflow to the existing cluster.
     *
     * @param request the information needed to run oozie workflow to the EMR cluster.
     *
     * @return the oozie workflow job that was submitted.
     * @throws Exception if an Oozie step couldn't be added to an EMR cluster.
     */
    @RequestMapping(value = "/emrOozieWorkflows", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_EMR_OOZIE_STEPS_POST)
    public OozieWorkflowJob runOozieJobToEmrCluster(@RequestBody RunOozieWorkflowRequest request) throws Exception
    {
        return emrService.runOozieWorkflow(request);
    }

    /**
     * Retrieves an existing Oozie workflow job for the specified EMR cluster and workflow job ID.
     * 
     * @param namespace Namespace of the EMR cluster
     * @param emrClusterDefinitionName EMR cluster definition name
     * @param emrClusterName EMR cluster name
     * @param oozieWorkflowJobId Oozie workflow job ID
     * @param verbose true to return more detailed information, false or null otherwise
     * @return {@link OozieWorkflowJob} details
     * @throws Exception when errors occur, whether user or system
     */
    @RequestMapping(value = "/emrOozieWorkflows/namespaces/{namespace}/emrClusterDefinitionNames/{emrClusterDefinitionName}"
        + "/emrClusterNames/{emrClusterName}/oozieWorkflowJobIds/{oozieWorkflowJobId}", method = RequestMethod.GET)
    @ResponseBody
    @Secured(SecurityFunctions.FN_EMR_OOZIE_WORKFLOW_GET)
    public OozieWorkflowJob getEmrOozieWorkflow(@PathVariable("namespace") String namespace,
        @PathVariable("emrClusterDefinitionName") String emrClusterDefinitionName, @PathVariable("emrClusterName") String emrClusterName,
        @PathVariable("oozieWorkflowJobId") String oozieWorkflowJobId, 
        @RequestParam(value = "verbose", required = false, defaultValue = "false") Boolean verbose) throws Exception
    {
        return emrService.getEmrOozieWorkflowJob(namespace, emrClusterDefinitionName, emrClusterName, oozieWorkflowJobId, verbose);
    }
}