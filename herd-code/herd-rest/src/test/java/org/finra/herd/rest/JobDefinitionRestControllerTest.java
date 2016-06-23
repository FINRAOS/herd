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
package org.finra.herd.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.bpmn.model.StartEvent;
import org.junit.Test;

import org.finra.herd.model.api.xml.JobDefinition;
import org.finra.herd.model.api.xml.JobDefinitionCreateRequest;
import org.finra.herd.model.api.xml.JobDefinitionUpdateRequest;

/**
 * This class tests various functionality within the job definition REST controller.
 */
public class JobDefinitionRestControllerTest extends AbstractRestTest
{
    @Test
    public void testCreateJobDefinition() throws Exception
    {
        String namespace = NAMESPACE;
        String jobName = JOB_NAME;
        String activitiJobXml = getActivitiJobXml(namespace, jobName);
        createNamespaceEntity(namespace);
        JobDefinition jobDefinition =
            jobDefinitionRestController.createJobDefinition(new JobDefinitionCreateRequest(namespace, jobName, null, activitiJobXml, null, null));
        assertNotNull(jobDefinition);
        assertEquals(namespace, jobDefinition.getNamespace());
        assertEquals(jobName, jobDefinition.getJobName());
    }

    @Test
    public void testGetJobDefinition() throws Exception
    {
        String namespace = NAMESPACE;
        String jobName = JOB_NAME;
        String activitiJobXml = getActivitiJobXml(namespace, jobName);
        createNamespaceEntity(namespace);
        jobDefinitionRestController.createJobDefinition(new JobDefinitionCreateRequest(namespace, jobName, null, activitiJobXml, null, null));
        JobDefinition jobDefinition = jobDefinitionRestController.getJobDefinition(namespace, jobName);
        assertNotNull(jobDefinition);
        assertEquals(namespace, jobDefinition.getNamespace());
        assertEquals(jobName, jobDefinition.getJobName());
    }

    @Test
    public void testUpdateJobDefinition() throws Exception
    {
        String namespace = NAMESPACE;
        String jobName = JOB_NAME;
        String activitiJobXml = getActivitiJobXml(namespace, jobName);
        createNamespaceEntity(namespace);
        jobDefinitionRestController.createJobDefinition(new JobDefinitionCreateRequest(namespace, jobName, null, activitiJobXml, null, null));

        JobDefinition jobDefinition =
            jobDefinitionRestController.updateJobDefinition(namespace, jobName, new JobDefinitionUpdateRequest(null, activitiJobXml, null, null));
        assertNotNull(jobDefinition);
        assertEquals(namespace, jobDefinition.getNamespace());
        assertEquals(jobName, jobDefinition.getJobName());
    }

    private String getActivitiJobXml(String namespace, String jobName)
    {
        BpmnModel bpmnModel = new BpmnModel();
        Process process = new Process();
        process.setId(namespace + '.' + jobName);
        {
            StartEvent element = new StartEvent();
            element.setId("start");
            process.addFlowElement(element);
        }
        {
            EndEvent element = new EndEvent();
            element.setId("end");
            process.addFlowElement(element);
        }
        process.addFlowElement(new SequenceFlow("start", "end"));
        bpmnModel.addProcess(process);
        return getActivitiXmlFromBpmnModel(bpmnModel);
    }
}
