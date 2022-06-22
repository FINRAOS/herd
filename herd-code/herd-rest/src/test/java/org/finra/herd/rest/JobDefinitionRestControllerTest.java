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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.bpmn.model.StartEvent;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.JobDefinition;
import org.finra.herd.model.api.xml.JobDefinitionCreateRequest;
import org.finra.herd.model.api.xml.JobDefinitionKey;
import org.finra.herd.model.api.xml.JobDefinitionKeys;
import org.finra.herd.model.api.xml.JobDefinitionUpdateRequest;
import org.finra.herd.service.JobDefinitionService;

/**
 * This class tests various functionality within the job definition REST controller.
 */
public class JobDefinitionRestControllerTest extends AbstractRestTest
{
    @InjectMocks
    private JobDefinitionRestController jobDefinitionRestController;

    @Mock
    private JobDefinitionService jobDefinitionService;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateJobDefinition() throws Exception
    {
        String namespace = NAMESPACE;
        String jobName = JOB_NAME;
        String activitiJobXml = getActivitiJobXml(namespace, jobName);
        JobDefinitionCreateRequest request = new JobDefinitionCreateRequest(namespace, jobName, null, activitiJobXml, null, null);

        JobDefinition jobDefinition = getJobDefinition(NAMESPACE, JOB_NAME);

        when(jobDefinitionService.createJobDefinition(request, true)).thenReturn(jobDefinition);

        JobDefinition resultJobDefinition =
            jobDefinitionRestController.createJobDefinition(new JobDefinitionCreateRequest(namespace, jobName, null, activitiJobXml, null, null));

        // Verify the external calls.
        verify(jobDefinitionService).createJobDefinition(request, true);
        verifyNoMoreInteractions(jobDefinitionService);
        // Validate the returned object.
        assertEquals(jobDefinition, resultJobDefinition);
    }

    @Test
    public void testGetJobDefinition() throws Exception
    {
        String namespace = NAMESPACE;
        String jobName = JOB_NAME;
        JobDefinition jobDefinition = getJobDefinition(NAMESPACE, JOB_NAME);
        when(jobDefinitionService.getJobDefinition(namespace, jobName)).thenReturn(jobDefinition);

        JobDefinition resultJobDefinition = jobDefinitionRestController.getJobDefinition(namespace, jobName);
        // Verify the external calls.
        verify(jobDefinitionService).getJobDefinition(namespace, jobName);
        verifyNoMoreInteractions(jobDefinitionService);
        // Validate the returned object.
        assertEquals(jobDefinition, resultJobDefinition);
    }

    @Test
    public void testUpdateJobDefinition() throws Exception
    {
        String namespace = NAMESPACE;
        String jobName = JOB_NAME;
        String activitiJobXml = getActivitiJobXml(NAMESPACE, JOB_NAME);
        JobDefinitionUpdateRequest request = new JobDefinitionUpdateRequest(null, activitiJobXml, null, null);
        JobDefinition jobDefinition = getJobDefinition(NAMESPACE, JOB_NAME);

        when(jobDefinitionService.updateJobDefinition(namespace, jobName, request, true)).thenReturn(jobDefinition);
        JobDefinition resultJobDefinition =
            jobDefinitionRestController.updateJobDefinition(namespace, jobName, new JobDefinitionUpdateRequest(null, activitiJobXml, null, null));
        // Verify the external calls.
        verify(jobDefinitionService).updateJobDefinition(namespace, jobName, request, true);
        verifyNoMoreInteractions(jobDefinitionService);
        // Validate the returned object.
        assertEquals(jobDefinition, resultJobDefinition);
    }

    @Test
    public void testGetJobDefinitionKeys()
    {
        // Create an job definition keys.
        JobDefinitionKeys jobDefinitionKeys = new JobDefinitionKeys(Arrays.asList(new JobDefinitionKey(NAMESPACE, JOB_NAME)));

        // Mock the external calls.
        when(jobDefinitionService.getJobDefinitionKeys(NAMESPACE)).thenReturn(jobDefinitionKeys);

        // Call the method under test.
        JobDefinitionKeys result = jobDefinitionRestController.getJobDefinitionKeys(NAMESPACE);

        // Validate the results.
        assertEquals(jobDefinitionKeys, result);

        // Verify the external calls.
        verify(jobDefinitionService).getJobDefinitionKeys(NAMESPACE);
        verifyNoMoreInteractions(jobDefinitionService);
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

    private JobDefinition getJobDefinition(String namespace, String jobName)
    {
        String activitiJobXml = getActivitiJobXml(namespace, jobName);
        JobDefinition jobDefinition = new JobDefinition();
        jobDefinition.setActivitiJobXml(activitiJobXml);
        jobDefinition.setJobName(JOB_NAME);
        jobDefinition.setJobName(NAMESPACE);

        return jobDefinition;
    }
}
