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
package org.finra.herd.service.activiti;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.ScriptTask;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.bpmn.model.StartEvent;
import org.activiti.engine.ActivitiException;
import org.junit.Test;

import org.finra.herd.service.activiti.task.HerdActivitiServiceTaskTest;

public class ActivitiScriptTaskTest extends HerdActivitiServiceTaskTest
{

    @Test
    public void testActivitiScriptTaskHappyPathExecution()
    {
        String script = "execution.setVariable('testKey', 'testValue'); var allVariables = execution.getVariables();";
        createScriptTaskJobAndVerifySuccess(script, null);
    }

    @Test
    // test javascript built-int object JSON and Date
    public void testActivitiScriptTaskHappyPathJsBuiltInObject()
    {
        String script = "JSON.parse('{\"testKey\": \"testValue\"}'); var a = Date();";
        createScriptTaskJobAndVerifySuccess(script, null);
    }

    @Test
    // test javascript built-in Map and Array
    public void testActivitiScriptTaskHappyPathJsBuiltInObject2()
    {
        String script = "var message = {}; message.errors = []; message.errors.push(\"a\"); ";
        createScriptTaskJobAndVerifySuccess(script, null);
    }

    @Test
    public void testActivitiScriptTaskHappyPathWithJsScriptFormat()
    {
        String script = "execution.getVariables();";
        createScriptTaskJobAndVerifySuccess(script, "js");
    }

    @Test
    public void testActivitiScriptTaskWithProhibitedJavaClassUsingReflection()
    {
        String script = ".class.class.forName('java.lang.Runtime').methods[6].invoke(null).exec('pwd');";
        createScriptTaskJobAndVerifyException(script, null, new ActivitiException(
            "Problem evaluating script: <eval>:1:0 Expected an operand but found ." + System.lineSeparator() +
                ".class.class.forName('java.lang.Runtime').methods[6].invoke(null).exec('pwd');" + System.lineSeparator() +
                "^ in <eval> at line number 1 at column number 0"));
    }

    @Test
    public void testActivitiScriptTaskWithProhibitedJavaClassWithJsScriptFormat()
    {
        createScriptTaskJobAndVerifyException("var parser = new org.springframework.expression.spel.standard.SpelExpressionParser();", "js",
            new RuntimeException("java.lang.ClassNotFoundException: org.springframework.expression.spel.standard.SpelExpressionParser"));
    }

    @Test
    public void testActivitiScriptTaskWithProhibitedJavaClassMethod()
    {
        createScriptTaskJobAndVerifyException("java.lang.Runtime.getRuntime().exec(\"echo hello\");", null,
            new RuntimeException("java.lang.ClassNotFoundException: java.lang.Runtime.getRuntime"));
    }

    private void createScriptTaskJobAndVerifySuccess(String javascript, String scriptFormat)
    {
        buildActivitiXmlWithScriptTask(javascript, scriptFormat == null ? "javascript" : scriptFormat);
    }

    private void createScriptTaskJobAndVerifyException(String javascript, String scriptFormat, Exception expectedException)
    {
        try
        {
            String activitiXml = buildActivitiXmlWithScriptTask(javascript, scriptFormat == null ? "javascript" : scriptFormat);
            jobServiceTestHelper.createJobFromActivitiXml(activitiXml, new ArrayList<>());
        }
        catch (Exception e)
        {
            assertEquals(expectedException.getClass(), e.getClass());
            assertEquals(expectedException.getMessage(), e.getMessage());
        }
    }

    private String buildActivitiXmlWithScriptTask(String javascript, String scriptFormat)
    {
        // Create a workflow
        BpmnModel bpmnModel = new BpmnModel();
        Process process = new Process();
        process.setId("testNamespace.testHerdWorkflow");

        // Set StartEvent
        StartEvent startEvent = new StartEvent();
        startEvent.setId("start");
        process.addFlowElement(startEvent);

        // Set ScriptTask
        ScriptTask scriptTask = new ScriptTask();
        scriptTask.setId("testScriptTask");
        scriptTask.setScriptFormat(scriptFormat);
        scriptTask.setScript(javascript);
        process.addFlowElement(scriptTask);

        // Set EndEvent
        EndEvent endEvent = new EndEvent();
        endEvent.setId("end");
        process.addFlowElement(endEvent);

        process.addFlowElement(new SequenceFlow(startEvent.getId(), scriptTask.getId()));
        process.addFlowElement(new SequenceFlow(scriptTask.getId(), endEvent.getId()));

        bpmnModel.addProcess(process);

        return getActivitiXmlFromBpmnModel(bpmnModel);
    }
}