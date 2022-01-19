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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import org.finra.herd.service.activiti.ActivitiHelper;
import org.finra.herd.service.helper.EmrStepHelper;
import org.finra.herd.service.helper.EmrStepHelperFactory;

public class BaseAddEmrStepTest
{
    @InjectMocks
    private BaseAddEmrStep baseAddEmrStep;

    @Mock
    private EmrStepHelperFactory emrStepHelperFactory;

    @Mock
    private ActivitiHelper activitiHelper;

    @Mock
    private Expression namespace;

    @Mock
    private Expression emrClusterDefinitionName;

    @Mock
    private Expression emrClusterName;

    @Mock
    private Expression stepName;

    @Mock
    private Expression continueOnError;

    @Mock
    private Expression scriptLocation;

    @Mock
    private Expression scriptArguments;

    @Mock
    private Expression emrClusterId;

    @Before
    public void before()
    {
        baseAddEmrStep = new BaseAddEmrStep()
        {
            @Override
            public void executeImpl(DelegateExecution execution) throws Exception
            {

            }
        };
        initMocks(this);
    }

    @Test
    public void populateCommonParamsAssertEmrStepHelperPopulatedCorrectly()
    {
        Object request = "request";
        DelegateExecution execution = mock(DelegateExecution.class);

        EmrStepHelper emrStepHelper = mock(EmrStepHelper.class);
        when(emrStepHelperFactory.getStepHelper(any())).thenReturn(emrStepHelper);
        String namespaceString = "namespaceString";
        when(activitiHelper.getExpressionVariableAsString(same(namespace), any())).thenReturn(namespaceString);
        String stepNameString = "stepNameString";
        when(activitiHelper.getExpressionVariableAsString(same(stepName), any())).thenReturn(stepNameString);
        Boolean continueOnErrorBoolean = false;
        when(activitiHelper.getExpressionVariableAsBoolean(same(continueOnError), any(), any(), anyBoolean(), any())).thenReturn(continueOnErrorBoolean);
        String emrClusterDefinitionNameString = "emrClusterDefinitionNameString";
        when(activitiHelper.getExpressionVariableAsString(same(emrClusterDefinitionName), any())).thenReturn(emrClusterDefinitionNameString);
        String emrClusterNameString = "emrClusterNameString";
        when(activitiHelper.getExpressionVariableAsString(same(emrClusterName), any())).thenReturn(emrClusterNameString);
        String emrClusterIdString = "emrClusterIdString";
        when(activitiHelper.getExpressionVariableAsString(same(emrClusterId), any())).thenReturn(emrClusterIdString);

        baseAddEmrStep.populateCommonParams(request, execution);

        verify(emrStepHelper).setRequestStepName(request, stepNameString);
        verify(emrStepHelper).setRequestContinueOnError(request, continueOnErrorBoolean);
        verify(emrStepHelper).setRequestNamespace(request, namespaceString);
        verify(emrStepHelper).setRequestEmrClusterDefinitionName(request, emrClusterDefinitionNameString);
        verify(emrStepHelper).setRequestEmrClusterName(request, emrClusterNameString);
        verify(emrStepHelper).setRequestEmrClusterId(request, emrClusterIdString);
    }
}
