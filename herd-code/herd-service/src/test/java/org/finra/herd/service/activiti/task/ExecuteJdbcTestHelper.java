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

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.jpa.JobDefinitionEntity;

@Component
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class ExecuteJdbcTestHelper extends HerdActivitiServiceTaskTest
{
    /**
     * Cleans up Herd database after ExecuteJdbcWithReceiveTask test by deleting the test job definition entity.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void cleanUpHerdDatabaseAfterExecuteJdbcWithReceiveTaskTest()
    {
        // Retrieve the test job definition entity.
        JobDefinitionEntity jobDefinitionEntity = jobDefinitionDao.getJobDefinitionByAltKey(TEST_ACTIVITI_NAMESPACE_CD, TEST_ACTIVITI_JOB_NAME);

        // Delete the job definition.
        if (jobDefinitionEntity != null)
        {
            jobDefinitionDao.delete(jobDefinitionEntity);
        }
    }

    /**
     * Prepares Herd database for ExecuteJdbcWithReceiveTask test by creating and persisting a test job definition entity.
     *
     * @throws Exception
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void prepareHerdDatabaseForExecuteJdbcWithReceiveTaskTest() throws Exception
    {
        // Create a test job definition.
        createJobDefinition("classpath:org/finra/herd/service/testActivitiWorkflowExecuteJdbcTaskWithReceiveTask.bpmn20.xml");
    }
}
