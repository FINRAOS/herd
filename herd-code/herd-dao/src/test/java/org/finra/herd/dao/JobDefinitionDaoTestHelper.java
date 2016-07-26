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
package org.finra.herd.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;

@Component
public class JobDefinitionDaoTestHelper
{
    @Autowired
    private JobDefinitionDao jobDefinitionDao;

    @Autowired
    private NamespaceDao namespaceDao;

    @Autowired
    private NamespaceDaoTestHelper namespaceDaoTestHelper;

    /**
     * Creates and persists a new job definition entity.
     *
     * @param namespaceCode the namespace code
     * @param jobName the job name
     * @param description the job definition description
     * @param activitiId the job definition Activiti ID
     *
     * @return the newly created job definition entity
     */
    public JobDefinitionEntity createJobDefinitionEntity(String namespaceCode, String jobName, String description, String activitiId)
    {
        // Create a namespace entity if needed.
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(namespaceCode);
        if (namespaceEntity == null)
        {
            namespaceEntity = namespaceDaoTestHelper.createNamespaceEntity(namespaceCode);
        }

        return createJobDefinitionEntity(namespaceEntity, jobName, description, activitiId);
    }

    /**
     * Creates and persists a new job definition entity.
     *
     * @param namespaceEntity the namespace entity
     * @param jobName the job name
     * @param description the job definition description
     * @param activitiId the job definition Activiti ID
     *
     * @return the newly created job definition entity
     */
    public JobDefinitionEntity createJobDefinitionEntity(NamespaceEntity namespaceEntity, String jobName, String description, String activitiId)
    {
        JobDefinitionEntity jobDefinitionEntity = new JobDefinitionEntity();
        jobDefinitionEntity.setNamespace(namespaceEntity);
        jobDefinitionEntity.setName(jobName);
        jobDefinitionEntity.setDescription(description);
        jobDefinitionEntity.setActivitiId(activitiId);
        return jobDefinitionDao.saveAndRefresh(jobDefinitionEntity);
    }
}
