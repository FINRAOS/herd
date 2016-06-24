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

import java.util.Collection;
import java.util.List;

import org.finra.herd.model.jpa.JobDefinitionEntity;

public interface JobDefinitionDao extends BaseJpaDao
{
    /**
     * Gets a job definition entity by an alternate key.
     *
     * @param namespace the namespace (case-insensitive)
     * @param jobName the job name (case-insensitive)
     *
     * @return the job definition entity
     */
    public JobDefinitionEntity getJobDefinitionByAltKey(String namespace, String jobName);

    /**
     * Gets a job definition entity by its process definition ID.
     *
     * @param processDefinitionId the process definition ID
     *
     * @return the job definition entity
     */
    public JobDefinitionEntity getJobDefinitionByProcessDefinitionId(String processDefinitionId);

    /**
     * Gets a list of job definitions by optional filter criteria.
     *
     * @param namespace the namespace, may be null (case-insensitive)
     * @param jobName the job name, may be null (case-insensitive)
     *
     * @return the list of job definitions
     */
    public List<JobDefinitionEntity> getJobDefinitionsByFilter(String namespace, String jobName);

    /**
     * Gets a list of job definitions by optional filter criteria.
     *
     * @param namespaces the collection of namespaces, may be empty or null
     * @param jobName the job name, may be null (case-insensitive)
     *
     * @return the list of job definition entities
     */
    public List<JobDefinitionEntity> getJobDefinitionsByFilter(Collection<String> namespaces, String jobName);
}
