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
package org.finra.herd.service.helper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.JobDefinitionDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.jpa.JobDefinitionEntity;

/**
 * Helper for data provider related operations which require DAO.
 */
@Component
public class JobDefinitionDaoHelper
{
    @Autowired
    private JobDefinitionDao jobDefinitionDao;

    /**
     * Retrieve and ensures that a job definition entity exists.
     *
     * @param namespace the namespace (case insensitive)
     * @param jobName the job name (case insensitive)
     *
     * @return the job definition entity
     * @throws ObjectNotFoundException if the storage entity doesn't exist
     */
    public JobDefinitionEntity getJobDefinitionEntity(String namespace, String jobName) throws ObjectNotFoundException
    {
        JobDefinitionEntity jobDefinitionEntity = jobDefinitionDao.getJobDefinitionByAltKey(namespace, jobName);

        if (jobDefinitionEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Job definition with namespace \"%s\" and job name \"%s\" doesn't exist.", namespace, jobName));
        }

        return jobDefinitionEntity;
    }
}
