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

import org.finra.herd.dao.EmrClusterDefinitionDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;

/**
 * Helper for EMR cluster definition related operations which require DAO.
 */
@Component
public class EmrClusterDefinitionDaoHelper
{
    @Autowired
    private EmrClusterDefinitionDao emrClusterDefinitionDao;

    /**
     * Gets an EMR cluster definition entity based on the key and makes sure that it exists.
     *
     * @param emrClusterDefinitionKey the EMR cluster definition key
     *
     * @return the EMR cluster definition entity
     * @throws ObjectNotFoundException if the EMR cluster definition entity doesn't exist
     */
    public EmrClusterDefinitionEntity getEmrClusterDefinitionEntity(EmrClusterDefinitionKey emrClusterDefinitionKey) throws ObjectNotFoundException
    {
        return getEmrClusterDefinitionEntity(emrClusterDefinitionKey.getNamespace(), emrClusterDefinitionKey.getEmrClusterDefinitionName());
    }

    /**
     * Gets an EMR cluster definition entity based on the namespace and cluster definition name and makes sure that it exists.
     *
     * @param namespace the namespace
     * @param clusterDefinitionName the cluster definition name
     *
     * @return the EMR cluster definition entity
     * @throws ObjectNotFoundException if the EMR cluster definition entity doesn't exist
     */
    public EmrClusterDefinitionEntity getEmrClusterDefinitionEntity(String namespace, String clusterDefinitionName) throws ObjectNotFoundException
    {
        // Get the EMR cluster definition and ensure it exists.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = emrClusterDefinitionDao.getEmrClusterDefinitionByAltKey(namespace, clusterDefinitionName);
        if (emrClusterDefinitionEntity == null)
        {
            throw new ObjectNotFoundException("EMR cluster definition with name \"" + clusterDefinitionName + "\" doesn't exist for namespace \"" +
                namespace + "\".");
        }
        return emrClusterDefinitionEntity;
    }
}
