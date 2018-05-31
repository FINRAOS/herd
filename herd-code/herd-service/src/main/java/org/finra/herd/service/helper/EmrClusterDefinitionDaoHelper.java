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

import java.util.List;

import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.EmrClusterDefinitionDao;
import org.finra.herd.dao.NamespaceDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;

/**
 * Helper for EMR cluster definition related operations which require DAO.
 */
@Component
public class EmrClusterDefinitionDaoHelper
{
    @Autowired
    private EmrClusterDefinitionDao emrClusterDefinitionDao;

    @Autowired
    private NamespaceDao namespaceDao;

    /**
     * Retrieves an EMR cluster definition entity based on the EMR cluster definition key and makes sure that it exists.
     *
     * @param emrClusterDefinitionKey the EMR cluster definition key
     *
     * @return the EMR cluster definition entity
     * @throws ObjectNotFoundException if EMR cluster definition entity doesn't exist
     */
    public EmrClusterDefinitionEntity getEmrClusterDefinitionEntity(EmrClusterDefinitionKey emrClusterDefinitionKey) throws ObjectNotFoundException
    {
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = null;

        // Try to retrieve the relative namespace entity.
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(emrClusterDefinitionKey.getNamespace());

        // If namespace entity exists, try to retrieve EMR cluster definition entity by namespace entity and EMR cluster definition name.
        if (namespaceEntity != null)
        {
            emrClusterDefinitionEntity =
                emrClusterDefinitionDao.getEmrClusterDefinitionByNamespaceAndName(namespaceEntity, emrClusterDefinitionKey.getEmrClusterDefinitionName());
        }

        // Throw an exception if EMR cluster definition entity does not exist.
        if (emrClusterDefinitionEntity == null)
        {
            throw new ObjectNotFoundException(String
                .format("EMR cluster definition with name \"%s\" doesn't exist for namespace \"%s\".", emrClusterDefinitionKey.getEmrClusterDefinitionName(),
                    emrClusterDefinitionKey.getNamespace()));
        }

        return emrClusterDefinitionEntity;
    }

    /**
     * Gets a list of keys for all EMR cluster definitions defined in the system for the specified namespace.
     *
     * @param namespace the namespace (case-insensitive)
     *
     * @return the list of EMR cluster definition keys
     */
    public List<EmrClusterDefinitionKey> getEmrClusterDefinitionKeys(String namespace)
    {
        // Try to retrieve the relative namespace entity.
        NamespaceEntity namespaceEntity = namespaceDao.getNamespaceByCd(namespace);

        // If namespace entity exists, retrieve EMR cluster definition keys by namespace entity.
        if (namespaceEntity != null)
        {
            return emrClusterDefinitionDao.getEmrClusterDefinitionKeysByNamespace(namespaceEntity);
        }
        else
        {
            return Lists.newArrayList();
        }
    }
}
