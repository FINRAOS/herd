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

import org.finra.herd.dao.PartitionKeyGroupDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.PartitionKeyGroupKey;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;

/**
 * Helper for partition key group related operations which require DAO.
 */
@Component
public class PartitionKeyGroupDaoHelper
{
    @Autowired
    private PartitionKeyGroupDao partitionKeyGroupDao;

    /**
     * Gets the partition key group entity and ensure it exists.
     *
     * @param partitionKeyGroupKey the partition key group key
     *
     * @return the partition key group entity
     * @throws ObjectNotFoundException if the partition key group entity doesn't exist
     */
    public PartitionKeyGroupEntity getPartitionKeyGroupEntity(PartitionKeyGroupKey partitionKeyGroupKey) throws ObjectNotFoundException
    {
        return getPartitionKeyGroupEntity(partitionKeyGroupKey.getPartitionKeyGroupName());
    }

    /**
     * Gets the partition key group entity and ensure it exists.
     *
     * @param partitionKeyGroupName the partition key group name (case insensitive)
     *
     * @return the partition key group entity
     * @throws ObjectNotFoundException if the partition key group entity doesn't exist
     */
    public PartitionKeyGroupEntity getPartitionKeyGroupEntity(String partitionKeyGroupName) throws ObjectNotFoundException
    {
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDao.getPartitionKeyGroupByName(partitionKeyGroupName);

        if (partitionKeyGroupEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Partition key group \"%s\" doesn't exist.", partitionKeyGroupName));
        }

        return partitionKeyGroupEntity;
    }
}
