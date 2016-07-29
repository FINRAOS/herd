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

import org.finra.herd.model.jpa.PartitionKeyGroupEntity;

@Component
public class PartitionKeyGroupDaoTestHelper
{
    @Autowired
    private PartitionKeyGroupDao partitionKeyGroupDao;

    /**
     * Creates and persists a new partition key group entity.
     *
     * @param partitionKeyGroupName the name of the partition key group
     *
     * @return the newly created partition key group entity.
     */
    public PartitionKeyGroupEntity createPartitionKeyGroupEntity(String partitionKeyGroupName)
    {
        PartitionKeyGroupEntity partitionKeyGroupEntity = new PartitionKeyGroupEntity();
        partitionKeyGroupEntity.setPartitionKeyGroupName(partitionKeyGroupName);
        return partitionKeyGroupDao.saveAndRefresh(partitionKeyGroupEntity);
    }
}
