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
package org.finra.herd.rest;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.finra.herd.model.api.xml.PartitionKeyGroup;
import org.finra.herd.model.api.xml.PartitionKeyGroupKeys;

/**
 * This class tests various functionality within the partition key group REST controller.
 */
public class PartitionKeyGroupRestControllerTest extends AbstractRestTest
{
    @Test
    public void testCreatePartitionKeyGroup()
    {
        // Create a partition key group.
        PartitionKeyGroup resultPartitionKeyGroup =
            partitionKeyGroupRestController.createPartitionKeyGroup(createPartitionKeyGroupCreateRequest(PARTITION_KEY_GROUP));

        // Validate the returned object.
        validatePartitionKeyGroup(PARTITION_KEY_GROUP, resultPartitionKeyGroup);
    }

    @Test
    public void testGetPartitionKeyGroup()
    {
        // Create and persist a partition key group entity.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Retrieve the partition key group.
        PartitionKeyGroup resultPartitionKeyGroup = partitionKeyGroupRestController.getPartitionKeyGroup(PARTITION_KEY_GROUP);

        // Validate the returned object.
        validatePartitionKeyGroup(PARTITION_KEY_GROUP, resultPartitionKeyGroup);
    }

    @Test
    public void testDeletePartitionKeyGroup()
    {
        // Create and persist a partition key group entity.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Validate that this partition key group exists.
        partitionKeyGroupRestController.getPartitionKeyGroup(PARTITION_KEY_GROUP);

        // Delete this partition key group.
        PartitionKeyGroup deletedPartitionKeyGroup = partitionKeyGroupRestController.deletePartitionKeyGroup(PARTITION_KEY_GROUP);

        // Validate the returned object.
        validatePartitionKeyGroup(PARTITION_KEY_GROUP, deletedPartitionKeyGroup);

        // Ensure that this partition key group is no longer there.
        assertNull(partitionKeyGroupDao.getPartitionKeyGroupByKey(createPartitionKeyGroupKey(PARTITION_KEY_GROUP)));
    }

    @Test
    public void testGetPartitionKeyGroups()
    {
        // Create and persist two partition key group entities.
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);
        createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Get the list of partition key groups.
        PartitionKeyGroupKeys partitionKeyGroupKeys = partitionKeyGroupRestController.getPartitionKeyGroups();

        // Validate the returned object.
        assertTrue(partitionKeyGroupKeys.getPartitionKeyGroupKeys().size() >= 2);
        assertTrue(partitionKeyGroupKeys.getPartitionKeyGroupKeys().contains(createPartitionKeyGroupKey(PARTITION_KEY_GROUP)));
        assertTrue(partitionKeyGroupKeys.getPartitionKeyGroupKeys().contains(createPartitionKeyGroupKey(PARTITION_KEY_GROUP_2)));
    }
}
