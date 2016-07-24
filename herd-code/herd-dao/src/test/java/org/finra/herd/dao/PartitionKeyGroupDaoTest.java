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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.PartitionKeyGroupKey;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;

public class PartitionKeyGroupDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetPartitionKeyGroupByKey()
    {
        // Create relative database entities.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Retrieve partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDao.getPartitionKeyGroupByKey(new PartitionKeyGroupKey(PARTITION_KEY_GROUP));

        // Validate the results.
        assertNotNull(partitionKeyGroupEntity);
        assertTrue(partitionKeyGroupEntity.getPartitionKeyGroupName().equals(PARTITION_KEY_GROUP));
    }

    @Test
    public void testGetPartitionKeyGroupByKeyInUpperCase()
    {
        // Create relative database entities.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP.toLowerCase());

        // Retrieve partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDao.getPartitionKeyGroupByName(PARTITION_KEY_GROUP.toUpperCase());

        // Validate the results.
        assertNotNull(partitionKeyGroupEntity);
        assertTrue(partitionKeyGroupEntity.getPartitionKeyGroupName().equals(PARTITION_KEY_GROUP.toLowerCase()));
    }

    @Test
    public void testGetPartitionKeyGroupByKeyInLowerCase()
    {
        // Create relative database entities.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP.toUpperCase());

        // Retrieve partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDao.getPartitionKeyGroupByName(PARTITION_KEY_GROUP.toLowerCase());

        // Validate the results.
        assertNotNull(partitionKeyGroupEntity);
        assertTrue(partitionKeyGroupEntity.getPartitionKeyGroupName().equals(PARTITION_KEY_GROUP.toUpperCase()));
    }

    @Test
    public void testGetPartitionKeyGroupByKeyInvalidKey()
    {
        // Create relative database entities.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Try to retrieve partition key group entity using an invalid name.
        assertNull(partitionKeyGroupDao.getPartitionKeyGroupByName("I_DO_NOT_EXIST"));
    }

    @Test
    public void testGetPartitionKeyGroupByKeyMultipleRecordsFound()
    {
        // Create relative database entities.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP.toUpperCase());
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP.toLowerCase());

        // Try to retrieve a partition key group when multiple entities exist with the same name (using case insensitive string comparison).
        try
        {
            partitionKeyGroupDao.getPartitionKeyGroupByName(PARTITION_KEY_GROUP);
            fail("Should throw an IllegalArgumentException if finds more than one partition key group entities with the same name.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Found more than one \"%s\" partition key group.", PARTITION_KEY_GROUP), e.getMessage());
        }
    }

    @Test
    public void testGetPartitionKeyGroups()
    {
        // Create and persist two partition key group entities.
        for (PartitionKeyGroupKey partitionKeyGroupKey : getTestPartitionKeyGroupKeys())
        {
            partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(partitionKeyGroupKey.getPartitionKeyGroupName());
        }

        // Get the list of partition key groups.
        List<PartitionKeyGroupKey> resultPartitionKeyGroupKeys = partitionKeyGroupDao.getPartitionKeyGroups();

        // Validate the results.
        assertNotNull(resultPartitionKeyGroupKeys);
        assertTrue(resultPartitionKeyGroupKeys.containsAll(getTestPartitionKeyGroupKeys()));
    }

    /**
     * Returns a list of test partition key group keys.
     *
     * @return the list of test partition key group keys
     */
    private List<PartitionKeyGroupKey> getTestPartitionKeyGroupKeys()
    {
        // Get a list of test file type keys.
        return Arrays.asList(new PartitionKeyGroupKey(PARTITION_KEY_GROUP), new PartitionKeyGroupKey(PARTITION_KEY_GROUP_2));
    }
}
