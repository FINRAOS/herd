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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.PartitionKeyGroup;
import org.finra.herd.model.api.xml.PartitionKeyGroupKey;
import org.finra.herd.model.api.xml.PartitionKeyGroupKeys;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;

/**
 * This class tests various functionality within the partition key group REST controller.
 */
public class PartitionKeyGroupServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreatePartitionKeyGroup()
    {
        // Create a partition key group.
        PartitionKeyGroup resultPartitionKeyGroup = partitionKeyGroupServiceTestHelper.createPartitionKeyGroup(PARTITION_KEY_GROUP);

        // Validate the returned object.
        partitionKeyGroupServiceTestHelper.validatePartitionKeyGroup(PARTITION_KEY_GROUP, resultPartitionKeyGroup);
    }

    @Test
    public void testCreatePartitionKeyGroupMissingRequiredParameters()
    {
        // Try to perform a create without specifying partition key group name.
        try
        {
            partitionKeyGroupServiceTestHelper.createPartitionKeyGroup(BLANK_TEXT);
            fail("Should throw an IllegalArgumentException when partition key group is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition key group name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreatePartitionKeyGroupTrimParameters()
    {
        // Create a partition key group by passing partition key group name with leading and trailing whitespace characters.
        PartitionKeyGroup resultPartitionKeyGroup = partitionKeyGroupServiceTestHelper.createPartitionKeyGroup(addWhitespace(PARTITION_KEY_GROUP));

        // Validate the returned object.
        partitionKeyGroupServiceTestHelper.validatePartitionKeyGroup(PARTITION_KEY_GROUP, resultPartitionKeyGroup);
    }

    @Test
    public void testCreatePartitionKeyGroupInvalidParameters()
    {
        // Try to perform a create when partition key group name contains a forward slash character.
        try
        {
            partitionKeyGroupServiceTestHelper.createPartitionKeyGroup(addSlash(PARTITION_KEY_GROUP));
            fail("Should throw an IllegalArgumentException when partition key group name contains a forward slash character.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Partition key group name can not contain a forward slash character.", e.getMessage());
        }
    }

    @Test
    public void testCreatePartitionKeyGroupEntityAlreadyExists()
    {
        // Create and persist a partition key group entity.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Try to create a partition key group with the same partition key group name.
        try
        {
            partitionKeyGroupServiceTestHelper.createPartitionKeyGroup(PARTITION_KEY_GROUP);
        }
        catch (AlreadyExistsException e)
        {
            assertEquals(String.format("Unable to create partition key group with name \"%s\" because it already exists.", PARTITION_KEY_GROUP),
                e.getMessage());
        }
    }

    @Test
    public void testGetPartitionKeyGroup()
    {
        // Create and persist a partition key group entity.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Retrieve the partition key group.
        PartitionKeyGroup resultPartitionKeyGroup = partitionKeyGroupService.getPartitionKeyGroup(new PartitionKeyGroupKey(PARTITION_KEY_GROUP));

        // Validate the returned object.
        partitionKeyGroupServiceTestHelper.validatePartitionKeyGroup(PARTITION_KEY_GROUP, resultPartitionKeyGroup);
    }

    @Test
    public void testGetPartitionKeyGroupMissingRequiredParameters()
    {
        // Try to perform a get without specifying partition key group name.
        try
        {
            partitionKeyGroupService.getPartitionKeyGroup(new PartitionKeyGroupKey(BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when partition key group is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition key group name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetPartitionKeyGroupTrimParameters()
    {
        // Create and persist a partition key group entity.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Retrieve the partition key group by passing partition key group name with leading and trailing whitespace characters.
        PartitionKeyGroup resultPartitionKeyGroup = partitionKeyGroupService.getPartitionKeyGroup(new PartitionKeyGroupKey(addWhitespace(PARTITION_KEY_GROUP)));

        // Validate the returned object.
        partitionKeyGroupServiceTestHelper.validatePartitionKeyGroup(PARTITION_KEY_GROUP, resultPartitionKeyGroup);
    }

    @Test
    public void testGetPartitionKeyGroupUpperCaseParameters()
    {
        // Create and persist a partition key group entity with a lowercase name.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP.toLowerCase());

        // Retrieve the partition key group by passing partition key group name in upper case.
        PartitionKeyGroup resultPartitionKeyGroup = partitionKeyGroupService.getPartitionKeyGroup(new PartitionKeyGroupKey(PARTITION_KEY_GROUP.toUpperCase()));

        // Validate the returned object.
        partitionKeyGroupServiceTestHelper.validatePartitionKeyGroup(PARTITION_KEY_GROUP.toLowerCase(), resultPartitionKeyGroup);
    }

    @Test
    public void testGetPartitionKeyGroupLowerCaseParameters()
    {
        // Create and persist a partition key group with an uppercase name.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP.toUpperCase());

        // Retrieve the partition key group by passing partition key group name in lower case.
        PartitionKeyGroup resultPartitionKeyGroup = partitionKeyGroupService.getPartitionKeyGroup(new PartitionKeyGroupKey(PARTITION_KEY_GROUP.toLowerCase()));

        // Validate the returned object.
        partitionKeyGroupServiceTestHelper.validatePartitionKeyGroup(PARTITION_KEY_GROUP.toUpperCase(), resultPartitionKeyGroup);
    }

    @Test
    public void testDeletePartitionKeyGroup()
    {
        // Create and persist a partition key group entity.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Validate that this partition key group exists.
        partitionKeyGroupService.getPartitionKeyGroup(new PartitionKeyGroupKey(PARTITION_KEY_GROUP));

        // Delete this partition key group.
        PartitionKeyGroup deletedPartitionKeyGroup = partitionKeyGroupService.deletePartitionKeyGroup(new PartitionKeyGroupKey(PARTITION_KEY_GROUP));

        // Validate the returned object.
        partitionKeyGroupServiceTestHelper.validatePartitionKeyGroup(PARTITION_KEY_GROUP, deletedPartitionKeyGroup);

        // Ensure that this partition key group is no longer there.
        assertNull(partitionKeyGroupDao.getPartitionKeyGroupByKey(partitionKeyGroupServiceTestHelper.createPartitionKeyGroupKey(PARTITION_KEY_GROUP)));
    }

    @Test
    public void testDeletePartitionKeyGroupMissingRequiredParameters()
    {
        // Try to perform a delete without specifying partition key group name.
        try
        {
            partitionKeyGroupService.deletePartitionKeyGroup(new PartitionKeyGroupKey(BLANK_TEXT));
            fail("Should throw an IllegalArgumentException when partition key group is not specified.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition key group name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testDeletePartitionKeyGroupTrimParameters()
    {
        // Create and persist a partition key group entity.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Validate that this partition key group exists.
        partitionKeyGroupService.getPartitionKeyGroup(new PartitionKeyGroupKey(PARTITION_KEY_GROUP));

        // Delete this partition key group by passing partition key group name with leading and trailing whitespace characters.
        PartitionKeyGroup deletedPartitionKeyGroup =
            partitionKeyGroupService.deletePartitionKeyGroup(new PartitionKeyGroupKey(addWhitespace(PARTITION_KEY_GROUP)));

        // Validate the returned object.
        partitionKeyGroupServiceTestHelper.validatePartitionKeyGroup(PARTITION_KEY_GROUP, deletedPartitionKeyGroup);

        // Ensure that this partition key group is no longer there.
        assertNull(partitionKeyGroupDao.getPartitionKeyGroupByKey(partitionKeyGroupServiceTestHelper.createPartitionKeyGroupKey(PARTITION_KEY_GROUP)));
    }

    @Test
    public void testDeletePartitionKeyGroupUpperCaseParameters()
    {
        // Create and persist a partition key group entity with a lowercase name.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP.toLowerCase());

        // Delete this partition key group by passing partition key group name in upper case.
        PartitionKeyGroup deletedPartitionKeyGroup =
            partitionKeyGroupService.deletePartitionKeyGroup(new PartitionKeyGroupKey(PARTITION_KEY_GROUP.toUpperCase()));

        // Validate the returned object.
        partitionKeyGroupServiceTestHelper.validatePartitionKeyGroup(PARTITION_KEY_GROUP.toLowerCase(), deletedPartitionKeyGroup);

        // Ensure that this partition key group is no longer there.
        assertNull(
            partitionKeyGroupDao.getPartitionKeyGroupByKey(partitionKeyGroupServiceTestHelper.createPartitionKeyGroupKey(PARTITION_KEY_GROUP.toLowerCase())));
    }

    @Test
    public void testDeletePartitionKeyGroupLowerCaseParameters()
    {
        // Create and persist a partition key group entity with an uppercase name.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP.toUpperCase());

        // Validate that this partition key group exists.
        partitionKeyGroupService.getPartitionKeyGroup(new PartitionKeyGroupKey(PARTITION_KEY_GROUP.toUpperCase()));

        // Retrieve the partition key group by passing partition key group name in lower case.
        PartitionKeyGroup deletedPartitionKeyGroup =
            partitionKeyGroupService.deletePartitionKeyGroup(new PartitionKeyGroupKey(PARTITION_KEY_GROUP.toLowerCase()));

        // Validate the returned object.
        partitionKeyGroupServiceTestHelper.validatePartitionKeyGroup(PARTITION_KEY_GROUP.toUpperCase(), deletedPartitionKeyGroup);

        // Ensure that this partition key group is no longer there.
        assertNull(
            partitionKeyGroupDao.getPartitionKeyGroupByKey(partitionKeyGroupServiceTestHelper.createPartitionKeyGroupKey(PARTITION_KEY_GROUP.toUpperCase())));
    }

    @Test
    public void testDeletePartitionKeyGroupExpectedPartitionValuesPresent()
    {
        // Create and persist a partition key group entity.
        PartitionKeyGroupEntity partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Add expected partition values to this partition key group.
        expectedPartitionValueDaoTestHelper.createExpectedPartitionValueProcessDatesForApril2014(PARTITION_KEY_GROUP);
        herdDao.saveAndRefresh(partitionKeyGroupEntity);

        // Delete this partition key group.
        PartitionKeyGroup deletedPartitionKeyGroup = partitionKeyGroupService.deletePartitionKeyGroup(new PartitionKeyGroupKey(PARTITION_KEY_GROUP));

        // Validate the returned object.
        partitionKeyGroupServiceTestHelper.validatePartitionKeyGroup(PARTITION_KEY_GROUP, deletedPartitionKeyGroup);

        // Ensure that this partition key group is no longer there.
        assertNull(partitionKeyGroupDao.getPartitionKeyGroupByKey(partitionKeyGroupServiceTestHelper.createPartitionKeyGroupKey(PARTITION_KEY_GROUP)));
    }

    @Test
    public void testDeletePartitionKeyGroupUsedByFormat()
    {
        // Create a partition key group.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);

        // Create a business object format that uses this partition key group.
        businessObjectFormatDaoTestHelper
            .createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, INITIAL_FORMAT_VERSION, FORMAT_DESCRIPTION,
                FORMAT_DOCUMENT_SCHEMA, true, PARTITION_KEY, PARTITION_KEY_GROUP);

        // Try to delete this partition key group.
        try
        {
            partitionKeyGroupService.deletePartitionKeyGroup(new PartitionKeyGroupKey(PARTITION_KEY_GROUP));
            fail("Suppose to throw an IllegalArgumentException when partition key group is used by business object format.");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Can not delete \"%s\" partition key group since it is being used by a business object format.", PARTITION_KEY_GROUP),
                e.getMessage());
        }
    }

    @Test
    public void testGetPartitionKeyGroups()
    {
        // Create and persist two partition key group entities.
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP);
        partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(PARTITION_KEY_GROUP_2);

        // Get the list of partition key groups.
        PartitionKeyGroupKeys partitionKeyGroupKeys = partitionKeyGroupService.getPartitionKeyGroups();

        // Validate the returned object.
        assertTrue(partitionKeyGroupKeys.getPartitionKeyGroupKeys().size() >= 2);
        assertTrue(
            partitionKeyGroupKeys.getPartitionKeyGroupKeys().contains(partitionKeyGroupServiceTestHelper.createPartitionKeyGroupKey(PARTITION_KEY_GROUP)));
        assertTrue(
            partitionKeyGroupKeys.getPartitionKeyGroupKeys().contains(partitionKeyGroupServiceTestHelper.createPartitionKeyGroupKey(PARTITION_KEY_GROUP_2)));
    }
}
