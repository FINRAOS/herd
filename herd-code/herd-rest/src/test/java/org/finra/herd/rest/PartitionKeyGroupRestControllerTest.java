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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.PartitionKeyGroup;
import org.finra.herd.model.api.xml.PartitionKeyGroupCreateRequest;
import org.finra.herd.model.api.xml.PartitionKeyGroupKey;
import org.finra.herd.model.api.xml.PartitionKeyGroupKeys;
import org.finra.herd.service.PartitionKeyGroupService;

/**
 * This class tests various functionality within the partition key group REST controller.
 */
public class PartitionKeyGroupRestControllerTest extends AbstractRestTest
{
    @Mock
    private PartitionKeyGroupService partitionKeyGroupService;

    @InjectMocks
    private PartitionKeyGroupRestController partitionKeyGroupRestController;

    @Before()
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreatePartitionKeyGroup()
    {
        // Create a partition key group.
        PartitionKeyGroup partitionKeyGroup = new PartitionKeyGroup(new PartitionKeyGroupKey(PARTITION_KEY_GROUP));
        PartitionKeyGroupCreateRequest request = partitionKeyGroupServiceTestHelper.createPartitionKeyGroupCreateRequest(PARTITION_KEY_GROUP);
        when(partitionKeyGroupService.createPartitionKeyGroup(request)).thenReturn(partitionKeyGroup);
        PartitionKeyGroup resultPartitionKeyGroup = partitionKeyGroupRestController.createPartitionKeyGroup(request);

        // Validate the returned object.
        partitionKeyGroupServiceTestHelper.validatePartitionKeyGroup(PARTITION_KEY_GROUP, resultPartitionKeyGroup);
        // Verify the external calls.
        verify(partitionKeyGroupService).createPartitionKeyGroup(request);
        verifyNoMoreInteractions(partitionKeyGroupService);
        // Validate the returned object.
        assertEquals(partitionKeyGroup, resultPartitionKeyGroup);
    }

    @Test
    public void testGetPartitionKeyGroup()
    {
        PartitionKeyGroup partitionKeyGroup = new PartitionKeyGroup(new PartitionKeyGroupKey(PARTITION_KEY_GROUP));
        PartitionKeyGroupKey partitionKeyGroupKey = new PartitionKeyGroupKey(PARTITION_KEY_GROUP);

        when(partitionKeyGroupService.getPartitionKeyGroup(partitionKeyGroupKey)).thenReturn(partitionKeyGroup);

        // Retrieve the partition key group.
        PartitionKeyGroup resultPartitionKeyGroup = partitionKeyGroupRestController.getPartitionKeyGroup(PARTITION_KEY_GROUP);

        // Verify the external calls.
        verify(partitionKeyGroupService).getPartitionKeyGroup(partitionKeyGroupKey);
        verifyNoMoreInteractions(partitionKeyGroupService);
        // Validate the returned object.
        assertEquals(partitionKeyGroup, resultPartitionKeyGroup);
    }

    @Test
    public void testDeletePartitionKeyGroup()
    {
        PartitionKeyGroup partitionKeyGroup = new PartitionKeyGroup(new PartitionKeyGroupKey(PARTITION_KEY_GROUP));
        PartitionKeyGroupKey partitionKeyGroupKey = new PartitionKeyGroupKey(PARTITION_KEY_GROUP);

        when(partitionKeyGroupService.deletePartitionKeyGroup(partitionKeyGroupKey)).thenReturn(partitionKeyGroup);

        // Delete this partition key group.
        PartitionKeyGroup deletedPartitionKeyGroup = partitionKeyGroupRestController.deletePartitionKeyGroup(PARTITION_KEY_GROUP);
        // Verify the external calls.
        verify(partitionKeyGroupService).deletePartitionKeyGroup(partitionKeyGroupKey);
        verifyNoMoreInteractions(partitionKeyGroupService);
        // Validate the returned object.
        assertEquals(partitionKeyGroup, deletedPartitionKeyGroup);
    }

    @Test
    public void testGetPartitionKeyGroups()
    {
        PartitionKeyGroupKeys partitionKeyGroupKeys =
            new PartitionKeyGroupKeys(Arrays.asList(new PartitionKeyGroupKey(PARTITION_KEY_GROUP), new PartitionKeyGroupKey(PARTITION_KEY_GROUP_2)));
        when(partitionKeyGroupService.getPartitionKeyGroups()).thenReturn(partitionKeyGroupKeys);
        // Get the list of partition key groups.
        PartitionKeyGroupKeys resultPartitionKeyGroupKeys = partitionKeyGroupRestController.getPartitionKeyGroups();

        // Validate the returned object.
        assertTrue(resultPartitionKeyGroupKeys.getPartitionKeyGroupKeys().size() >= 2);
        assertTrue(resultPartitionKeyGroupKeys.getPartitionKeyGroupKeys()
            .contains(partitionKeyGroupServiceTestHelper.createPartitionKeyGroupKey(PARTITION_KEY_GROUP)));
        assertTrue(resultPartitionKeyGroupKeys.getPartitionKeyGroupKeys()
            .contains(partitionKeyGroupServiceTestHelper.createPartitionKeyGroupKey(PARTITION_KEY_GROUP_2)));

        // Verify the external calls.
        verify(partitionKeyGroupService).getPartitionKeyGroups();
        verifyNoMoreInteractions(partitionKeyGroupService);
        // Validate the returned object.
        assertEquals(partitionKeyGroupKeys, resultPartitionKeyGroupKeys);
    }
}
