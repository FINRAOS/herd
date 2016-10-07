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
import static org.junit.Assert.assertNotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.PartitionKeyGroup;
import org.finra.herd.model.api.xml.PartitionKeyGroupCreateRequest;
import org.finra.herd.model.api.xml.PartitionKeyGroupKey;

@Component
public class PartitionKeyGroupServiceTestHelper
{
    @Autowired
    private PartitionKeyGroupService partitionKeyGroupService;

    /**
     * Creates partition key group by calling the relative service method.
     *
     * @param partitionKeyGroupName the partition key group name
     *
     * @return the newly created partition key group
     */
    public PartitionKeyGroup createPartitionKeyGroup(String partitionKeyGroupName)
    {
        PartitionKeyGroupCreateRequest request = createPartitionKeyGroupCreateRequest(partitionKeyGroupName);
        return partitionKeyGroupService.createPartitionKeyGroup(request);
    }

    /**
     * Creates a partition key group create request.
     *
     * @param partitionKeyGroupName the partition key group name
     *
     * @return the created partition key group create request
     */
    public PartitionKeyGroupCreateRequest createPartitionKeyGroupCreateRequest(String partitionKeyGroupName)
    {
        PartitionKeyGroupCreateRequest partitionKeyGroupCreateRequest = new PartitionKeyGroupCreateRequest();
        partitionKeyGroupCreateRequest.setPartitionKeyGroupKey(createPartitionKeyGroupKey(partitionKeyGroupName));
        return partitionKeyGroupCreateRequest;
    }

    /**
     * Creates a partition key group key.
     *
     * @param partitionKeyGroupName the partition key group name
     *
     * @return the created partition key group key
     */
    public PartitionKeyGroupKey createPartitionKeyGroupKey(String partitionKeyGroupName)
    {
        PartitionKeyGroupKey partitionKeyGroupKey = new PartitionKeyGroupKey();
        partitionKeyGroupKey.setPartitionKeyGroupName(partitionKeyGroupName);
        return partitionKeyGroupKey;
    }

    /**
     * Validates partition key group contents against specified arguments.
     *
     * @param expectedPartitionKeyGroupName the expected partition key group name
     * @param actualPartitionKeyGroup the partition key group object instance to be validated
     */
    public void validatePartitionKeyGroup(String expectedPartitionKeyGroupName, PartitionKeyGroup actualPartitionKeyGroup)
    {
        assertNotNull(actualPartitionKeyGroup);
        assertEquals(expectedPartitionKeyGroupName, actualPartitionKeyGroup.getPartitionKeyGroupKey().getPartitionKeyGroupName());
    }
}
