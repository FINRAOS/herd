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
package org.finra.dm.service;

import org.finra.dm.model.api.xml.PartitionKeyGroup;
import org.finra.dm.model.api.xml.PartitionKeyGroupCreateRequest;
import org.finra.dm.model.api.xml.PartitionKeyGroupKey;
import org.finra.dm.model.api.xml.PartitionKeyGroupKeys;

/**
 * The partition key group service.
 */
public interface PartitionKeyGroupService
{
    public PartitionKeyGroup createPartitionKeyGroup(PartitionKeyGroupCreateRequest businessObjectFormatCreateRequest);

    public PartitionKeyGroup getPartitionKeyGroup(PartitionKeyGroupKey partitionKeyGroupKey);

    public PartitionKeyGroup deletePartitionKeyGroup(PartitionKeyGroupKey partitionKeyGroupKey);

    public PartitionKeyGroupKeys getPartitionKeyGroups();
}
