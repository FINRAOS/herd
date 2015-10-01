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
package org.finra.dm.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.dm.model.dto.SecurityFunctions;
import org.finra.dm.model.api.xml.PartitionKeyGroup;
import org.finra.dm.model.api.xml.PartitionKeyGroupCreateRequest;
import org.finra.dm.model.api.xml.PartitionKeyGroupKey;
import org.finra.dm.model.api.xml.PartitionKeyGroupKeys;
import org.finra.dm.service.PartitionKeyGroupService;
import org.finra.dm.ui.constants.UiConstants;

/**
 * The REST controller that handles partition key group REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
public class PartitionKeyGroupRestController extends DmBaseController
{
    public static final String PARTITION_KEY_GROUPS_URI_PREFIX = "/partitionKeyGroups";

    @Autowired
    private PartitionKeyGroupService partitionKeyGroupService;

    /**
     * Creates a new partition key group.
     *
     * @param request the information needed to create a partition key group
     *
     * @return the newly created partition key group information
     */
    @RequestMapping(value = PARTITION_KEY_GROUPS_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_PARTITION_KEY_GROUPS_POST)
    public PartitionKeyGroup createPartitionKeyGroup(@RequestBody PartitionKeyGroupCreateRequest request)
    {
        return partitionKeyGroupService.createPartitionKeyGroup(request);
    }

    /**
     * Gets an existing partition key group by name.
     *
     * @param partitionKeyGroupName the partition key group name
     *
     * @return the partition key group information
     */
    @RequestMapping(value = PARTITION_KEY_GROUPS_URI_PREFIX + "/{partitionKeyGroupName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_PARTITION_KEY_GROUPS_GET)
    public PartitionKeyGroup getPartitionKeyGroup(@PathVariable("partitionKeyGroupName") String partitionKeyGroupName)
    {
        PartitionKeyGroupKey partitionKeyGroupKey = new PartitionKeyGroupKey();
        partitionKeyGroupKey.setPartitionKeyGroupName(partitionKeyGroupName);
        return partitionKeyGroupService.getPartitionKeyGroup(partitionKeyGroupKey);
    }

    /**
     * Deletes an existing partition key group by name.
     *
     * @param partitionKeyGroupName the partition key group name
     *
     * @return the partition key group that got deleted
     */
    @RequestMapping(value = PARTITION_KEY_GROUPS_URI_PREFIX + "/{partitionKeyGroupName}", method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_PARTITION_KEY_GROUPS_DELETE)
    public PartitionKeyGroup deletePartitionKeyGroup(@PathVariable("partitionKeyGroupName") String partitionKeyGroupName)
    {
        PartitionKeyGroupKey partitionKeyGroupKey = new PartitionKeyGroupKey();
        partitionKeyGroupKey.setPartitionKeyGroupName(partitionKeyGroupName);
        return partitionKeyGroupService.deletePartitionKeyGroup(partitionKeyGroupKey);
    }

    /**
     * Gets a list of all existing partition key groups.
     *
     * @return the partition key groups
     */
    @RequestMapping(value = PARTITION_KEY_GROUPS_URI_PREFIX, method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_PARTITION_KEY_GROUPS_ALL_GET)
    public PartitionKeyGroupKeys getPartitionKeyGroups()
    {
        return partitionKeyGroupService.getPartitionKeyGroups();
    }
}
