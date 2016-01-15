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

import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.ExpectedPartitionValueInformation;
import org.finra.herd.model.api.xml.ExpectedPartitionValueKey;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesCreateRequest;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesDeleteRequest;
import org.finra.herd.model.api.xml.ExpectedPartitionValuesInformation;
import org.finra.herd.model.api.xml.PartitionKeyGroupKey;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.ExpectedPartitionValueService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles expected partition value REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Expected Partition Value")
public class ExpectedPartitionValueRestController extends HerdBaseController
{
    public static final String EXPECTED_PARTITION_VALUES_URI_PREFIX = "/expectedPartitionValues";

    @Autowired
    private ExpectedPartitionValueService expectedPartitionValueService;

    /**
     * Creates a list of expected partition values for an existing partition key group.
     *
     * @param request the information needed to create the expected partition values
     *
     * @return the newly created expected partition values
     */
    @RequestMapping(value = EXPECTED_PARTITION_VALUES_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_EXPECTED_PARTITION_VALUES_POST)
    public ExpectedPartitionValuesInformation createExpectedPartitionValues(@RequestBody ExpectedPartitionValuesCreateRequest request)
    {
        return expectedPartitionValueService.createExpectedPartitionValues(request);
    }

    /**
     * Retrieves an existing expected partition value plus/minus an optional offset.
     *
     * @param partitionKeyGroupName the partition key group name
     * @param expectedPartitionValue the expected partition value to retrieve plus/minus an optional offset
     * @param offset the optional positive or negative offset
     *
     * @return the expected partition value
     */
    @RequestMapping(
        value = EXPECTED_PARTITION_VALUES_URI_PREFIX + "/partitionKeyGroups/{partitionKeyGroupName}/expectedPartitionValues/{expectedPartitionValue}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_EXPECTED_PARTITION_VALUES_GET)
    public ExpectedPartitionValueInformation getExpectedPartitionValue(@PathVariable("partitionKeyGroupName") String partitionKeyGroupName,
        @PathVariable("expectedPartitionValue") String expectedPartitionValue, @RequestParam(value = "offset", required = false) Integer offset)
    {
        ExpectedPartitionValueKey expectedPartitionValueKey = new ExpectedPartitionValueKey();
        expectedPartitionValueKey.setPartitionKeyGroupName(partitionKeyGroupName);
        expectedPartitionValueKey.setExpectedPartitionValue(expectedPartitionValue);
        return expectedPartitionValueService.getExpectedPartitionValue(expectedPartitionValueKey, offset);
    }

    /**
     * Retrieves a range of existing expected partition values.
     *
     * @param partitionKeyGroupName the partition key group name
     * @param startExpectedPartitionValue the start expected partition value for the expected partition value range
     * @param endExpectedPartitionValue the end expected partition value for the expected partition value range
     *
     * @return the expected partition values
     */
    @RequestMapping(value = EXPECTED_PARTITION_VALUES_URI_PREFIX + "/partitionKeyGroups/{partitionKeyGroupName}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_EXPECTED_PARTITION_VALUES_GET)
    public ExpectedPartitionValuesInformation getExpectedPartitionValues(@PathVariable("partitionKeyGroupName") String partitionKeyGroupName,
        @RequestParam(value = "startExpectedPartitionValue", required = false) String startExpectedPartitionValue,
        @RequestParam(value = "endExpectedPartitionValue", required = false) String endExpectedPartitionValue)
    {
        PartitionKeyGroupKey partitionKeyGroupKey = new PartitionKeyGroupKey();
        partitionKeyGroupKey.setPartitionKeyGroupName(partitionKeyGroupName);
        PartitionValueRange partitionValueRange = new PartitionValueRange();
        partitionValueRange.setStartPartitionValue(startExpectedPartitionValue);
        partitionValueRange.setEndPartitionValue(endExpectedPartitionValue);
        return expectedPartitionValueService.getExpectedPartitionValues(partitionKeyGroupKey, partitionValueRange);
    }

    /**
     * Deletes specified expected partition values from an existing partition key group which is identified by name.
     *
     * @param request the information needed to delete the expected partition values
     *
     * @return the expected partition values that got deleted
     */
    @RequestMapping(value = EXPECTED_PARTITION_VALUES_URI_PREFIX, method = RequestMethod.DELETE, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_EXPECTED_PARTITION_VALUES_DELETE)
    public ExpectedPartitionValuesInformation deleteExpectedPartitionValues(@RequestBody ExpectedPartitionValuesDeleteRequest request)
    {
        return expectedPartitionValueService.deleteExpectedPartitionValues(request);
    }
}
