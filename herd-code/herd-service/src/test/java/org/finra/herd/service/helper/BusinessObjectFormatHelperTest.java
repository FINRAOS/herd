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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.finra.herd.service.AbstractServiceTest;

/**
 * This class tests functionality within the BusinessObjectFormatHelper class.
 */
public class BusinessObjectFormatHelperTest extends AbstractServiceTest
{
    @Test
    public void testGetPartitionKeyToPartitionLevelMapping()
    {
        // Declare objects to validate the results.
        Map<String, Integer> result;
        Map<String, Integer> expectedResult;

        // One partition key and no partition level values.
        result = businessObjectFormatHelper.getPartitionKeyToPartitionLevelMapping(Collections.singletonList(PARTITION_KEY),
            Collections.singletonList(Collections.emptyList()));
        expectedResult = new HashMap<>();
        assertEquals(expectedResult, result);

        // One partition key and one partition level value.
        result = businessObjectFormatHelper.getPartitionKeyToPartitionLevelMapping(Collections.singletonList(PARTITION_KEY),
            Collections.singletonList(Collections.singletonList(INTEGER_VALUE)));
        expectedResult = new HashMap<>();
        expectedResult.put(PARTITION_KEY, INTEGER_VALUE);
        assertEquals(expectedResult, result);

        // One partition key and multiple and identical partition level values.
        result = businessObjectFormatHelper.getPartitionKeyToPartitionLevelMapping(Collections.singletonList(PARTITION_KEY),
            Collections.singletonList(Arrays.asList(INTEGER_VALUE, INTEGER_VALUE, INTEGER_VALUE)));
        expectedResult = new HashMap<>();
        expectedResult.put(PARTITION_KEY, INTEGER_VALUE);
        assertEquals(expectedResult, result);

        // One partition key and multiple and identical partition level values.
        result = businessObjectFormatHelper.getPartitionKeyToPartitionLevelMapping(Collections.singletonList(PARTITION_KEY),
            Collections.singletonList(Arrays.asList(INTEGER_VALUE, INTEGER_VALUE)));
        expectedResult = new HashMap<>();
        expectedResult.put(PARTITION_KEY, INTEGER_VALUE);
        assertEquals(expectedResult, result);

        // One partition key and multiple and different partition level values.
        result = businessObjectFormatHelper.getPartitionKeyToPartitionLevelMapping(Collections.singletonList(PARTITION_KEY),
            Collections.singletonList(Arrays.asList(INTEGER_VALUE, INTEGER_VALUE_2)));
        expectedResult = new HashMap<>();
        assertEquals(expectedResult, result);

        // Multiple partition keys and multiple partition level values some different and some identical.
        result = businessObjectFormatHelper.getPartitionKeyToPartitionLevelMapping(Arrays.asList(COLUMN_NAME, COLUMN_NAME_2, PARTITION_KEY),
            Arrays.asList(Arrays.asList(INTEGER_VALUE, INTEGER_VALUE), Arrays.asList(INTEGER_VALUE, INTEGER_VALUE_2),
                Arrays.asList(INTEGER_VALUE_2, INTEGER_VALUE_2)));
        expectedResult = new HashMap<>();
        expectedResult.put(COLUMN_NAME, INTEGER_VALUE);
        expectedResult.put(PARTITION_KEY, INTEGER_VALUE_2);
        assertEquals(expectedResult, result);
    }
}
