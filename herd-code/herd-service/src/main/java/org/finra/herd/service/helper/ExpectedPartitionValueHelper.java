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

import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.ExpectedPartitionValueKey;

/**
 * A helper class for ExpectedPartitionValue related code.
 */
@Component
public class ExpectedPartitionValueHelper
{
    /**
     * Validates the expected partition value key. This method also trims the key parameters.
     *
     * @param expectedPartitionValueKey the expected partition value key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateExpectedPartitionValueKey(ExpectedPartitionValueKey expectedPartitionValueKey) throws IllegalArgumentException
    {
        // Validate.
        Assert.notNull(expectedPartitionValueKey, "An expected partition value key must be specified.");
        Assert.hasText(expectedPartitionValueKey.getPartitionKeyGroupName(), "A partition key group name must be specified.");
        Assert.hasText(expectedPartitionValueKey.getExpectedPartitionValue(), "An expected partition value must be specified.");

        // Remove leading and trailing spaces.
        expectedPartitionValueKey.setPartitionKeyGroupName(expectedPartitionValueKey.getPartitionKeyGroupName().trim());
        expectedPartitionValueKey.setExpectedPartitionValue(expectedPartitionValueKey.getExpectedPartitionValue().trim());
    }
}
