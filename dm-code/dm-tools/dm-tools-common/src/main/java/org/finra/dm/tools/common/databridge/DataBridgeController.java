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
package org.finra.dm.tools.common.databridge;

import org.springframework.beans.factory.annotation.Autowired;

import org.finra.dm.service.S3Service;
import org.finra.dm.service.helper.DmHelper;

/**
 * A base class for the uploader and downloader controller. This class is abstract since it is not an actual controller, but just a help class that provides
 * common base functionality that should be used by an extending class.
 */
public abstract class DataBridgeController
{
    public static final Integer MAX_THREADS = 100;       // Maximum number of threads to be used by the Amazon S3 TransferManager.
    public static final Integer MIN_THREADS = 3;         // Minimum number of threads to be used by the Amazon S3 TransferManager.

    @Autowired
    protected S3Service s3Service;

    @Autowired
    protected DmHelper dmHelper;

    /**
     * Adjusts an Integer value according to the specified range of values.
     *
     * @param origValue the original value to be adjusted
     * @param minValue the minimum value of the range
     * @param maxValue the maximum value of the range
     *
     * @return the value adjusted per the specified range of values
     */
    protected Integer adjustIntegerValue(Integer origValue, Integer minValue, Integer maxValue)
    {
        Integer resultValue = origValue;

        if (resultValue.compareTo(minValue) < 0)
        {
            resultValue = minValue;
        }

        if (resultValue.compareTo(maxValue) > 0)
        {
            resultValue = maxValue;
        }

        return resultValue;
    }
}
