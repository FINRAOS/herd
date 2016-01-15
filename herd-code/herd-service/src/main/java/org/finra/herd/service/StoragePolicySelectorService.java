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

import java.util.List;

import org.finra.herd.model.dto.StoragePolicySelection;

/**
 * The storage policy selector service.
 */
public interface StoragePolicySelectorService
{
    /**
     * Finds business object data matching storage policies configured in the system and sends storage policy selection message to the specified SQS queue.
     *
     * @param sqsQueueName the SQS queue name to send storage policy selections to
     * @param maxResult the maximum number of results to retrieve
     *
     * @return the list of storage policy selections sent to the SQS queue
     */
    public List<StoragePolicySelection> execute(String sqsQueueName, int maxResult);
}
