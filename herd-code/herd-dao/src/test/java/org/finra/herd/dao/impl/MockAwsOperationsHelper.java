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
package org.finra.herd.dao.impl;

import com.amazonaws.services.elasticmapreduce.model.ClusterState;

/**
 * Helper class for mock aws operations. 
 */
public class MockAwsOperationsHelper
{
    public static final String AMAZON_SERVICE_EXCEPTION = "amazon_service_exception";
    public static final String AMAZON_BAD_REQUEST = "amazon_bad_request";
    public static final String AMAZON_NOT_FOUND = "amazon_not_found_exception";

    public static final String AMAZON_THROTTLING_EXCEPTION = "amazon_throttling_exception";

    public static final String AMAZON_CLUSTER_STATUS_WAITING = ClusterState.WAITING.toString();
    public static final String AMAZON_CLUSTER_STATUS_RUNNING = ClusterState.RUNNING.toString();
}
