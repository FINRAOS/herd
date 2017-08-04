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
package org.finra.herd.dao;

import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.stereotype.Component;

/**
 * PreBuiltTransportClientFactory
 * <p>
 * Factory to build a PreBuiltTransportClient. This will allow us to safely unit test the PreBuiltTransportClient class.
 */
@Component
public class PreBuiltTransportClientFactory
{
    /**
     * A method to retrieve a PreBuiltTransportClient.
     *
     * @param settings Elasticsearch settings
     *
     * @return a PreBuiltTransportClient
     */
    public PreBuiltTransportClient getPreBuiltTransportClient(Settings settings)
    {
        return new PreBuiltTransportClient(settings);
    }

    /**
     * A method to retrieve a PreBuiltTransportClient with the SearchGuardPlugin.
     *
     * @param settings Elasticsearch settings
     *
     * @return a PreBuiltTransportClient
     */
    public PreBuiltTransportClient getPreBuiltTransportClientWithSearchGuardPlugin(Settings settings)
    {
        return new PreBuiltTransportClient(settings, SearchGuardSSLPlugin.class);
    }
}
