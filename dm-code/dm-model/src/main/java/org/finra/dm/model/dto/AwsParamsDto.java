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
package org.finra.dm.model.dto;

/**
 * A DTO that holds various parameters for making an EMR Cluster create request.
 * <p/>
 * Consider using the builder to make constructing this class easier. For example:
 * <p/>
 * <p/>
 * <pre>
 * AwsParamsDto params = AwsParamsDto.builder().
 *         httpProxyHost(&quot;httpProxyHost&quot;).httpProxyPort(&quot;httpProxyPort&quot;).build();
 * </pre>
 */
public class AwsParamsDto
{
    /**
     * An HTTP proxy host.
     */
    private String httpProxyHost;

    /**
     * An HTTP proxy port.
     */
    private Integer httpProxyPort;

    public String getHttpProxyHost()
    {
        return httpProxyHost;
    }

    public void setHttpProxyHost(String httpProxyHost)
    {
        this.httpProxyHost = httpProxyHost;
    }

    public Integer getHttpProxyPort()
    {
        return httpProxyPort;
    }

    public void setHttpProxyPort(Integer httpProxyPort)
    {
        this.httpProxyPort = httpProxyPort;
    }
}
