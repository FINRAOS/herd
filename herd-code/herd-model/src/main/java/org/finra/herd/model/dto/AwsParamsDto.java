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
package org.finra.herd.model.dto;

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
     * An AWS access key ID that identifies the security credentials.
     */
    private String awsAccessKeyId;

    /**
     * An AWS secret access key.
     */
    private String awsSecretKey;

    /**
     * An AWS session token.
     */
    private String sessionToken;

    /**
     * An HTTP proxy host.
     */
    private String httpProxyHost;

    /**
     * An HTTP proxy port.
     */
    private Integer httpProxyPort;

    public String getAwsAccessKeyId()
    {
        return awsAccessKeyId;
    }

    public void setAwsAccessKeyId(String awsAccessKeyId)
    {
        this.awsAccessKeyId = awsAccessKeyId;
    }

    public String getAwsSecretKey()
    {
        return awsSecretKey;
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (!(object instanceof AwsParamsDto))
        {
            return false;
        }

        AwsParamsDto that = (AwsParamsDto) object;

        if (awsAccessKeyId != null ? !awsAccessKeyId.equals(that.awsAccessKeyId) : that.awsAccessKeyId != null)
        {
            return false;
        }
        if (awsSecretKey != null ? !awsSecretKey.equals(that.awsSecretKey) : that.awsSecretKey != null)
        {
            return false;
        }
        if (httpProxyHost != null ? !httpProxyHost.equals(that.httpProxyHost) : that.httpProxyHost != null)
        {
            return false;
        }
        if (httpProxyPort != null ? !httpProxyPort.equals(that.httpProxyPort) : that.httpProxyPort != null)
        {
            return false;
        }
        if (sessionToken != null ? !sessionToken.equals(that.sessionToken) : that.sessionToken != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = awsAccessKeyId != null ? awsAccessKeyId.hashCode() : 0;
        result = 31 * result + (awsSecretKey != null ? awsSecretKey.hashCode() : 0);
        result = 31 * result + (sessionToken != null ? sessionToken.hashCode() : 0);
        result = 31 * result + (httpProxyHost != null ? httpProxyHost.hashCode() : 0);
        result = 31 * result + (httpProxyPort != null ? httpProxyPort.hashCode() : 0);
        return result;
    }

    public String getSessionToken()
    {

        return sessionToken;
    }

    public void setSessionToken(String sessionToken)
    {
        this.sessionToken = sessionToken;
    }

    public void setAwsSecretKey(String awsSecretKey)
    {
        this.awsSecretKey = awsSecretKey;
    }

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
