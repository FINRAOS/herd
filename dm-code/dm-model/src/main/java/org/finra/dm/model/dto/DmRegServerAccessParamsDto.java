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
 * A DTO that holds various parameters required to communicate with the Data Management Registration Server.
 * <p/>
 * Consider using the builder to make constructing this class easier. For example:
 * <p/>
 * <pre>
 * DmRegServerAccessParamsDto params = DmRegServerAccessParamsDto
 *     .builder().dmRegServerHost(&quot;myHost&quot;).dmRegServerPort(myPort).build();
 * </pre>
 */
public class DmRegServerAccessParamsDto
{
    /**
     * The Data Management Registration Server hostname
     */
    private String dmRegServerHost;

    /**
     * The Data Management Registration Server port.
     */
    private Integer dmRegServerPort;

    /**
     * This determines if SSL must be used to communicate with the Data Management Registration Server. If set to true, enables SSL (HTTPS) to communicate with
     * the Data Management Registration Service. Otherwise, uses HTTP.
     */
    private Boolean useSsl;

    /**
     * The username to be used for HTTPS client authentication with the Data Management Registration Server.
     */
    private String username;

    /**
     * The password to be used for HTTPS client authentication with the Data Management Registration Server.
     */
    private String password;

    public String getDmRegServerHost()
    {
        return dmRegServerHost;
    }

    public void setDmRegServerHost(String dmRegServerHost)
    {
        this.dmRegServerHost = dmRegServerHost;
    }

    public Integer getDmRegServerPort()
    {
        return dmRegServerPort;
    }

    public void setDmRegServerPort(Integer dmRegServerPort)
    {
        this.dmRegServerPort = dmRegServerPort;
    }

    public Boolean getUseSsl()
    {
        return useSsl;
    }

    public void setUseSsl(Boolean useSsl)
    {
        this.useSsl = useSsl;
    }

    public String getUsername()
    {
        return username;
    }

    public void setUsername(String username)
    {
        this.username = username;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    /**
     * Returns a builder that can easily build this DTO.
     *
     * @return the builder.
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * A builder that makes it easier to construct this DTO.
     */
    public static class Builder
    {
        private DmRegServerAccessParamsDto params = new DmRegServerAccessParamsDto();

        public Builder dmRegServerHost(String dmRegServerHost)
        {
            params.setDmRegServerHost(dmRegServerHost);
            return this;
        }

        public Builder dmRegServerPort(Integer dmRegServerPort)
        {
            params.setDmRegServerPort(dmRegServerPort);
            return this;
        }

        public Builder useSsl(Boolean useSsl)
        {
            params.setUseSsl(useSsl);
            return this;
        }

        public Builder username(String username)
        {
            params.setUsername(username);
            return this;
        }

        public Builder password(String password)
        {
            params.setPassword(password);
            return this;
        }

        public DmRegServerAccessParamsDto build()
        {
            return params;
        }
    }
}
