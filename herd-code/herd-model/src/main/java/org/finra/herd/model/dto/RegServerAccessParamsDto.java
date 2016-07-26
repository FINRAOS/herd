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
 * A DTO that holds various parameters required to communicate with the registration server.
 * <p/>
 * Consider using the builder to make constructing this class easier. For example:
 * <p/>
 * <pre>
 * RegServerAccessParamsDto params = RegServerAccessParamsDto
 *     .builder().regServerHost(&quot;myHost&quot;).regServerPort(myPort).build();
 * </pre>
 */
public class RegServerAccessParamsDto
{
    /**
     * The password to be used for HTTPS client authentication with the registration server.
     */
    private String password;

    /**
     * The Registration Server hostname
     */
    private String regServerHost;

    /**
     * The Registration Server port.
     */
    private Integer regServerPort;

    /**
     * This determines if SSL must be used to communicate with the registration server. If set to true, enables SSL (HTTPS) to communicate with the registration
     * server. Otherwise, uses HTTP.
     */
    private Boolean useSsl;

    /**
     * The username to be used for HTTPS client authentication with the registration server.
     */
    private String username;

    /**
     * Returns a builder that can easily build this DTO.
     *
     * @return the builder.
     */
    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (!(object instanceof RegServerAccessParamsDto))
        {
            return false;
        }

        RegServerAccessParamsDto that = (RegServerAccessParamsDto) object;

        if (password != null ? !password.equals(that.password) : that.password != null)
        {
            return false;
        }
        if (regServerHost != null ? !regServerHost.equals(that.regServerHost) : that.regServerHost != null)
        {
            return false;
        }
        if (regServerPort != null ? !regServerPort.equals(that.regServerPort) : that.regServerPort != null)
        {
            return false;
        }
        if (useSsl != null ? !useSsl.equals(that.useSsl) : that.useSsl != null)
        {
            return false;
        }
        if (username != null ? !username.equals(that.username) : that.username != null)
        {
            return false;
        }

        return true;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public String getRegServerHost()
    {
        return regServerHost;
    }

    public void setRegServerHost(String regServerHost)
    {
        this.regServerHost = regServerHost;
    }

    public Integer getRegServerPort()
    {
        return regServerPort;
    }

    public void setRegServerPort(Integer regServerPort)
    {
        this.regServerPort = regServerPort;
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

    @Override
    public int hashCode()
    {
        int result = regServerHost != null ? regServerHost.hashCode() : 0;
        result = 31 * result + (regServerPort != null ? regServerPort.hashCode() : 0);
        result = 31 * result + (useSsl != null ? useSsl.hashCode() : 0);
        result = 31 * result + (username != null ? username.hashCode() : 0);
        result = 31 * result + (password != null ? password.hashCode() : 0);
        return result;
    }

    /**
     * A builder that makes it easier to construct this DTO.
     */
    public static class Builder
    {
        private RegServerAccessParamsDto params = new RegServerAccessParamsDto();

        public RegServerAccessParamsDto build()
        {
            return params;
        }

        public Builder password(String password)
        {
            params.setPassword(password);
            return this;
        }

        public Builder regServerHost(String regServerHost)
        {
            params.setRegServerHost(regServerHost);
            return this;
        }

        public Builder regServerPort(Integer regServerPort)
        {
            params.setRegServerPort(regServerPort);
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
    }
}
