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
package org.finra.herd.tools.common.oauth2;

import java.util.Objects;

public class AuthKey
{
    private final String username;
    private final String password;
    private final String authUrl;

    /**
     * Constructor for AuthKey used to get access token
     *
     * @param username the Herd API client user
     * @param password the Herd API client password
     * @param authUrl  the url to get Herd API client access token
     */
    public AuthKey(String username, String password, String authUrl)
    {
        this.username = username;
        this.password = password;
        this.authUrl = authUrl;
    }

    public String getUsername()
    {
        return username;
    }

    public String getPassword()
    {
        return password;
    }

    public String getAuthUrl()
    {
        return authUrl;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null || getClass() != obj.getClass())
        {
            return false;
        }
        AuthKey that = (AuthKey) obj;
        return Objects.equals(username, that.username) && Objects.equals(password, that.password) && Objects.equals(authUrl, that.authUrl);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(username, password, authUrl);
    }
}
