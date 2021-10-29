package org.finra.herd.tools.common.oauth2;

import java.util.Objects;

public class AuthKey
{
    private final String username;
    private final String password;
    private final String authUrl;

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
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        AuthKey that = (AuthKey) o;
        return Objects.equals(username, that.username) && Objects.equals(password, that.password) && Objects.equals(authUrl, that.authUrl);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(username, password, authUrl);
    }
}
