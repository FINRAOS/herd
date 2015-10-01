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
package org.finra.dm.app.security;

import java.util.Collection;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.User;

import org.finra.dm.model.dto.ApplicationUser;

@SuppressFBWarnings(value = "EQ_DOESNT_OVERRIDE_EQUALS", justification = "We will use the base class equals method which only uses the username property.")
public class SecurityUserWrapper extends User
{
    private static final long serialVersionUID = -204573707605950187L;

    private ApplicationUser applicationUser;

    public SecurityUserWrapper(String username, String password, boolean enabled, boolean accountNonExpired, boolean credentialsNonExpired,
        boolean accountNonLocked, Collection<? extends GrantedAuthority> authorities, ApplicationUser applicationUser)
    {
        super(username, password, enabled, accountNonExpired, credentialsNonExpired, accountNonLocked, authorities);
        setApplicationUser(applicationUser);
    }

    public ApplicationUser getApplicationUser()
    {
        return applicationUser;
    }

    public void setApplicationUser(ApplicationUser applicationUser)
    {
        this.applicationUser = applicationUser;
    }
}
