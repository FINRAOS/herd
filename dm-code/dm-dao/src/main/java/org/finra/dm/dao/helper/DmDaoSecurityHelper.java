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
package org.finra.dm.dao.helper;

import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.stereotype.Component;

/**
 * A DAO security helper class. Note that we have an application tier security helper which covers higher level security helper needs whereas this helper covers
 * lower level needs at the DAO tier. We might want to consider merging the application tier security helper methods into this tier/class although we would have
 * to move additional supporting classes.
 */
@Component
public class DmDaoSecurityHelper
{
    /**
     * The system user.
     */
    public static final String SYSTEM_USER = "SYSTEM";

    /**
     * Gets the currently logged in username. If no user is logged in, then the "SYSTEM" user is returned.
     *
     * @return the currently logged in user.
     */
    public String getCurrentUsername()
    {
        String username = SYSTEM_USER;
        if (SecurityContextHolder.getContext().getAuthentication() != null)
        {
            User user = (User) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
            username = user.getUsername();
        }
        return username;
    }
}
