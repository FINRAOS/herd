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
package org.finra.herd.core.helper;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.security.cert.Certificate;
import java.util.Collection;
import java.util.Collections;

/**
 * Helper for {@link SecurityManager} related operations.
 */
public class SecurityManagerHelper
{
    /**
     * Returns true if a security manager is enabled in the current JVM context.
     * 
     * @return true if enabled, false if not.
     */
    public static boolean isSecurityManagerEnabled()
    {
        return System.getSecurityManager() != null;
    }

    /**
     * {@link #doPrivileged(PrivilegedAction, Collection)} with no permissions.
     * 
     * @param <T> the return type
     * @param privilegedAction the {@link PrivilegedAction} to execute
     * @return return value of the privileged action
     */
    public static <T> T doPrivileged(PrivilegedAction<T> privilegedAction)
    {
        return doPrivileged(privilegedAction, Collections.<Permission>emptyList());
    }

    /**
     * Executes the given {@link PrivilegedAction} using the given collection of {@link Permission}.
     * 
     * @param <T> the return type
     * @param privilegedAction the {@link PrivilegedAction} to execute
     * @param permissions collection of permissions to apply. May be empty but not null.
     * @return return value of the privileged action
     */
    public static <T> T doPrivileged(PrivilegedAction<T> privilegedAction, Collection<Permission> permissions)
    {
        CodeSource codeSource = new CodeSource(null, (Certificate[]) null);
        PermissionCollection permissionCollection = new Permissions();
        for (Permission permission : permissions)
        {
            permissionCollection.add(permission);
        }
        ProtectionDomain protectionDomain = new ProtectionDomain(codeSource, permissionCollection);
        ProtectionDomain[] protectionDomains = {protectionDomain};
        AccessControlContext accessControlContext = new AccessControlContext(protectionDomains);
        return AccessController.doPrivileged(privilegedAction, accessControlContext);
    }
}
