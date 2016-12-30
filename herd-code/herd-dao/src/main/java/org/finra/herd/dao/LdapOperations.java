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

import java.util.List;

import org.springframework.ldap.core.AttributesMapper;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.query.LdapQuery;

/**
 * Wrapper interface for {@link org.springframework.ldap.core.LdapTemplate } operations.
 */
public interface LdapOperations
{
    /**
     * Executes {@link org.springframework.ldap.core.LdapTemplate#search(org.springframework.ldap.query.LdapQuery,
     * org.springframework.ldap.core.AttributesMapper)}.
     *
     * @param ldapTemplate the LDAP template to use
     * @param query the LDAP query specification
     * @param mapper the <code>Attributes</code> to supply all found Attributes to
     *
     * @return the list of objects constructed by the given {@link org.springframework.ldap.core.AttributesMapper}
     */
    <T> List<T> search(LdapTemplate ldapTemplate, LdapQuery query, AttributesMapper<T> mapper);
}
