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

import org.springframework.stereotype.Component;

import org.finra.herd.dao.credstash.CredStash;
import org.finra.herd.dao.credstash.JCredStashWrapper;

/**
 * CredStashFactory
 * <p>
 * Factory to return a CredStash interface. This is used for decoupling the CredStash interface from its implementation and so that we can easily mock the
 * credstash interface.
 */
@Component
public class CredStashFactory
{
    /**
     * Method to retrieve a credstash interface for getting a credential from credstash.
     * @param region the region of the credstash instance where  the credential is stored
     * @param tableName the name of the credstash table where the credential is stored
     *
     * @return the credstash interface that includes the getCredential method
     */
    public CredStash getCredStash(String region, String tableName)
    {
        return new JCredStashWrapper(region, tableName);
    }
}
