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
package org.finra.herd.service.helper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.NotificationRegistrationKey;

@Component
public class BusinessObjectDataNotificationRegistrationHelper
{
    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    /**
     * Validates the business object data notification registration key. This method also trims the key parameters.
     *
     * @param key the business object data notification registration key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateBusinessObjectDataNotificationRegistrationKey(NotificationRegistrationKey key) throws IllegalArgumentException
    {
        // Validate.
        Assert.notNull(key, "A business object data notification registration key must be specified.");
        key.setNamespace(alternateKeyHelper.validateStringParameter("namespace", key.getNamespace()));
        key.setNotificationName(alternateKeyHelper.validateStringParameter("notification name", key.getNotificationName()));
    }
}
