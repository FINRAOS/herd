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
package org.finra.dm.service;

import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistration;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistrationCreateRequest;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistrationKey;
import org.finra.dm.model.api.xml.BusinessObjectDataNotificationRegistrationKeys;

/**
 * The business object data notification service.
 */
public interface BusinessObjectDataNotificationRegistrationService
{
    public BusinessObjectDataNotificationRegistration createBusinessObjectDataNotificationRegistration(
        BusinessObjectDataNotificationRegistrationCreateRequest request);

    public BusinessObjectDataNotificationRegistration getBusinessObjectDataNotificationRegistration(BusinessObjectDataNotificationRegistrationKey key);

    public BusinessObjectDataNotificationRegistration deleteBusinessObjectDataNotificationRegistration(BusinessObjectDataNotificationRegistrationKey key);

    public BusinessObjectDataNotificationRegistrationKeys getBusinessObjectDataNotificationRegistrations(String namespace);
}
