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
package org.finra.herd.service.impl;

import org.finra.herd.service.NotificationActionService;

public abstract class NotificationActionServiceImpl implements NotificationActionService
{
    protected static final String PARAM_BUSINESS_OBJECT_DATA = "notification_businessObjectData";

    protected static final String PARAM_BUSINESS_OBJECT_DATA_VERSION = "notification_businessObjectDataVersion";

    protected static final String PARAM_BUSINESS_OBJECT_DEFINITION_NAME = "notification_businessObjectDefinitionName";

    protected static final String PARAM_BUSINESS_OBJECT_DEFINITION_NAMESPACE = "notification_businessObjectDefinitionNamespace";

    protected static final String PARAM_BUSINESS_OBJECT_FORMAT_FILE_TYPE = "notification_businessObjectFormatFileType";

    protected static final String PARAM_BUSINESS_OBJECT_FORMAT_USAGE = "notification_businessObjectFormatUsage";

    protected static final String PARAM_BUSINESS_OBJECT_FORMAT_VERSION = "notification_businessObjectFormatVersion";

    protected static final String PARAM_CORRELATION_DATA = "notification_correlationData";

    protected static final String PARAM_NAMESPACE = "notification_namespace";

    protected static final String PARAM_NOTIFICATION_NAME = "notification_name";

    protected static final String PARAM_PARTITION_COLUMN_NAMES = "notification_partitionColumnNames";

    protected static final String PARAM_PARTITION_VALUES = "notification_partitionValues";

    protected NotificationActionServiceImpl()
    {
        // Prevent classes from instantiating except sub-classes.
    }
}
