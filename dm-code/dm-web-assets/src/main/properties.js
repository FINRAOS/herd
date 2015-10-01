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
/**
 * <p>
 * Provides a constant properties lookup for the entire application.
 * </p>
 * <p>
 * Property values:
 * </p>
 * <dl>
 * <dt>opsStatus.partitionValue.date.format</dt>
 * <dd>the date format for a date type partition value.</dd>
 * <dt>opsStatus.default.filter.date.range</dt>
 * <dd>the default date range to display when either start or end partition
 * value date is specified in the object status page.</dd>
 * <dt>opsStatus.default.filter.businessObjectFormatFileType</dt>
 * <dd>the default format file type filter value for the object status page.</dd>
 * <dt>opsStatus.default.filter.partitionKeyGroup</dt>
 * <dd>the default format partition key group filter value for the object
 * status page.</dd>
 * <dt>opsStatus.default.filter.storageName</dt>
 * <dd>the default storage name filter value for the object status page.</dd>
 * <dt>opsStatus.default.calendar.navigation.days</dt>
 * <dd>the number of days to scroll when moving calendar backwards or forwards
 * in the object status page.</dd>
 * </dl>
 */
(function ()
{
    'use strict';

    angular.module('dm').constant('properties', {
        'opsStatus.partitionValue.date.format': 'YYYY-MM-DD',
        'opsStatus.default.filter.date.range': 3 * 7,
        'opsStatus.default.filter.businessObjectFormatFileType': 'BZ',
        'opsStatus.default.filter.partitionKeyGroup': 'TRADE_DT',
        'opsStatus.default.filter.storageName': 'S3_MANAGED',
        'opsStatus.default.calendar.navigation.days': 7,
        'opsStatus.status.image.available': 'ic_check_circle_green.png'
    });
})();