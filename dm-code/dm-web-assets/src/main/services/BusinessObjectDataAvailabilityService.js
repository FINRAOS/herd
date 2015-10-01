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
(function()
{
    'use strict';

    angular.module('dm').service('BusinessObjectDataAvailabilityService', BusinessObjectDataAvailabilityService);

    /**
     * @constructor
     * @param {RestService} RestService
     * @param {UrlBuilderService} UrlBuilderService
     */
    function BusinessObjectDataAvailabilityService(RestService, UrlBuilderService)
    {
        this.RestService = RestService;
        this.UrlBuilderService = UrlBuilderService;
    }

    /**
     * @param {Object} getBusinessObjectDataAvailabilityRequest
     * @param {String} getBusinessObjectDataAvailabilityRequest.namespace
     * @param {String}
     *        getBusinessObjectDataAvailabilityRequest.businessObjectDefinitionName
     * @param {String}
     *        getBusinessObjectDataAvailabilityRequest.businessObjectFormatUsage
     * @param {String}
     *        getBusinessObjectDataAvailabilityRequest.businessObjectFormatFileType
     * @param {String}
     *        [getBusinessObjectDataAvailabilityRequest.businessObjectFormatVersion]
     * @param {Object}
     *        getBusinessObjectDataAvailabilityRequest.partitionValueFilters[]
     * @param {String}
     *        getBusinessObjectDataAvailabilityRequest.partitionValueFilters[].partitionKey
     * @param {String}
     *        getBusinessObjectDataAvailabilityRequest.partitionValueFilters[].partitionValues[]
     * @param {Object}
     *        getBusinessObjectDataAvailabilityRequest.partitionValueFilters[].partitionValueRange
     * @param {String}
     *        getBusinessObjectDataAvailabilityRequest.partitionValueFilters[].partitionValueRange.startPartitionValue
     * @param {String}
     *        getBusinessObjectDataAvailabilityRequest.partitionValueFilters[].partitionValueRange.endPartitionValue
     */
    BusinessObjectDataAvailabilityService.prototype.getBusinessObjectDataAvailability = function(getBusinessObjectDataAvailabilityRequest)
    {
        var url = this.UrlBuilderService.buildUrl({
            paths : [ 'businessObjectData', 'availability' ]
        });

        return this.RestService.request({
            url : url,
            method : 'POST',
            data : getBusinessObjectDataAvailabilityRequest
        });
    };
})();