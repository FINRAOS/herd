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

    angular.module('herd').service('ExpectedPartitionValuesService', ExpectedPartitionValuesService);

    /**
     * @param {UrlBuilderService} UrlBuilderService
     * @param {RestService} RestService
     */
    function ExpectedPartitionValuesService(UrlBuilderService, RestService)
    {
        this.UrlBuilderService = UrlBuilderService;
        this.RestService = RestService;
    }

    /**
     * Returns a promise which responds with a list of expected partition
     * values.
     * 
     * @param {Object} getExpectedPartitionValuesRequest
     * @param {String} getExpectedPartitionValuesRequest.partitionKeyGroupName
     * @param {String}
     *        [getExpectedPartitionValuesRequest.startExpectedPartitionValue]
     * @param {String}
     *        [getExpectedPartitionValuesRequest.endExpectedPartitionValue]
     * @returns {Promise} promise -
     *          <dl>
     *          <dt>{Object} response.data</dt>
     *          <dt>{String} response.data.partitionKeyGroupName</dt>
     *          <dt>{String} response.data.expectedPartitionValues[]</dt>
     *          </dl>
     */
    ExpectedPartitionValuesService.prototype.getExpectedPartitionValues = function(getExpectedPartitionValuesRequest)
    {
        var url = this.UrlBuilderService.buildUrl({
            paths : [ 'expectedPartitionValues', 'partitionKeyGroups', getExpectedPartitionValuesRequest.partitionKeyGroupName ]
        });

        return this.RestService.request({
            url : url,
            params : {
                startExpectedPartitionValue : getExpectedPartitionValuesRequest.startExpectedPartitionValue,
                endExpectedPartitionValue : getExpectedPartitionValuesRequest.endExpectedPartitionValue
            },
            method : 'GET'
        });
    };
})();