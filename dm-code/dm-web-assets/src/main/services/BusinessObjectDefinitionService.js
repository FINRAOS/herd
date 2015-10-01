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

    angular.module('dm').service('BusinessObjectDefinitionService', BusinessObjectDefinitionService);

    /**
     * A service which provides logic for business object definitions.
     * 
     * @param {RestService} RestService
     * @param {UrlBuilderService} UrlBuilderService
     */
    function BusinessObjectDefinitionService(RestService, UrlBuilderService)
    {
        this.RestService = RestService;
        this.UrlBuilderService = UrlBuilderService;
    }

    /**
     * @param {Object} getBusinessObjectDefinitionKeysRequest
     * @param {String} getBusinessObjectDefinitionKeysRequest.namespaceCode
     */
    BusinessObjectDefinitionService.prototype.getBusinessObjectDefinitionKeys = function(getBusinessObjectDefinitionKeysRequest)
    {
        var namespaceCode = getBusinessObjectDefinitionKeysRequest.namespaceCode;

        var url = this.UrlBuilderService.buildUrl({
            paths : [ 'businessObjectDefinitions', 'namespaces', namespaceCode ]
        });

        return this.RestService.request({
            url : url,
            method : 'GET'
        });
    };

})();