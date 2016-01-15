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

    angular.module('herd').service('FileTypesService', FileTypesService);

    /**
     * Service that provides access to file types.
     * 
     * @class
     * @param UrlBuilderService
     * @param RestService
     */
    function FileTypesService(UrlBuilderService, RestService)
    {
        var service = this;

        service.UrlBuilderService = UrlBuilderService;
        service.RestService = RestService;
    }

    /**
     * Requests a list of file types.
     * 
     * @returns Promise with REST response
     */
    FileTypesService.prototype.getFileTypes = function()
    {
        var service = this;

        var url = service.UrlBuilderService.buildUrl({
            paths : [ 'fileTypes' ]
        });

        return service.RestService.request({
            url : url,
            type : 'GET'
        });
    };

})();