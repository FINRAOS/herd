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
 * @namespace services
 */
(function()
{
    'use strict';

    /**
     * The business object format defined by Business Object Format GET REST
     * response.
     * 
     * @typedef BusinessObjectFormat
     * @type {Object}
     */

    angular.module('dm').service('BusinessObjectFormatService', BusinessObjectFormatService);

    /**
     * A service which provides logic for business object formats.
     * 
     * @param {RestService} RestService
     * 
     * @param {UrlBuilderService} UrlBuilderService
     */
    function BusinessObjectFormatService(RestService, UrlBuilderService)
    {
        this.RestService = RestService;
        this.UrlBuilderService = UrlBuilderService;
    }

    /**
     * Returns a promise to return a list of business object format keys.
     * 
     * @param {Object} getBusinessObjectFormatKeysRequest - the business object
     *        definition
     * @param {String} getBusinessObjectFormatKeysRequest.namespace - namespace
     *        code
     * @param {String}
     *        getBusinessObjectFormatKeysRequest.businessObjectDefinitionName -
     *        name of business object definition
     * @param {Boolean} [getBusinessObjectFormatKeysRequest.latestVersion=false] -
     *        true if the returned keys should only contain latest version,
     *        false to return all versions.
     * @returns Promise - promise to return a list of business object format
     */
    BusinessObjectFormatService.prototype.getBusinessObjectFormatKeys = function(getBusinessObjectFormatKeysRequest)
    {
        var namespace = getBusinessObjectFormatKeysRequest.namespace;
        var businessObjectDefinitionName = getBusinessObjectFormatKeysRequest.businessObjectDefinitionName;
        var latestVersion = getBusinessObjectFormatKeysRequest.latestVersion === true;

        var url = this.UrlBuilderService.buildUrl({
            paths : [ 'businessObjectFormats', 'namespaces', namespace, 'businessObjectDefinitionNames', businessObjectDefinitionName ]
        });

        return this.RestService.request({
            url : url,
            params : {
                'latestBusinessObjectFormatVersion' : latestVersion
            },
            method : 'GET'
        });
    };

    /**
     * @param {Object} getBusinessObjectFormatRequest
     * @param {String} getBusinessObjectFormatRequest.namespace
     * @param {String}
     *        getBusinessObjectFormatRequest.businessObjectDefinitionName
     * @param {String} getBusinessObjectFormatRequest.businessObjectFormatUsage
     * @param {String}
     *        getBusinessObjectFormatRequest.businessObjectFormatFileType
     * @param {Number}
     *        [getBusinessObjectFormatRequest.businessObjectFormatVersion]
     */
    BusinessObjectFormatService.prototype.getBusinessObjectFormat = function(getBusinessObjectFormatRequest)
    {
        var namespace = getBusinessObjectFormatRequest.namespace;
        var businessObjectDefinitionName = getBusinessObjectFormatRequest.businessObjectDefinitionName;
        var businessObjectFormatUsage = getBusinessObjectFormatRequest.businessObjectFormatUsage;
        var businessObjectFormatFileType = getBusinessObjectFormatRequest.businessObjectFormatFileType;
        var businessObjectFormatVersion = getBusinessObjectFormatRequest.businessObjectFormatVersion;

        var url = this.UrlBuilderService.buildUrl({
            paths : [
                'businessObjectFormats',
                'namespaces',
                namespace,
                'businessObjectDefinitionNames',
                businessObjectDefinitionName,
                'businessObjectFormatUsages',
                businessObjectFormatUsage,
                'businessObjectFormatFileTypes',
                businessObjectFormatFileType ]
        });

        return this.RestService.request({
            url : url,
            params : {
                'businessObjectFormatVersion' : businessObjectFormatVersion
            },
            method : 'GET'
        });
    };
})();