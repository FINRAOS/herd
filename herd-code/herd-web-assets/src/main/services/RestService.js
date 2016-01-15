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

    angular.module('herd').service('RestService', RestService);

    /**
     * Service that provides access to herd REST API.
     * 
     * @param $location
     * @param $http
     */
    function RestService($location, $http, properties)
    {
        this.$location = $location;
        this.$http = $http;
        this.properties = properties;
    }

    /**
     * Makes a REST request. The URL of the given options request will be
     * prefixed such that the request URL is an absolute URL. The requested URL
     * must begin with a path separator (/).
     * 
     * @param options - the options object as defined by $http
     * @returns Promise - the promised response
     */
    RestService.prototype.request = function(options)
    {
        // construct the absolute URL based on current $location
        var protocol = this.$location.protocol();
        var host = this.$location.host();
        var port = this.$location.port();
        var contextRoot = this.properties.contextRoot;

        var path = protocol + '://' + host;
        if (port)
        {
            path += ':' + port;
        }
        path += '/' + contextRoot + '/rest';

        options = _.clone(options); // shallow copy the options
        options.url = path + options.url; // prefix url
        return this.$http(options); // make http request
    };

})();