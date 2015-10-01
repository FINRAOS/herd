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

    angular.module('dm').service('UrlBuilderService', UrlBuilderService);

    /**
     * Service which provides logic to create URLs.
     */
    function UrlBuilderService()
    {

    }

    /**
     * @param {Object} buildUrlRequest
     * @param {String} buildUrlRequest.paths[]
     * @returns {Promise} a promise
     */
    UrlBuilderService.prototype.buildUrl = function(buildUrlRequest)
    {
        var builder = [];

        _(buildUrlRequest.paths).each(function(path)
        {
            path = encodeURIComponent(path);
            path = path.replace('%20', '+');
            builder.push('/');
            builder.push(path);
        });

        return builder.join('');
    };
})();