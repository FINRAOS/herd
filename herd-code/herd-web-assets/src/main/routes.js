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
 * The route configuration for the app.
 * </p>
 * <p>
 * Available routes:
 * </p>
 * <p>
 * <b>/objectStatus</b>
 * <p>
 * Displays the object status page.
 * </p>
 * </p>
 */
(function()
{
    'use strict';

    angular.module('herd').config(function($routeProvider)
    {
        $routeProvider.when('/objectStatus', {
            templateUrl : 'main/templates/ObjectStatusTemplate.html',
            controller : 'ObjectStatusController',
            controllerAs : 'vm',
            reloadOnSearch : false
        });

        $routeProvider.otherwise('/objectStatus');
    });

})();