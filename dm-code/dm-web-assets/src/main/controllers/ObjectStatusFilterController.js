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

    angular.module('dm').controller('ObjectStatusFilterController', ObjectStatusFilterController);

    /**
     * Controller for ObjectStatusFilterDirective.
     * 
     * @class
     * @param $scope
     * @param FileTypesService
     */
    function ObjectStatusFilterController($scope)
    {
        var vm = this;

        /**
         * The partition value range from the filter controls.
         */
        vm.partitionValueRange = {};
        vm.partitionValueRange.startDate = null;
        vm.partitionValueRange.endDate = null;

        initialize();

        /**
         * Initializes the controller.
         * 
         * Retrieves the list of file types from the service.
         * 
         * Attaches listeners to filter's start and end partition values such
         * that it updates the controller's value ranges.
         */
        function initialize()
        {
            $scope.$watch('vm.filter.startPartitionValue', function()
            {
                vm.partitionValueRange.startDate = vm.filter.startPartitionValue;
            });

            $scope.$watch('vm.filter.endPartitionValue', function()
            {
                vm.partitionValueRange.endDate = vm.filter.endPartitionValue;
            });
        }
    }

    /**
     * Updates the filter's value range based on the UI control.
     */
    ObjectStatusFilterController.prototype.onPartitionValueRangeChanged = function()
    {
        var vm = this;
        vm.filter.startPartitionValue = vm.partitionValueRange.startDate;
        vm.filter.endPartitionValue = vm.partitionValueRange.endDate;
    };
})();