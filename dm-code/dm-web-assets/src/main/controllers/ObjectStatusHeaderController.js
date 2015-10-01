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

    angular.module('dm').controller('ObjectStatusHeaderController', ObjectStatusHeaderController);

    /**
     * The controller for the ObjectStatusHeader component.
     * 
     * @class
     * @param $scope
     */
    function ObjectStatusHeaderController($scope)
    {
        var vm = this;

        /**
         * List of month headers
         */
        vm.monthHeaders = [];

        /**
         * List of date headers
         */
        vm.dateHeaders = [];

        var expectedPartitionValueLookup = {};

        initialize();

        /**
         * Initializes the controllers.
         * 
         * Attaches listeners to $scope events partitionValuesChanged and
         * expectedPartitionValuesChanged.
         */
        function initialize()
        {
            $scope.$on('partitionValuesChanged', onPartitionValuesChange);
            $scope.$on('expectedPartitionValuesChanged', onExpectedPartitionValuesChange);
        }

        /**
         * Event handler for partitionValuesChanged event.
         * 
         * Updates the headers.
         */
        function onPartitionValuesChange()
        {
            updateHeaders();
        }

        /**
         * Event handler for expectedPartitionValuesChanged event.
         * 
         * Updates expected partition lookup. Updates the headers.
         */
        function onExpectedPartitionValuesChange()
        {
            updateExpectedPartitionValueLookup();
            updateHeaders();
        }

        function updateExpectedPartitionValueLookup()
        {
            expectedPartitionValueLookup = {};
            _(vm.expectedPartitionValues).each(function(expectedPartitionValue)
            {
                expectedPartitionValueLookup[expectedPartitionValue] = true;
            });
        }

        /**
         * Updates the headers.
         * 
         * Month and date headers are re-populated based on the current list of
         * partition values.
         */
        function updateHeaders()
        {
            // Clear headers
            vm.monthHeaders.splice(0);
            vm.dateHeaders.splice(0);

            // Lookup month header by year-month pair
            var monthHeaderLookup = {};

            _(vm.partitionValues).each(function(partitionValue)
            {
                // Convert partition value into moment date
                var partitionValueDate = moment(partitionValue);

                // Create month ID. Concat of year and month
                var monthHeaderId = partitionValueDate.format('YYYY-MM');

                // Find month header
                var monthHeader = monthHeaderLookup[monthHeaderId];

                // If month header does not exist
                if (!monthHeader)
                {
                    // Create new month header
                    monthHeader = {
                        id : monthHeaderId,
                        content : partitionValueDate.format('MMM'),
                        colspan : 1
                    };

                    // Register to lookup
                    monthHeaderLookup[monthHeaderId] = monthHeader;

                    // Add to month headers
                    vm.monthHeaders.push(monthHeader);
                }
                // If month header exists
                else
                {
                    // Increment column span of header
                    monthHeader.colspan++;
                }

                // Create and add to list of date headers
                vm.dateHeaders.push({
                    id : partitionValue,
                    content : partitionValueDate.format('D'),
                    isNotExpected : !expectedPartitionValueLookup[partitionValue]
                });
            });
        }
    }
})();