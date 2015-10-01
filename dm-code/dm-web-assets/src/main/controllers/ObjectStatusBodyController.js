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

    angular.module('dm').controller('ObjectStatusBodyController', ObjectStatusBodyController);

    /**
     * The controller for ObjectStatusBody component.
     * 
     * @class
     * @memberof controllers
     * @param {Object} properties - system properties
     * @param {$scope} $scope - the scope of the controller
     */
    function ObjectStatusBodyController(properties, $scope)
    {
        var vm = this;
        
        vm.properties = properties;

        /**
         * The rows of the table body.
         */
        vm.rows = [];

        /**
         * A lookup of rows by their IDs.
         */
        var rowLookup = {};

        /**
         * A lookup of expected partition values by their value.
         */
        var expectedPartitionValueLookup = {};

        initialize();

        /**
         * Initializes the controller.
         * 
         * Attaches event listeners to $scope events for the following events:
         * 
         * <ul>
         * <li>partitionValuesChanged</li>
         * <li>businessObjectFormatsChanged</li>
         * <li>businessObjectDataAvailabilitiesChanged</li>
         * <li>expectedPartitionValuesChanged</li>
         * </ul>
         */
        function initialize()
        {
            $scope.$on('partitionValuesChanged', onPartitionValuesChange);
            $scope.$on('businessObjectFormatsChanged', onBusinessObjectFormatsChange);
            $scope.$on('businessObjectDataAvailabilitiesChanged', onBusinessObjectDataAvailabilitiesChange);
            $scope.$on('expectedPartitionValuesChanged', onExpectedPartitionValuesChange);
        }

        /**
         * Callback on 'partitionValuesChanged' event.
         * 
         * Calls {@link controllers.ObjectStatusBodyController~updateRows}
         */
        function onPartitionValuesChange()
        {
            updateRows();
        }

        /**
         * Callback on 'businessObjectFormatsChanged' event.
         * 
         * Calls {@link controllers.ObjectStatusBodyController~setRows}
         */
        function onBusinessObjectFormatsChange()
        {
            setRows();
        }

        /**
         * Callback on 'businessObjectDataAvailabilitiesChanged' event.
         * 
         * Calls {@link controllers.ObjectStatusBodyController~updateRows}
         */
        function onBusinessObjectDataAvailabilitiesChange()
        {
            updateRows();
        }

        /**
         * Callback on 'expectedPartitionValuesChanged' event.
         * 
         * Calls
         * {@link controllers.ObjectStatusBodyController~updateExpectedPartitionValueLookup}
         * then {@link controllers.ObjectStatusBodyController~updateRows}
         */
        function onExpectedPartitionValuesChange()
        {
            updateExpectedPartitionValueLookup();
            updateRows();
        }

        /**
         * Updates
         * {@link controllers.ObjectStatusBodyController.expectedPartitionValueLookup}
         * lookup based on the
         * {@link controllers.ObjectStatusBodyController.expectedPartitionValues}
         */
        function updateExpectedPartitionValueLookup()
        {
            expectedPartitionValueLookup = {};
            _(vm.expectedPartitionValues).each(function(expectedPartitionValue)
            {
                expectedPartitionValueLookup[expectedPartitionValue] = true;
            });
        }

        /**
         * Clears rows and re-populates it based on the current
         * businessObjectFormats.
         */
        function setRows()
        {
            vm.rows.splice(0);

            rowLookup = {};
            _(vm.businessObjectFormats).each(function(businessObjectFormat)
            {
                var id = getRowId(businessObjectFormat);
                var row = {
                    id : id,
                    namespace : businessObjectFormat.namespace,
                    businessObjectDefinitionName : businessObjectFormat.businessObjectDefinitionName,
                    statuses : [],
                    isError : businessObjectFormat.isError
                };
                rowLookup[id] = row;
                vm.rows.push(row);
            });

            updateRows();
        }

        /**
         * Creates a unique id for a row for the given business object format.
         */
        function getRowId(businessObjectFormat)
        {
            var builder = [];
            builder.push(businessObjectFormat.namespace);
            builder.push(businessObjectFormat.businessObjectDefinitionName);
            builder.push(businessObjectFormat.businessObjectFormatUsage);
            builder.push(businessObjectFormat.businessObjectFormatFileType);
            return builder.join(':');
        }

        /**
         * Updates existing rows' statuses based on the current
         * businessObjectDataAvailabilities.
         */
        function updateRows()
        {
            /*
             * For each row, populate the list of statuses based on the current
             * partition values and expected partition values.
             * 
             * This is done separately from the availabilities loop because the
             * list of availabilities are expected to be populated later. We
             * want to pre-populate the statuses with as much information we
             * have at this point of execution which does not depend on
             * availabilities.
             */
            _(vm.rows).each(function(row)
            {
                row.statuses.splice(0); // reset statuses
                _(vm.partitionValues).each(function(partitionValue)
                {
                    /*
                     * Add status without any image url. ID is the partition
                     * value.
                     */
                    row.statuses.push({
                        id : partitionValue,
                        isNotExpected : !expectedPartitionValueLookup[partitionValue]
                    });
                });
            });

            /*
             * For each availability, find the row which contains the format,
             * and update the statuses.
             */
            _(vm.businessObjectDataAvailabilities).each(function(businessObjectDataAvailability)
            {
                /*
                 * Create a lookup of availability statuses by the partition
                 * value.
                 */
                var availableStatuses = {};
                _(businessObjectDataAvailability.availableStatuses).each(function(availableStatus)
                {
                    availableStatuses[availableStatus.partitionValue] = true;
                });

                // Find the row which contains the availability's format
                var row = rowLookup[getRowId(businessObjectDataAvailability)];

                // If row exists
                if (row && !row.isError)
                {
                    row.isError = businessObjectDataAvailability.isError === true;

                    /*
                     * Create a lookup of statuses by its ID.
                     */
                    var statusLookup = {};
                    _(row.statuses).each(function(status)
                    {
                        statusLookup[status.id] = status;
                    });

                    /*
                     * For each partition value currently in the context.
                     */
                    _(vm.partitionValues).each(function(partitionValue)
                    {
                        /*
                         * Check whether an available status exists for the
                         * partition value, and set an image if exists.
                         */
                        var imageUrl = null;
                        if (availableStatuses[partitionValue])
                        {
                            imageUrl = vm.properties['opsStatus.status.image.available'];
                        }

                        // Set the image of the status
                        statusLookup[partitionValue].imageUrl = imageUrl;
                    });
                }
            });
        }
    }
})();