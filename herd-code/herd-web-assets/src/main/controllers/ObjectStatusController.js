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

    angular.module('herd').controller('ObjectStatusController', ObjectStatusController);

    /**
     * The controller for the ObjectStatus page.
     * 
     * @constructor
     * @param {$scope} $scope
     * @param {$location} $location
     * @param {Object} properties
     * @param {PageService} PageService
     * @param {DateService} DateService
     * @param {NamespaceService} NamespaceService
     * @param {BusinessObjectDefinitionService} BusinessObjectDefinitionService
     * @param {BusinessObjectFormatService} BusinessObjectFormatService
     * @param {BusinessObjectDataAvailabilityService}
     *        BusinessObjectDataAvailabilityService
     * @param {ExpectedPartitionValuesService} ExpectedPartitionValuesService
     */
    function ObjectStatusController($scope, $location, properties, PageService, DateService, NamespaceService, BusinessObjectDefinitionService,
        BusinessObjectFormatService, BusinessObjectDataAvailabilityService, ExpectedPartitionValuesService, FileTypesService)
    {
        var vm = this;

        vm.$scope = $scope;
        vm.$location = $location;
        vm.properties = properties;
        vm.PageService = PageService;
        vm.DateService = DateService;
        vm.partitionValueDateFormat = properties['opsStatus.partitionValue.date.format'];
        vm.defaultFilterDateRange = properties['opsStatus.default.filter.date.range'];
        vm.defaultFilterBusinessObjectFormatFileType = properties['opsStatus.default.filter.businessObjectFormatFileType'];
        vm.defaultFilterPartitionKeyGroup = properties['opsStatus.default.filter.partitionKeyGroup'];
        vm.defaultFilterStorageName = properties['opsStatus.default.filter.storageName'];
        vm.defaultCalendarNavigationDays = properties['opsStatus.default.calendar.navigation.days'];
        vm.NamespaceService = NamespaceService;
        vm.BusinessObjectDefinitionService = BusinessObjectDefinitionService;
        vm.BusinessObjectFormatService = BusinessObjectFormatService;
        vm.BusinessObjectDataAvailabilityService = BusinessObjectDataAvailabilityService;
        vm.ExpectedPartitionValuesService = ExpectedPartitionValuesService;
        vm.FileTypesService = FileTypesService;

        /**
         * The filter object.
         */
        vm.filter = {};
        vm.filter.businessObjectDefinitionName = null;
        vm.filter.startPartitionValue = null;
        vm.filter.endPartitionValue = null;
        vm.filter.businessObjectFormatFileType = null;
        vm.filter.storageName = null;
        vm.filter.partitionKeyGroup = null;
        vm.filter.fileTypes = [];

        /**
         * List of partition values between the current filter's start and end
         * values.
         */
        vm.partitionValues = [];

        /**
         * The list of formats retrieved from the service which match filters.
         */
        vm.businessObjectFormats = [];

        /**
         * The list of availabilities for the current list of formats.
         */
        vm.businessObjectDataAvailabilities = [];

        /**
         * The list of expected partition values within the range of the current
         * filter's start and end values.
         */
        vm.expectedPartitionValues = [];

        vm.doFilter = doFilter;
        vm.calendarBack = calendarBack;
        vm.calendarForward = calendarForward;

        initialize();

        /**
         * Initializes the controller.
         * 
         * Sets the page's title.
         * 
         * Initializes filters.
         * 
         * Executes filters.
         */
        function initialize()
        {
            vm.PageService.title = 'herd Object Status';

            vm.FileTypesService.getFileTypes().then(onGetFileTypesSuccess, onGetFileTypesError);
        }

        function onGetFileTypesSuccess(response)
        {
            vm.filter.fileTypes.splice(0);
            var fileTypeKeys = response.data.fileTypeKeys;
            _(fileTypeKeys).each(function(fileTypeKey)
            {
                vm.filter.fileTypes.push(fileTypeKey.fileTypeCode);
            });

            initializeFilters();
            doFilter();
        }

        function onGetFileTypesError()
        {
            vm.errorMessage = 'Error retrieving file types';
        }

        /**
         * Moves calendar dates back by configured amount.
         * 
         * Refreshes data on the page.
         */
        function calendarBack()
        {
            vm.filter.startPartitionValue = moment(vm.filter.startPartitionValue).subtract(vm.defaultCalendarNavigationDays, 'days').toDate();
            vm.filter.endPartitionValue = moment(vm.filter.endPartitionValue).subtract(vm.defaultCalendarNavigationDays, 'days').toDate();
            calendarRefresh();
        }

        /**
         * Moves calendar dates forward by configured amount.
         * 
         * Refreshes data on the page.
         */
        function calendarForward()
        {
            vm.filter.startPartitionValue = moment(vm.filter.startPartitionValue).add(vm.defaultCalendarNavigationDays, 'days').toDate();
            vm.filter.endPartitionValue = moment(vm.filter.endPartitionValue).add(vm.defaultCalendarNavigationDays, 'days').toDate();
            calendarRefresh();
        }

        /**
         * Refreshes data on the page.
         * 
         * Updates $location query parameters. Populates the partition values
         * from service. Populates expected partition values from service.
         * Retrieves availabilities.
         * 
         * Does not retrieve formats again - will reuse existing formats.
         */
        function calendarRefresh()
        {
            setQueryParams();
            populatePartitionValues();
            populateExpectedPartitionValues();
            getBusinessObjectDataAvailabilities();
        }

        /**
         * Populates the expected partition values based on the current filter.
         */
        function populateExpectedPartitionValues()
        {
            var getExpectedPartitionValuesRequest = {
                partitionKeyGroupName : vm.filter.partitionKeyGroup,
                startExpectedPartitionValue : formatDate(vm.filter.startPartitionValue),
                endExpectedPartitionValue : formatDate(vm.filter.endPartitionValue)
            };

            var getExpectedPartitionValuesPromise = ExpectedPartitionValuesService.getExpectedPartitionValues(getExpectedPartitionValuesRequest);
            getExpectedPartitionValuesPromise.then(onGetExpectedPartitionValuesSuccess, onGetExpectedPartitionValuesError);
        }

        /**
         * The callback function for
         * ExpectedPartitionValuesService.getExpectedPartitionValues.
         * 
         * Populates the expected partition value list.
         */
        function onGetExpectedPartitionValuesSuccess(response)
        {
            _(response.data.expectedPartitionValues).each(function(expectedPartitionValue)
            {
                vm.expectedPartitionValues.push(expectedPartitionValue);
            });
            vm.$scope.$broadcast('expectedPartitionValuesChanged');
        }

        function onGetExpectedPartitionValuesError()
        {
            vm.errorMessage = 'Error retrieving expected partition values';
        }

        /**
         * Initializes filters based on query parameters. If any filter value is
         * not specified in the query parameter, the app configured default
         * values are used.
         * 
         * The default end partition value is the current date. The default
         * start partition value is the end partition value minus the
         * pre-configured number of days.
         */
        function initializeFilters()
        {
            var queryParams = $location.search();

            /*
             * Parse filter values from the URL query parameters.
             */
            vm.filter.businessObjectDefinitionName = queryParams.businessObjectDefinitionName || null;
            vm.filter.startPartitionValue = parseDate(queryParams.startPartitionValue);
            vm.filter.endPartitionValue = parseDate(queryParams.endPartitionValue);
            vm.filter.businessObjectFormatFileType = queryParams.businessObjectFormatFileType || null;
            vm.filter.storageName = queryParams.storageName || null;
            vm.filter.partitionKeyGroup = queryParams.partitionKeyGroup || null;

            /*
             * Set filters to default values if not specified in the query
             * parameters.
             */
            if (!vm.filter.endPartitionValue)
            {
                if (vm.filter.startPartitionValue)
                {
                    vm.filter.endPartitionValue = moment(vm.filter.startPartitionValue).add(vm.defaultFilterDateRange - 1, 'days').toDate();
                }
                else
                {
                    vm.filter.endPartitionValue = vm.DateService.now().toDate();
                }
            }
            vm.filter.startPartitionValue = vm.filter.startPartitionValue ||
                moment(vm.filter.endPartitionValue).subtract(vm.defaultFilterDateRange - 1, 'days').toDate();
            vm.filter.businessObjectFormatFileType = vm.filter.businessObjectFormatFileType || vm.defaultFilterBusinessObjectFormatFileType;
            vm.filter.storageName = vm.filter.storageName || vm.defaultFilterStorageName;
            vm.filter.partitionKeyGroup = vm.filter.partitionKeyGroup || vm.defaultFilterPartitionKeyGroup;
        }

        /**
         * Validates the filters, and if valid,
         * 
         * Resets the list of formats and availabilities. This clears the UI of
         * all data while filtering.
         * 
         * Sets the filter values into the query parameter. Updates the URL to
         * be referrable by the current filters.
         * 
         * Sets the list of partition values based on the current filter's start
         * and end partition values.
         * 
         * Retrieves formats and availabilities based on the current filter.
         * 
         * Broadcasts the event businessObjectFormatsChanged to let child
         * directives to update accordingly.
         */
        function doFilter()
        {
            if (validateFilters())
            {
                vm.businessObjectFormats.splice(0);
                vm.businessObjectDataAvailabilities.splice(0);

                setQueryParams();
                populatePartitionValues();
                populateExpectedPartitionValues();

                vm.NamespaceService.getNamespaceKeys().then(onGetNamespaceKeysSuccess, onGetNamespaceKeysError);

                vm.$scope.$broadcast('businessObjectFormatsChanged');
            }
        }

        /**
         * Validates the filters and returns "true" if all fields are valid.
         * Sets the error message if there are any validation errors.
         * 
         * Start and end partition values are required.
         * 
         * Start partition value must be less than end partition value.
         */
        function validateFilters()
        {
            vm.errorMessage = null;

            var businessObjectDefinitionName = vm.filter.businessObjectDefinitionName;
            var startPartitionValue = vm.filter.startPartitionValue;
            var endPartitionValue = vm.filter.endPartitionValue;

            // Fail validation if definition name is empty.
            /*
             * This is really a placeholder and not meant to be a validation. We
             * just don't want the filter to execute when the field is empty,
             * but not display error message.
             */
            if (!businessObjectDefinitionName || businessObjectDefinitionName.length <= 0)
            {
                return false;
            }

            if (!startPartitionValue)
            {
                vm.errorMessage = 'Start partition value is required';
                return false;
            }

            if (!endPartitionValue)
            {
                vm.errorMessage = 'End partition value is required';
                return false;
            }

            if (startPartitionValue.valueOf() >= endPartitionValue.valueOf())
            {
                vm.errorMessage = 'The start partition value must be less than the end partition value';
                return false;
            }
            
            return true;
        }

        /**
         * Populates the list of partition values based on the current filter's
         * start and end partition value.
         * 
         * Broadcasts partitionValuesChanged event.
         */
        function populatePartitionValues()
        {
            vm.partitionValues.splice(0);
            var date = moment(vm.filter.startPartitionValue);
            while (date.valueOf() <= vm.filter.endPartitionValue.valueOf())
            {
                vm.partitionValues.push(formatDate(date));
                date.add(1, 'days');
            }
            vm.$scope.$broadcast('partitionValuesChanged');
        }

        /**
         * Sets the query parameters based on the current filters. This makes
         * the filter settings URL referrable.
         */
        function setQueryParams()
        {
            $location.search('businessObjectDefinitionName', vm.filter.businessObjectDefinitionName);
            $location.search('startPartitionValue', formatDate(vm.filter.startPartitionValue));
            $location.search('endPartitionValue', formatDate(vm.filter.endPartitionValue));
            $location.search('businessObjectFormatFileType', vm.filter.businessObjectFormatFileType);
            $location.search('storageName', vm.filter.storageName);
            $location.search('partitionKeyGroup', vm.filter.partitionKeyGroup);
        }

        /**
         * Formats the given date using the current date format.
         * 
         * See DateService.formatDate
         */
        function formatDate(date)
        {
            return vm.DateService.formatDate(date, vm.partitionValueDateFormat);
        }

        /**
         * Parses the given string using the current date format. Returns null
         * if string is in invalid format.
         * 
         * See DateService.parseDate
         */
        function parseDate(string)
        {
            return vm.DateService.parseDate(string, vm.partitionValueDateFormat);
        }

        /**
         * Callback function for NamespaceService.getNamespaceKeys().
         * 
         * For each namespace key returned, calls
         * BusinessObjectDefinitionService.getBusinessObjectDefinitionKeys().
         */
        function onGetNamespaceKeysSuccess(response)
        {
            _(response.data.namespaceKeys).each(function(namespaceKey)
            {
                var getBusinessObjectDefinitionKeysPromise = vm.BusinessObjectDefinitionService.getBusinessObjectDefinitionKeys(namespaceKey);
                getBusinessObjectDefinitionKeysPromise.then(onGetBusinessObjectDefinitionKeysSuccess, function()
                {
                    onGetBusinessObjectDefinitionKeysError(namespaceKey);
                });
            });
        }

        function onGetNamespaceKeysError()
        {
            vm.errorMessage = 'Error retrieving list of namespaces';
        }

        /**
         * Callback function for
         * BusinessObjectDefinitionService.getBusinessObjectDefinitionKeys().
         * 
         * For each definition keys returned, calls
         * BusinessObjectFormatService.getBusinessObjectFormatKeys() if the
         * definition name matches the filter value. The
         * getBusinessObjectFormatKeys() will only return latest format
         * versions.
         */
        function onGetBusinessObjectDefinitionKeysSuccess(response)
        {
            _(response.data.businessObjectDefinitionKeys).each(
                function(businessObjectDefinitionKey)
                {
                    var filterDefinitionName = vm.filter.businessObjectDefinitionName;
                    var definitionName = businessObjectDefinitionKey.businessObjectDefinitionName;

                    /*
                     * Filter matches if current definition name filter is not
                     * set, or if the definition name contains the filter value
                     * ignoring case.
                     * '*' matches all definition names.
                     */
                    if (filterDefinitionName === '*' || herd.utils.containsIgnoreCase(definitionName, filterDefinitionName))
                    {
                        var getBusinessObjectFormatKeysRequest = {
                            namespace : businessObjectDefinitionKey.namespace,
                            businessObjectDefinitionName : definitionName,
                            latestVersion : true
                        };

                        vm.BusinessObjectFormatService.getBusinessObjectFormatKeys(getBusinessObjectFormatKeysRequest).then(
                            onGetBusinessObjectFormatKeysSuccess, function()
                            {
                                onGetBusinessObjectFormatKeysError(businessObjectDefinitionKey);
                            });
                    }
                });
        }

        function onGetBusinessObjectDefinitionKeysError(namespaceKey)
        {
            addBusinessObjectFormat({
                namespace : namespaceKey.namespaceCode,
                isError : true
            });
        }

        /**
         * Callback function for
         * BusinessObjectFormatService.getBusinessObjectFormatKeys().
         * 
         * For each format keys returned, calls
         * BusinessObjectFormatService.getBusinessObjectFormat() if the format's
         * file type matches the filter value.
         */
        function onGetBusinessObjectFormatKeysSuccess(response)
        {
            _(response.data.businessObjectFormatKeys).each(function(businessObjectFormatKey)
            {
                var filterFileType = vm.filter.businessObjectFormatFileType;
                var fileType = businessObjectFormatKey.businessObjectFormatFileType;

                /*
                 * Filter matches if the file type filter is not set, or the
                 * file type equals the filter value ignoring case.
                 */
                if (!filterFileType || herd.utils.equalsIgnoreCase(filterFileType, fileType))
                {
                    vm.BusinessObjectFormatService.getBusinessObjectFormat(businessObjectFormatKey).then(onGetBusinessObjectFormatSuccess, function()
                    {
                        onGetBusinessObjectFormatError(businessObjectFormatKey);
                    });
                }
            });
        }

        function onGetBusinessObjectFormatKeysError(businessObjectDefinitionKey)
        {
            addBusinessObjectFormat({
                namespace : businessObjectDefinitionKey.namespace,
                businessObjectDefinitionName : businessObjectDefinitionKey.businessObjectDefinitionName,
                isError : true
            });
        }

        /**
         * Callback function for
         * BusinessObjectFormatService.getBusinessObjectFormat().
         * 
         * Calls
         * BusinessObjectDataAvailabilityService.getBusinessObjectDataAvailability()
         * if the format's partition key group matches the filter value. The
         * availability will return the storage name specified by the filter.
         */
        function onGetBusinessObjectFormatSuccess(response)
        {
            var businessObjectFormat = response.data;

            var filterPartitionKeyGroup = vm.filter.partitionKeyGroup;
            var partitionKeyGroup = businessObjectFormat.schema && businessObjectFormat.schema.partitionKeyGroup;

            /*
             * Filter matches if filter key group is not set, or if key group
             * equals filter value ignoring case.
             */
            if (!filterPartitionKeyGroup || herd.utils.equalsIgnoreCase(filterPartitionKeyGroup, partitionKeyGroup))
            {
                /*
                 * Add the format without error.
                 */
                addBusinessObjectFormat({
                    namespace : businessObjectFormat.namespace,
                    businessObjectDefinitionName : businessObjectFormat.businessObjectDefinitionName,
                    businessObjectFormatUsage : businessObjectFormat.businessObjectFormatUsage,
                    businessObjectFormatFileType : businessObjectFormat.businessObjectFormatFileType,
                    partitionKey : businessObjectFormat.partitionKey,
                    isError : false
                });

                /*
                 * Initiate request for availability.
                 */
                getBusinessObjectDataAvailability(businessObjectFormat);
            }
        }

        function onGetBusinessObjectFormatError(businessObjectFormatKey)
        {
            addBusinessObjectFormat({
                namespace : businessObjectFormatKey.namespace,
                businessObjectDefinitionName : businessObjectFormatKey.businessObjectDefinitionName,
                businessObjectFormatUsage : businessObjectFormatKey.businessObjectFormatUsage,
                businessObjectFormatFileType : businessObjectFormatKey.businessObjectFormatFileType,
                isError : true
            });
        }

        /**
         * Adds the format to the list of formats and broadcasts the
         * businessObjectFormatsChanged event.
         * 
         * @param {BusinessObjectFormat} businessObjectFormat
         */
        function addBusinessObjectFormat(businessObjectFormat)
        {
            vm.businessObjectFormats.push(businessObjectFormat);
            vm.$scope.$broadcast('businessObjectFormatsChanged');
        }

        /**
         * Gets the data availabilities for the current formats. Existing
         * availabilities will be removed.
         */
        function getBusinessObjectDataAvailabilities()
        {
            vm.businessObjectDataAvailabilities.splice(0);
            _(vm.businessObjectFormats).each(function(businessObjectFormat)
            {
                getBusinessObjectDataAvailability(businessObjectFormat);
            });
        }

        /**
         * Calls
         * BusinessObjectDataAvailabilityService.getBusinessObjectDataAvailability()
         * for the given format and current filter.
         * 
         * Appends the response availability to the list of availabilities.
         * 
         * @param {Object} businessObjectFormat
         * @param {String} businessObjectFormat.namespace
         * @param {String} businessObjectFormat.businessObjectDefinitionName
         * @param {String} businessObjectFormat.businessObjectFormatUsage
         * @param {String} businessObjectFormat.businessObjectFormatFileType
         * @param {String} businessObjectFormat.partitionKey
         */
        function getBusinessObjectDataAvailability(businessObjectFormat)
        {
            /*
             * Do not retrieve availability for formats with error.
             */
            if (!businessObjectFormat.isError)
            {
                /*
                 * Create the availability request based on the format and
                 * current filters.
                 */
                var getBusinessObjectDataAvailabilityRequest = {
                    namespace : businessObjectFormat.namespace,
                    businessObjectDefinitionName : businessObjectFormat.businessObjectDefinitionName,
                    businessObjectFormatUsage : businessObjectFormat.businessObjectFormatUsage,
                    businessObjectFormatFileType : businessObjectFormat.businessObjectFormatFileType,
                    businessObjectFormatVersion : businessObjectFormat.businessObjectFormatVersion,
                    partitionValueFilters : [ {
                        partitionKey : businessObjectFormat.partitionKey,
                        partitionValueRange : {
                            startPartitionValue : formatDate(vm.filter.startPartitionValue),
                            endPartitionValue : formatDate(vm.filter.endPartitionValue)
                        }
                    } ],
                    storageName : vm.filter.storageName
                };

                // Request availability
                vm.BusinessObjectDataAvailabilityService.getBusinessObjectDataAvailability(getBusinessObjectDataAvailabilityRequest).then(
                    onGetBusinessObjectDataAvailabilitySuccess, function()
                    {
                        onGetBusinessObjectDataAvailabilityError(getBusinessObjectDataAvailabilityRequest);
                    });
            }
        }

        /**
         * The callback for
         * BusinessObjectDataAvailabilityService.getBusinessObjectDataAvailability().
         * 
         * Adds the result of the availability to the list of availabilities.
         * 
         * Broadcasts businessObjectDataAvailabilitiesChanged
         * 
         * @param response
         */
        function onGetBusinessObjectDataAvailabilitySuccess(response)
        {
            var businessObjectDataAvailability = response.data;
            vm.businessObjectDataAvailabilities.push(businessObjectDataAvailability);
            vm.$scope.$broadcast('businessObjectDataAvailabilitiesChanged');
        }

        function onGetBusinessObjectDataAvailabilityError(getBusinessObjectDataAvailabilityRequest)
        {
            getBusinessObjectDataAvailabilityRequest.isError = true;
            vm.businessObjectDataAvailabilities.push(getBusinessObjectDataAvailabilityRequest);
            vm.$scope.$broadcast('businessObjectDataAvailabilitiesChanged');
        }
    }

})();