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
/*global $:false */
(function ()
{
    'use strict';

    describe('OpsStatusPageFunctionalTest', function ()
    {
        // Service variables
        var $controller;
        var $location;
        var DateService;
        var NamespaceService;
        var BusinessObjectDefinitionService;
        var BusinessObjectFormatService;
        var BusinessObjectDataAvailabilityService;
        var ExpectedPartitionValuesService;
        var FileTypesService;

        // Template variables
        var $rootScope;
        var $compile;
        var $templateCache;
        var $scope;

        var pageTemplate;

        var expectedPartitionValuesInRange = ['0000-00-00'];
        var defaultFilter;

        var MOCK_BLANK_RESPONSE = 'BLANK';

        beforeEach(module('dm'));

        /**
         * Create mocks for dependent services
         */
        beforeEach(inject(function ($injector)
        {
            // Stage service layer
            $controller = $injector.get('$controller');
            $rootScope = $injector.get('$rootScope');
            $location = $injector.get('$location');
            DateService = $injector.get('DateService');
            NamespaceService = $injector.get('NamespaceService');
            BusinessObjectDefinitionService = $injector.get('BusinessObjectDefinitionService');
            BusinessObjectFormatService = $injector.get('BusinessObjectFormatService');
            BusinessObjectDataAvailabilityService = $injector.get('BusinessObjectDataAvailabilityService');
            ExpectedPartitionValuesService = $injector.get('ExpectedPartitionValuesService');
            FileTypesService = $injector.get('FileTypesService');

            defaultFilter = {
                businessObjectDefinitionName: '*',
                startPartitionValue: expectedPartitionValuesInRange[0],
                endPartitionValue: expectedPartitionValuesInRange[expectedPartitionValuesInRange.length - 1],
                storageName: 'S3_MANAGED',
                partitionKeyGroup: 'TRADE_DT',
                businessObjectFormatFileType: 'testFileType'
            };

            setExpectedPartitionValues(['1234-12-23', '1234-12-24', '1234-12-25', '1234-12-26', '1234-12-27']);

            spyOn(ExpectedPartitionValuesService,
                    'getExpectedPartitionValues').and.returnValue(successResponse({expectedPartitionValues: expectedPartitionValuesInRange}));

            spyOn(FileTypesService, 'getFileTypes').and.returnValue(successResponse({
                fileTypeKeys: [
                    {fileTypeCode: 'testFileType'},
                    {fileTypeCode: 'someOtherFileType'},
                    {fileTypeCode: 'testFileType3'}
                ]
            }));

            // Stage template layer
            $compile = $injector.get('$compile');
            $templateCache = $injector.get('$templateCache');
            pageTemplate = $templateCache.get('main/templates/ObjectStatusTemplate.html');
        }));

        /**
         * Sets the expected partition values for the mock service and the default filter
         * @param values
         */
        function setExpectedPartitionValues(values)
        {
            values.sort();
            expectedPartitionValuesInRange = values;
            defaultFilter.startPartitionValue = values[0];
            defaultFilter.endPartitionValue = expectedPartitionValuesInRange[values.length - 1];
        }

        /**
         * Gets the expected Availability Statuses object based on the staged data
         * @param namespaceKeyList
         * @param businessObjectDefinitionKeyList Expects that the BDEF ends in -[0..31], for availability
         * @param usageList If undefined, one usage of type 'PRC' is assumed
         * @returns {Array}
         */
        function getExpectedStatuses(namespaceKeyList, businessObjectDefinitionKeyList, usageList)
        {
            var expected = [];
            if (usageList === undefined)
            {
                usageList = ['PRC'];
            }

            for (var nsIndex = 0; nsIndex < namespaceKeyList.length; nsIndex++)
            {
                for (var bdefIndex = 0; bdefIndex < businessObjectDefinitionKeyList.length; bdefIndex++)
                {
                    for (var usageCount = 0; usageCount < usageList.length; usageCount++)
                    {
                        var status = {
                            namespace: namespaceKeyList[nsIndex],
                            objectName: businessObjectDefinitionKeyList[bdefIndex],
                            statuses: intToBinArray(businessObjectDefinitionKeyList[bdefIndex].split('-')[1], 5),
                            isError: false
                        };

                        expected.push(status);
                    }
                }
            }
            return expected;
        }

        function mockNamespaceService(namespaceKeyList)
        {
            spyOn(NamespaceService, 'getNamespaceKeys').and.returnValue(successResponse({
                namespaceKeys: namespaceKeyList.map(function (namespaceKey)
                {
                    return {namespaceCode: namespaceKey};
                })
            }));
        }

        function mockBdefKeys(businessObjectDefinitionKeyList)
        {
            spyOn(BusinessObjectDefinitionService, 'getBusinessObjectDefinitionKeys').and.callFake(function (businessObjectDefinitionRequest)
            {
                var keys = [];
                if (businessObjectDefinitionRequest.namespaceCode !== MOCK_BLANK_RESPONSE)
                {
                    keys = businessObjectDefinitionKeyList.map(function (bdefKey)
                    {
                        return {
                            namespace: businessObjectDefinitionRequest.namespaceCode,
                            businessObjectDefinitionName: bdefKey
                        };
                    });
                }

                return successResponse({
                    businessObjectDefinitionKeys: keys
                });
            });
        }

        function mockFormatKeys(fileTypeList, usageList)
        {
            usageList = usageList || ['PRC'];
            spyOn(BusinessObjectFormatService, 'getBusinessObjectFormatKeys').and.callFake(function (getBusinessObjectFormatKeysRequest)
            {
                var keyList = [];
                if (getBusinessObjectFormatKeysRequest.businessObjectDefinitionName !== MOCK_BLANK_RESPONSE)
                {
                    usageList.forEach(function (usage)
                    {
                        keyList = keyList.concat(fileTypeList.map(function (formatKey)
                        {
                            return {
                                namespace: getBusinessObjectFormatKeysRequest.namespace,
                                businessObjectDefinitionName: getBusinessObjectFormatKeysRequest.businessObjectDefinitionName,
                                businessObjectFormatUsage: usage,
                                businessObjectFormatFileType: formatKey,
                                businessObjectFormatVersion: 0
                            };
                        }));

                    });
                }

                return successResponse({
                    businessObjectFormatKeys: keyList
                });
            });
        }

        function mockFormat()
        {
            spyOn(BusinessObjectFormatService, 'getBusinessObjectFormat').and.callFake(function (getBusinessObjectFormatRequest)
            {
                return successResponse({
                    namespace: getBusinessObjectFormatRequest.namespace,
                    businessObjectDefinitionName: getBusinessObjectFormatRequest.businessObjectDefinitionName,
                    businessObjectFormatUsage: getBusinessObjectFormatRequest.businessObjectFormatUsage,
                    businessObjectFormatFileType: getBusinessObjectFormatRequest.businessObjectFormatFileType,
                    businessObjectFormatVersion: getBusinessObjectFormatRequest.businessObjectFormatVersion,
                    partitionKey: 'testPartitionKey',
                    schema: {partitionKeyGroup: 'TRADE_DT'}
                });
            });
        }

        function mockAvailability()
        {
            spyOn(BusinessObjectDataAvailabilityService, 'getBusinessObjectDataAvailability').and.callFake(function (getBusinessObjectDataAvailabilityRequest)
            {
                // Assume that bdefs are something-0, something-1, something-2, etc
                // We will stage availability to be the binary representation of that number
                var bdefNumberString = getBusinessObjectDataAvailabilityRequest.businessObjectDefinitionName.split('-')[1];

                var binaryRepresentation = intToBinArray(bdefNumberString, 5);

                // Build available and not available statuses using the binary representation
                var availableStatusesArray = [];
                var notAvailableStatusesArray = [];
                for (var i = 0; i < expectedPartitionValuesInRange.length; i++)
                {
                    if (binaryRepresentation[i])
                    {
                        // It's available
                        availableStatusesArray.push({
                            businessObjectFormatVersion: getBusinessObjectDataAvailabilityRequest.businessObjectFormatVersion,
                            partitionValue: expectedPartitionValuesInRange[i],
                            businessObjectDataVersion: 0
                        });
                    }
                    else
                    {
                        // It's not available
                        notAvailableStatusesArray.push({
                            businessObjectFormatVersion: getBusinessObjectDataAvailabilityRequest.businessObjectFormatVersion,
                            partitionValue: expectedPartitionValuesInRange[i],
                            businessObjectDataVersion: 0,
                            reason: 'NOT REGISTERED'
                        });
                    }
                }

                // return the object
                return successResponse({
                    namespace: getBusinessObjectDataAvailabilityRequest.namespace,
                    businessObjectDefinitionName: getBusinessObjectDataAvailabilityRequest.businessObjectDefinitionName,
                    businessObjectFormatUsage: getBusinessObjectDataAvailabilityRequest.businessObjectFormatUsage,
                    businessObjectFormatFileType: getBusinessObjectDataAvailabilityRequest.businessObjectFormatFileType,
                    businessObjectFormatVersion: getBusinessObjectDataAvailabilityRequest.businessObjectFormatVersion,
                    partitionValueFilters: getBusinessObjectDataAvailabilityRequest.partitionValueFilters,
                    businessObjectDataVersion: 0,
                    storageName: getBusinessObjectDataAvailabilityRequest.storageName,
                    availableStatuses: availableStatusesArray,
                    notAvailableStatuses: notAvailableStatusesArray
                });
            });
        }

        /**
         * Mocks GET Namespaces, GET Bdefs, GET Formats, GET Format, and POST Availability
         * @param namespaceKeyList
         * @param businessObjectDefinitionKeyList Expected to end in -[0..31] for availability binary mapping
         * @param fileTypeList
         * @param usageList
         */
        function mockAllDataServices(namespaceKeyList, businessObjectDefinitionKeyList, fileTypeList, usageList)
        {
            mockNamespaceService(namespaceKeyList);
            mockBdefKeys(businessObjectDefinitionKeyList);
            mockFormatKeys(fileTypeList, usageList);
            mockFormat();
            mockAvailability();
        }

        /**
         * This method will convert an int-parseable string into an array which is a binary representation of that integer.
         * For instance, 4 will become [false, false, true] and 6 will become [false, true, true].
         * @param integer the number to convert
         * @param numPositions how many positions to pad. If this is less than the length of the normal output, it is ignored.
         * @returns {Array}
         */
        function intToBinArray(integer, numPositions)
        {
            var intAsBinary = parseInt(integer).toString(2);
            var response = [];
            for (var i = intAsBinary.length - 1; i >= 0; i--)
            {
                response.push(intAsBinary[i] === '1');
            }

            while (response.length < numPositions)
            {
                response.push(false);
            }
            return response;
        }

        /**
         * Parses the statuses in the view into an object
         * @param view
         * @returns {Array} of Objects {namespace, objectName, isError(boolean), statuses(boolean array)}
         */
        function getStatusesFromView(view)
        {
            var result = [];
            view.find('.object-status-body-row').each(function (rowIndex, row)
            {
                var statusRow = {};
                statusRow.namespace = $(row).find('td.object-namespace').text();
                statusRow.objectName = $(row).find('td.object-name').text();
                statusRow.isError = $(row).hasClass('is-error');
                statusRow.statuses = [];

                // If there's an image, the data is available; push it as true
                // Using push() works because .each() operates sequentially
                $(row).find('td.object-status').each(function (index, status)
                {
                    if ($(status).find('img').length)
                    {
                        statusRow.statuses.push(true);
                    }
                    else
                    {
                        statusRow.statuses.push(false);
                    }
                });
                result.push(statusRow);
            });

            return result;
        }

        /**
         * Parses the header from the view into an object
         * @param view
         * @returns {{topHeader: Array of {header, colspan}, bottomHeader: Array of {value, [opt]isNotExpected=true}}
         */
        function getHeaderFromView(view)
        {
            var returnObject = {
                topHeader: [],
                bottomHeader: []
            };

            view.find('table.status-table > thead > tr.table-header-level-1 > th').each(function (index, element)
            {
                returnObject.topHeader.push({
                    header: element.innerText,
                    colspan: element.colSpan
                });
            });
            view.find('table.status-table > thead > tr.table-header-level-2 > th').each(function (index, element)
            {
                var th = {value: element.innerText};

                // So we don't have to put isNotExpected=false in a bunch of tests, only set this if it's true
                if ($(element).hasClass('not-expected'))
                {
                    th.isNotExpected = true;
                }
                returnObject.bottomHeader.push(th);
            });

            return returnObject;
        }

        /**
         * Gets the expected header given a list of months and days
         *
         * @param months
         * @param days
         * @param notExpected if true, all days are not expected
         * @returns {{topHeader: Array of {header, colspan}, bottomHeader: Array of {value, (opt)isNotExpected=true}}
         */
        function getExpectedHeaders(months, days, notExpected)
        {
            var returnObject = {
                topHeader: [],
                bottomHeader: []
            };

            // Populate the top header
            returnObject.topHeader = [
                {
                    header: 'Namespace',
                    colspan: 1
                },
                {
                    header: 'Object',
                    colspan: 1
                },
                {
                    header: '<',
                    colspan: 1
                }
            ];
            months.forEach(function (element)
            {
                returnObject.topHeader.push({
                    header: element.monthName,
                    colspan: element.numDays
                });
            });
            returnObject.topHeader.push({
                header: '>',
                colspan: 1
            });

            // Populate the bottom header
            returnObject.bottomHeader = [
                {value: ''},
                {value: ''},
                {value: ''}
            ];
            days.forEach(function (element)
            {
                var day = {value: element};

                // Only add this if it's true, since default state is false and we don't want to populate the default value in our tests
                if (notExpected)
                {
                    day.isNotExpected = true;
                }
                returnObject.bottomHeader.push(day);
            });
            returnObject.bottomHeader.push({value: ''});

            return returnObject;
        }

        /**
         * Sets the filter in the UI
         * @param view
         * @param filter
         */
        function setFilter(view, filter)
        {
            view.find('.filter-name-input').val(filter.businessObjectDefinitionName).change();
            view.find('.dm-date-range-start-input').val(moment(filter.startPartitionValue).format('YYYY-MM-DD')).change();
            view.find('.dm-date-range-end-input').val(moment(filter.endPartitionValue).format('YYYY-MM-DD')).change();
            view.find('.filter-fileType-input').val('string:' + filter.businessObjectFormatFileType).change();
            view.find('.filter-submit-button').click();

            $scope.$digest();
        }

        /**
         * Builds a promise which calls the success callback
         */
        function successResponse(data)
        {
            return {
                then: function (success)
                {
                    success({
                        data: data
                    });
                }
            };
        }

        /**
         * Builds a promise which calls the error callback
         */
        function errorResponse(statusCode, statusDescription, message, messageDetails)
        {
            return {
                then: function (success, error)
                {
                    error({
                        statusCode: statusCode,
                        statusDescription: statusDescription,
                        message: message,
                        messageDetails: messageDetails
                    });
                }
            };
        }

        /**
         * Helper method to validate that an error message appeared on the provided view
         * @param view
         * @param errorMessage
         */
        function expectErrorMessage(view, errorMessage)
        {
            expect(view.find('p.error span').text()).toBe(errorMessage);
        }

        /**
         * Creates a new controller and map it to the view, then return the view
         */
        function stageView()
        {
            $scope = $rootScope.$new();

            $scope.vm = $controller('ObjectStatusController', {
                $scope: $scope
            });
            var view = $compile(pageTemplate)($scope);

            $scope.$digest();

            return view;
        }

        it('Happy path functional', function ()
        {
            // Stage the data
            var namespaceList = ['testNameSpace'];
            var bdefList = [];
            for (var i = 0; i < 32; i++)
            {
                bdefList.push('testBDEF-' + i);
            }

            var formatList = ['testFileType'];

            mockAllDataServices(namespaceList, bdefList, formatList);
            var expectedStatuses = getExpectedStatuses(namespaceList, bdefList);

            // Set up DOM
            var view = stageView();

            // Stage the filter
            setFilter(view, defaultFilter);

            var headers = getHeaderFromView(view);
            var expectedHeaders = getExpectedHeaders([
                {
                    monthName: 'Dec',
                    numDays: 5
                }
            ], ['23', '24', '25', '26', '27']);
            expect(headers).toEqual(expectedHeaders);

            // Let's see what's in the view
            var statuses = getStatusesFromView(view);
            expect(statuses).toEqual(expectedStatuses);
        });

        describe('given filter.businessObjectDefinitionName is not specified', function ()
        {
            beforeEach(function ()
            {
                // Stage all the data
                mockAllDataServices([ 'namespace' ], [ 'bdef-1' ], [ 'filetype' ]);
                defaultFilter.businessObjectDefinitionName = null;
            });

            it('does not execute filter, therefore the statuses are empty', function ()
            {
                var view = stageView();
                var statuses = getStatusesFromView(view);
                expect(statuses).toEqual([]);
            });
        });

        describe('Errors in various services - ', function ()
        {
            beforeEach(function ()
            {
                // Stage all the data
                mockAllDataServices(['namespace'], ['bdef-1'], ['filetype']);
                defaultFilter.businessObjectFormatFileType = 'filetype';
            });

            it('ExpectedPartitionValuesService', function ()
            {
                ExpectedPartitionValuesService.getExpectedPartitionValues.and.returnValue(errorResponse(400, 'Bad Request'));
                var view = stageView();
                setFilter(view, defaultFilter);
                expectErrorMessage(view, 'Error retrieving expected partition values');

            });

            it('FileTypesService', function ()
            {
                FileTypesService.getFileTypes.and.returnValue(errorResponse(400, 'Bad Request'));
                var view = stageView();
                expectErrorMessage(view, 'Error retrieving file types');
            });

            it('NamespaceService', function ()
            {
                NamespaceService.getNamespaceKeys.and.returnValue(errorResponse(400, 'Bad Request'));
                var view = stageView();
                setFilter(view, defaultFilter);
                expectErrorMessage(view, 'Error retrieving list of namespaces');
            });

            it('BusinessObjectDefinitionService', function ()
            {
                BusinessObjectDefinitionService.getBusinessObjectDefinitionKeys.and.returnValue(errorResponse(400, 'Bad Request'));
                var view = stageView();
                setFilter(view, defaultFilter);
                var statuses = getStatusesFromView(view);
                expect(statuses).toEqual([
                    {
                        namespace: 'namespace',
                        objectName: '',
                        isError: true,
                        statuses: [false, false, false, false, false]
                    }
                ]);
            });

            it('BusinessObjectFormatService getBusinessObjectFormatKeys', function ()
            {
                BusinessObjectFormatService.getBusinessObjectFormatKeys.and.returnValue(errorResponse(400, 'Bad Request'));
                var view = stageView();
                setFilter(view, defaultFilter);
                var statuses = getStatusesFromView(view);
                expect(statuses).toEqual([
                    {
                        namespace: 'namespace',
                        objectName: 'bdef-1',
                        isError: true,
                        statuses: [false, false, false, false, false]
                    }
                ]);
            });

            it('BusinessObjectFormatService getBusinessObjectFormat', function ()
            {
                BusinessObjectFormatService.getBusinessObjectFormat.and.returnValue(errorResponse(400, 'Bad Request'));
                var view = stageView();
                setFilter(view, defaultFilter);
                var statuses = getStatusesFromView(view);
                expect(statuses).toEqual([
                    {
                        namespace: 'namespace',
                        objectName: 'bdef-1',
                        isError: true,
                        statuses: [false, false, false, false, false]
                    }
                ]);
            });

            it('BusinessObjectDataAvailabilityService', function ()
            {
                BusinessObjectDataAvailabilityService.getBusinessObjectDataAvailability.and.returnValue(errorResponse(400, 'Bad Request'));
                var view = stageView();
                setFilter(view, defaultFilter);
                var statuses = getStatusesFromView(view);
                expect(statuses).toEqual([
                    {
                        namespace: 'namespace',
                        objectName: 'bdef-1',
                        isError: true,
                        statuses: [false, false, false, false, false]
                    }
                ]);
            });
        });

        it('REST iteration tests', function ()
        {
            var namespaces = ['ns1', 'ns2'];
            var bdefs = ['BDEF1-31', 'BDEF2-31']; // -31 for "all available"
            var formatList = ['testFileType'];
            var usageList = ['USG1', 'USG2'];
            mockAllDataServices(namespaces, bdefs, formatList, usageList);
            var expectedStatuses = getExpectedStatuses(namespaces, bdefs, usageList);

            // Set up DOM
            var view = stageView();

            // Stage the filter
            setFilter(view, defaultFilter);

            // Let's see what's in the view
            var statuses = getStatusesFromView(view);
            expect(statuses).toEqual(expectedStatuses);
        });

        describe('Empty REST responses - ', function ()
        {
            it('Namespace', function ()
            {
                mockAllDataServices([], ['bdef-1'], ['testFileType']);
                var view = stageView();

                defaultFilter.businessObjectFormatFileType = 'testFileType';
                setFilter(view, defaultFilter);

                var statuses = getStatusesFromView(view);
                expect(statuses.length).toBe(0);
            });

            it('BDEF for one NS', function ()
            {
                mockAllDataServices(['NS1', MOCK_BLANK_RESPONSE], ['BDEF1-0', 'BDEF2-0'], ['testFileType']);

                var view = stageView();

                defaultFilter.businessObjectFormatFileType = 'testFileType';
                setFilter(view, defaultFilter);

                var statuses = getStatusesFromView(view);
                expect(statuses).toEqual(getExpectedStatuses(['NS1'], ['BDEF1-0', 'BDEF2-0']));
            });

            it('BDEF for any NS', function ()
            {
                mockAllDataServices(['NS1', 'NS2'], [], ['testFileType']);
                var view = stageView();

                defaultFilter.businessObjectFormatFileType = 'testFileType';
                setFilter(view, defaultFilter);

                var statuses = getStatusesFromView(view);
                expect(statuses.length).toBe(0);
            });

            it('Format for one BDEF', function ()
            {
                mockAllDataServices(['NS1'], ['BDEF1-0', MOCK_BLANK_RESPONSE], ['testFileType']);

                var view = stageView();

                defaultFilter.businessObjectFormatFileType = 'testFileType';
                setFilter(view, defaultFilter);

                var statuses = getStatusesFromView(view);
                expect(statuses).toEqual(getExpectedStatuses(['NS1'], ['BDEF1-0']));
            });

            it('Formats for any BDEF', function ()
            {
                mockAllDataServices(['NS1', 'NS2'], ['BDEF1-0', 'BDEF2-0'], []);
                var view = stageView();

                defaultFilter.businessObjectFormatFileType = 'testFileType';
                setFilter(view, defaultFilter);

                var statuses = getStatusesFromView(view);
                expect(statuses.length).toBe(0);
            });
        });

        it('Error when start date greater than end date', function ()
        {
            mockAllDataServices([], [], []);
            var view = stageView();
            setFilter(view, {
                businessObjectDefinitionName: defaultFilter.businessObjectDefinitionName,
                startPartitionValue: defaultFilter.endPartitionValue,
                endPartitionValue: defaultFilter.startPartitionValue,
                storageName: defaultFilter.storageName,
                partitionKeyGroup: defaultFilter.partitionKeyGroup,
                businessObjectFormatFileType: defaultFilter.businessObjectFormatFileType
            });

            expectErrorMessage(view, 'The start partition value must be less than the end partition value');
        });

        describe('Filter tests - ', function ()
        {
            var view;
            beforeEach(function ()
            {
                mockAllDataServices(['NS1'], ['ABCDE-0', 'BCDEF-0', 'CDEFG-0'], ['testFileType']);
                view = stageView();
            });

            it('prefix', function ()
            {
                defaultFilter.businessObjectDefinitionName = 'ABC';
                setFilter(view, defaultFilter);
                var statuses = getStatusesFromView(view);
                expect(statuses.length).toBe(1);
            });

            it('midfix', function ()
            {
                defaultFilter.businessObjectDefinitionName = 'DEF';
                setFilter(view, defaultFilter);
                var statuses = getStatusesFromView(view);
                expect(statuses.length).toBe(2);
            });

            it('postfix', function ()
            {
                defaultFilter.businessObjectDefinitionName = '-0';
                setFilter(view, defaultFilter);
                var statuses = getStatusesFromView(view);
                expect(statuses.length).toBe(3);
            });

            it('case-insensitive', function ()
            {
                defaultFilter.businessObjectDefinitionName = 'cDe';
                setFilter(view, defaultFilter);
                var statuses = getStatusesFromView(view);
                expect(statuses.length).toBe(3);
            });
        });

        it('Non-partition keys have different styling', function ()
        {
            mockAllDataServices(['NS1'], ['BDEF-0'], ['testFileType']);
            var view = stageView();
            setFilter(view, {
                businessObjectDefinitionName: defaultFilter.businessObjectDefinitionName,
                startPartitionValue: defaultFilter.endPartitionValue,
                endPartitionValue: moment(defaultFilter.endPartitionValue).add(1, 'days').format('YYYY-MM-DD'),
                storageName: defaultFilter.storageName,
                partitionKeyGroup: defaultFilter.partitionKeyGroup,
                businessObjectFormatFileType: 'testFileType'
            });

            var headers = getHeaderFromView(view);
            expect(headers.bottomHeader[3].isNotExpected).toBeUndefined();
            expect(headers.bottomHeader[4].isNotExpected).toBeTruthy();
        });

        it('Scrolling calendar left/right updates headers by 7 days', function ()
        {
            mockAllDataServices(['NS1'], ['BDEF-31'], ['testFileType']);
            var view = stageView();
            setFilter(view, defaultFilter);
            // get a baseline to ensure the days are what we expect
            var headers = getHeaderFromView(view);
            expect(headers).toEqual(getExpectedHeaders([
                {
                    monthName: 'Dec',
                    numDays: 5
                }
            ], ['23', '24', '25', '26', '27']));

            // scroll right
            view.find('.calendar-forward-button').click();
            $scope.$digest();
            headers = getHeaderFromView(view);
            expect(headers).toEqual(getExpectedHeaders([
                {
                    monthName: 'Dec',
                    numDays: 2
                },
                {
                    monthName: 'Jan',
                    numDays: 3
                }
            ], ['30', '31', '1', '2', '3'], true));

            // scroll left twice
            view.find('.calendar-back-button').click();
            $scope.$digest();
            view.find('.calendar-back-button').click();
            $scope.$digest();
            headers = getHeaderFromView(view);
            expect(headers).toEqual(getExpectedHeaders([
                {
                    monthName: 'Dec',
                    numDays: 5
                }
            ], ['16', '17', '18', '19', '20'], true));
        });

        it('availability over a leap day', function ()
        {
            mockAllDataServices(['NS1'], ['BDEF-0'], ['testFileType']);
            var view = stageView();
            setFilter(view, {
                businessObjectDefinitionName: defaultFilter.businessObjectDefinitionName,
                startPartitionValue: '2012-02-28',
                endPartitionValue: '2012-03-01',
                storageName: defaultFilter.storageName,
                partitionKeyGroup: defaultFilter.partitionKeyGroup,
                businessObjectFormatFileType: 'testFileType'
            });

            var headers = getHeaderFromView(view);
            expect(headers).toEqual(getExpectedHeaders([
                {
                    monthName: 'Feb',
                    numDays: 2
                },
                {
                    monthName: 'Mar',
                    numDays: 1
                }
            ], ['28', '29', '1'], true));
        });

        describe('DatePicker tests', function ()
        {
            var view;
            beforeEach(function ()
            {
                mockAllDataServices([], [], []);
                view = stageView();
            });

            function clickStartRange()
            {
                view.find('.dm-date-range-start-input').click();
                $scope.$digest();
            }

            function clickEndRange()
            {
                view.find('.dm-date-range-end-input').click();
                $scope.$digest();
            }

            function isStartRangePickerOpen()
            {
                return view.find('.dm-date-range-start ul.dropdown-menu').css('display') == 'block';
            }

            function isEndRangePickerOpen()
            {
                return view.find('.dm-date-range-end ul.dropdown-menu').css('display') == 'block';
            }

            it('startDateRange opens date picker widget', function ()
            {
                expect(isStartRangePickerOpen()).toBeFalsy();
                clickStartRange();
                expect(isStartRangePickerOpen()).toBeTruthy();
            });

            it('endDateRange opens date picker widget', function ()
            {
                expect(isEndRangePickerOpen()).toBeFalsy();
                clickEndRange();
                expect(isEndRangePickerOpen()).toBeTruthy();
            });

            it('Opening one date picker closes the other', function ()
            {
                clickStartRange();
                expect(isStartRangePickerOpen()).toBeTruthy();
                expect(isEndRangePickerOpen()).toBeFalsy();
                clickEndRange();
                expect(isStartRangePickerOpen()).toBeFalsy();
                expect(isEndRangePickerOpen()).toBeTruthy();
            });
        });

        describe('file type dropdown tests', function ()
        {
            var fileTypesList;
            beforeEach(function ()
            {
                fileTypesList = ['FT1', 'FT2', 'FT3', 'REALLY REALLY REEEEEEEEEAAAAALLLY LONG FILE TYPE'];
            });

            function stageFileTypeResponse(fileTypes)
            {
                FileTypesService.getFileTypes.and.returnValue(successResponse({
                    fileTypeKeys: fileTypes.map(function (item)
                    {
                        return {fileTypeCode: item};
                    })
                }));
            }

            it('default selected value is BZ', function ()
            {
                fileTypesList.push('BZ');
                stageFileTypeResponse(fileTypesList);

                mockAllDataServices([], [], []);
                var view = stageView();

                expect(view.find('.filter-fileType option[selected]').text()).toBe('BZ');
            });

            it('if BZ not present, nothing selected', function ()
            {
                stageFileTypeResponse(fileTypesList);

                mockAllDataServices([], [], []);
                var view = stageView();

                expect(view.find('.filter-fileType option[selected]').text()).toBe('');
            });

            it('Contents of dropdown appear when clicked', function ()
            {
                fileTypesList.push('BZ');
                stageFileTypeResponse(fileTypesList);
                mockAllDataServices([], [], []);
                var view = stageView();

                var actualFileTypes = [];
                view.find('.filter-fileType-input option').each(function (index, option)
                {
                    actualFileTypes.push(option.innerText);
                });
                expect(actualFileTypes).toEqual(fileTypesList);
            });
        });

        it('only look at latest format', function ()
        {
            mockAllDataServices(['NS'], ['BDEF-1'], ['fileType'], ['PRC'], [0, 1]);
        });
    });
})();