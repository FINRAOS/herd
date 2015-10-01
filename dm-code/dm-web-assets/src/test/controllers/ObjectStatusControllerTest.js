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

    describe('ObjectStatusController', function()
    {
        var MOCK_DATE = '1234-12-23';

        var MOCK_DATE_FORMAT = 'YYYY-MM-DD';

        var MOCK_PROPERTIES = {
            'opsStatus.partitionValue.date.format' : MOCK_DATE_FORMAT,
            'opsStatus.default.filter.date.range' : 3,
            'opsStatus.default.filter.businessObjectFormatFileType' : 'test_businessObjectFormatFileType',
            'opsStatus.default.filter.partitionKeyGroup' : 'test_partitionKeyGroup',
            'opsStatus.default.filter.storageName' : 'test_storageName',
            'opsStatus.default.calendar.navigation.days' : 1
        };

        var MOCK_RESPONSE_NAMESPACE_KEY = {
            namespaceCode : 'test_namespaceCode'
        };

        var MOCK_RESPONSE_DEFINITION_KEY = {
            namespace : 'test_namespace',
            businessObjectDefinitionName : 'test_businessObjectDefinitionName'
        };

        var MOCK_RESPONSE_FORMAT_KEY = {
            namespace : 'test_namespace',
            businessObjectDefinitionName : 'test_businessObjectDefinitionName',
            businessObjectFormatUsage : 'test_businessObjectFormatUsage',
            businessObjectFormatFileType : 'test_businessObjectFormatFileType'
        };

        var MOCK_RESPONSE_FORMAT = {
            namespace : 'test_namespace',
            businessObjectDefinitionName : 'test_businessObjectDefinitionName',
            businessObjectFormatUsage : 'test_businessObjectFormatUsage',
            businessObjectFormatFileType : 'test_businessObjectFormatFileType',
            businessObjectFormatVersion : 1234,
            partitionKey : 'test_partitionKey',
            schema : {
                partitionKeyGroup : 'test_partitionKeyGroup'
            }
        };

        var MOCK_RESPONSE_AVAILABILITY = 'test_availability';

        var MOCK_RESPONSE_EXPECTED_PARTITION_VALUES = {
            expectedPartitionValues : [ '1234-12-23', '1234-12-22', '1234-12-21' ]
        };

        var MOCK_RESPONSE_FILE_TYPES = {
            fileTypeKeys : [ {
                fileTypeCode : 'test_fileTypeCode1'
            }, {
                fileTypeCode : 'test_fileTypeCode2'
            } ]
        };

        var $controller;
        var $rootScope;
        var $location;
        var DateService;
        var NamespaceService;
        var BusinessObjectDefinitionService;
        var BusinessObjectFormatService;
        var BusinessObjectDataAvailabilityService;
        var ExpectedPartitionValuesService;
        var FileTypesService;

        beforeEach(module('dm'));

        /*
         * Inject mock properties constant
         */
        beforeEach(module(function($provide)
        {
            $provide.constant('properties', MOCK_PROPERTIES);
        }));

        /*
         * Create mocks for dependent services
         */
        beforeEach(inject(function($injector)
        {
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

            spyOn(DateService, 'now').and.returnValue(moment(MOCK_DATE));

            spyOn(NamespaceService, 'getNamespaceKeys').and.returnValue(successResponse({
                namespaceKeys : [ MOCK_RESPONSE_NAMESPACE_KEY ]
            }));

            spyOn(BusinessObjectDefinitionService, 'getBusinessObjectDefinitionKeys').and.returnValue(successResponse({
                businessObjectDefinitionKeys : [ MOCK_RESPONSE_DEFINITION_KEY ]
            }));

            spyOn(BusinessObjectFormatService, 'getBusinessObjectFormatKeys').and.returnValue(successResponse({
                businessObjectFormatKeys : [ MOCK_RESPONSE_FORMAT_KEY ]
            }));

            spyOn(BusinessObjectFormatService, 'getBusinessObjectFormat').and.returnValue(successResponse(MOCK_RESPONSE_FORMAT));

            spyOn(BusinessObjectDataAvailabilityService, 'getBusinessObjectDataAvailability').and.returnValue(successResponse(MOCK_RESPONSE_AVAILABILITY));

            spyOn(ExpectedPartitionValuesService, 'getExpectedPartitionValues').and.returnValue(successResponse(MOCK_RESPONSE_EXPECTED_PARTITION_VALUES));

            spyOn(FileTypesService, 'getFileTypes').and.returnValue(successResponse(MOCK_RESPONSE_FILE_TYPES));
        }));

        var defaultFilter = {
            businessObjectDefinitionName : null,
            startPartitionValue : moment(MOCK_DATE).subtract(MOCK_PROPERTIES['opsStatus.default.filter.date.range'] - 1, 'days').toDate(),
            endPartitionValue : moment(MOCK_DATE).toDate(),
            businessObjectFormatFileType : MOCK_PROPERTIES['opsStatus.default.filter.businessObjectFormatFileType'],
            storageName : MOCK_PROPERTIES['opsStatus.default.filter.storageName'],
            partitionKeyGroup : MOCK_PROPERTIES['opsStatus.default.filter.partitionKeyGroup'],
            fileTypes : [ 'test_fileTypeCode1', 'test_fileTypeCode2' ]
        };

        describe('initialize()', function()
        {
            describe('given all service responses match filters', function()
            {
                describe('when default filters are used', function()
                {
                    beforeEach(function()
                    {
                        var ctx = this;

                        var $scope = $rootScope.$new();

                        $scope.$on('partitionValuesChanged', function()
                        {
                            ctx.partitionValuesChanged = true;
                        });

                        $scope.$on('expectedPartitionValuesChanged', function()
                        {
                            ctx.expectedPartitionValuesChanged = true;
                        });

                        ctx.controller = $controller('ObjectStatusController', {
                            $scope : $scope
                        });
                    });

                    it('controller.filter is initialized correctly', function()
                    {
                        expect(this.controller.filter).toEqual(defaultFilter);
                    });

                    it('does not call NamespaceService.getNamespaceKeys()', function()
                    {
                        expect(NamespaceService.getNamespaceKeys).not.toHaveBeenCalled();
                    });
                });
            });

            describe('given url parameter specifies startPartitionValue but does not specify endPartitionValue', function()
            {
                beforeEach(function()
                {
                    $location.search('startPartitionValue', moment('2015-06-25', 'YYYY-MM-DD').format());
                    $location.search('endPartitionValue', null);
                });

                describe('when controller is initialized', function()
                {
                    var ObjectStatusController;

                    beforeEach(function()
                    {
                        ObjectStatusController = $controller('ObjectStatusController', {
                            $scope : $rootScope.$new()
                        });
                    });

                    it('sets endPartitionValue to startPartitionValue + range', function()
                    {
                        expect(ObjectStatusController.filter.endPartitionValue).toEqual(moment('2015-06-27', 'YYYY-MM-DD').toDate());
                    });
                });
            });

            describe('given url parameter specifies endPartitionValue', function()
            {
                beforeEach(function()
                {
                    $location.search('endPartitionValue', moment('2015-06-25', 'YYYY-MM-DD').format());
                });

                describe('when controller is initialized', function()
                {
                    var ObjectStatusController;

                    beforeEach(function()
                    {
                        ObjectStatusController = $controller('ObjectStatusController', {
                            $scope : $rootScope.$new()
                        });
                    });

                    it('sets endPartitionValue to be equal to url parameter', function()
                    {
                        expect(ObjectStatusController.filter.endPartitionValue).toEqual(moment('2015-06-25', 'YYYY-MM-DD').toDate());
                    });
                });
            });

            describe('given getFileTypes() responds with an error', function()
            {
                var ObjectStatusController;

                beforeEach(function()
                {
                    FileTypesService.getFileTypes.and.returnValue({
                        then : function(success, error)
                        {
                            error();
                        }
                    });

                    ObjectStatusController = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });
                });

                it('sets error message', function()
                {
                    expect(ObjectStatusController.errorMessage).toBe('Error retrieving file types');
                });
            });

            describe('given result file type does not match filter', function()
            {
                beforeEach(function()
                {
                    BusinessObjectFormatService.getBusinessObjectFormatKeys.and.returnValue(successResponse({
                        businessObjectFormatKeys : [ {
                            namespace : MOCK_RESPONSE_FORMAT_KEY.namespace,
                            businessObjectDefinitionName : MOCK_RESPONSE_FORMAT_KEY.businessObjectDefinitionName,
                            businessObjectFormatUsageCode : MOCK_RESPONSE_FORMAT_KEY.businessObjectFormatUsageCode,
                            businessObjectFormatFileType : 'DOES_NOT_MATCH',
                        } ]
                    }));

                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });
                });

                it('does not call BusinessObjectFormatService.getBusinessObjectFormat()', function()
                {
                    expect(BusinessObjectFormatService.getBusinessObjectFormat).not.toHaveBeenCalled();
                });

                it('does not call BusinessObjectDataAvailabilityService.getBusinessObjectDataAvailability()', function()
                {
                    expect(BusinessObjectDataAvailabilityService.getBusinessObjectDataAvailability).not.toHaveBeenCalled();
                });
            });

            describe('given result partition key group does not match filter', function()
            {
                beforeEach(function()
                {
                    BusinessObjectFormatService.getBusinessObjectFormat.and.returnValue(successResponse({
                        businessObjectFormatKeys : {
                            namespace : MOCK_RESPONSE_FORMAT.namespace,
                            businessObjectDefinitionName : MOCK_RESPONSE_FORMAT.businessObjectDefinitionName,
                            businessObjectFormatUsage : MOCK_RESPONSE_FORMAT.businessObjectFormatUsage,
                            businessObjectFormatFileType : MOCK_RESPONSE_FORMAT.businessObjectFormatFileType,
                            partitionKey : MOCK_RESPONSE_FORMAT.partitionKey,
                            schema : {
                                partitionKeyGroup : 'DOES_NOT_MATCH',
                            }
                        }
                    }));

                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });
                });

                it('does not call BusinessObjectDataAvailabilityService.getBusinessObjectDataAvailability()', function()
                {
                    expect(BusinessObjectDataAvailabilityService.getBusinessObjectDataAvailability).not.toHaveBeenCalled();
                });
            });
        });

        describe('doFilter()', function()
        {
            describe('when definition name filter matches', function()
            {
                beforeEach(function()
                {
                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });

                    this.controller.filter.businessObjectDefinitionName = MOCK_RESPONSE_DEFINITION_KEY.businessObjectDefinitionName;
                    BusinessObjectFormatService.getBusinessObjectFormatKeys.calls.reset();
                    this.controller.doFilter();
                });

                it('calls BusinessObjectFormatService.getBusinessObjectFormatKeys()', function()
                {
                    expect(BusinessObjectFormatService.getBusinessObjectFormatKeys).toHaveBeenCalled();
                });
            });

            describe('when definition name filter does not match', function()
            {
                beforeEach(function()
                {
                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });

                    this.controller.filter.businessObjectDefinitionName = 'DOES_NOT_MATCH';
                    NamespaceService.getNamespaceKeys.calls.reset();
                    BusinessObjectFormatService.getBusinessObjectFormatKeys.calls.reset();
                    this.controller.doFilter();
                });

                it('calls NamespaceService.getNamespaceKeys()', function()
                {
                    expect(NamespaceService.getNamespaceKeys).toHaveBeenCalled();
                });

                it('does not call BusinessObjectFormatService.getBusinessObjectFormatKeys()', function()
                {
                    expect(BusinessObjectFormatService.getBusinessObjectFormatKeys).not.toHaveBeenCalled();
                });
            });

            describe('when start partition value is empty and defintion name is "*"', function()
            {
                beforeEach(function()
                {
                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });

                    NamespaceService.getNamespaceKeys.calls.reset();

                    this.controller.filter.businessObjectDefinitionName = "*";
                    this.controller.filter.startPartitionValue = null;
                    this.controller.doFilter();
                });

                it('does not call NamespaceService.getNamespaceKeys', function()
                {
                    expect(NamespaceService.getNamespaceKeys).not.toHaveBeenCalled();
                });

                it('sets errorMessage with error message', function()
                {
                    expect(this.controller.errorMessage).toBe('Start partition value is required');
                });
            });

            describe('when endPartitionValue is empty and defintion name is "*"', function()
            {
                beforeEach(function()
                {
                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });

                    NamespaceService.getNamespaceKeys.calls.reset();

                    this.controller.filter.businessObjectDefinitionName = "*";
                    this.controller.filter.endPartitionValue = null;
                    this.controller.doFilter();
                });

                it('does not call NamespaceService.getNamespaceKeys', function()
                {
                    expect(NamespaceService.getNamespaceKeys).not.toHaveBeenCalled();
                });

                it('sets errorMessage with error message', function()
                {
                    expect(this.controller.errorMessage).toBe('End partition value is required');
                });
            });

            describe('when startPartitionValue greater than endPartitionValue and defintion name is "*"', function()
            {
                beforeEach(function()
                {
                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });

                    NamespaceService.getNamespaceKeys.calls.reset();

                    this.controller.filter.businessObjectDefinitionName = "*";
                    this.controller.filter.startPartitionValue = moment('1234-12-24');
                    this.controller.filter.endPartitionValue = moment('1234-12-23');
                    this.controller.doFilter();
                });

                it('does not call NamespaceService.getNamespaceKeys', function()
                {
                    expect(NamespaceService.getNamespaceKeys).not.toHaveBeenCalled();
                });

                it('sets errorMessage with error message', function()
                {
                    expect(this.controller.errorMessage).toBe('The start partition value must be less than the end partition value');
                });
            });

            describe('when startPartitionValue equals endPartitionValue and defintion name is "*"', function()
            {
                beforeEach(function()
                {
                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });

                    NamespaceService.getNamespaceKeys.calls.reset();

                    this.controller.filter.businessObjectDefinitionName = "*";
                    this.controller.filter.startPartitionValue = moment('1234-12-23');
                    this.controller.filter.endPartitionValue = moment('1234-12-23');
                    this.controller.doFilter();
                });

                it('does not call NamespaceService.getNamespaceKeys', function()
                {
                    expect(NamespaceService.getNamespaceKeys).not.toHaveBeenCalled();
                });

                it('sets errorMessage with error message', function()
                {
                    expect(this.controller.errorMessage).toBe('The start partition value must be less than the end partition value');
                });
            });

            describe('when businessObjectFormatFileType is specified and defintion name is "*"', function()
            {
                beforeEach(function()
                {
                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });

                    this.controller.filter.businessObjectDefinitionName = "*";
                    this.controller.filter.businessObjectFormatFileType = MOCK_RESPONSE_FORMAT_KEY.businessObjectFormatFileType;
                    this.controller.doFilter();
                });

                it('calls BusinessObjectFormatService.getBusinessObjectFormat', function()
                {
                    expect(BusinessObjectFormatService.getBusinessObjectFormat).toHaveBeenCalled();
                });
            });

            describe('when businessObjectFormatFileType does not match and defintion name is "*"', function()
            {
                beforeEach(function()
                {
                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });

                    this.controller.filter.businessObjectDefinitionName = "*";
                    this.controller.filter.businessObjectFormatFileType = 'does not match';
                    this.controller.doFilter();
                });

                it('calls BusinessObjectFormatService.getBusinessObjectFormat', function()
                {
                    expect(BusinessObjectFormatService.getBusinessObjectFormat).not.toHaveBeenCalled();
                });
            });

            describe('when partitionKeyGroup does not match and defintion name is "*"', function()
            {
                beforeEach(function()
                {
                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });

                    this.controller.filter.businessObjectDefinitionName = "*";
                    this.controller.filter.partitionKeyGroup = 'does not match';
                    this.controller.doFilter();
                });

                it('does not add formats', function()
                {
                    expect(this.controller.businessObjectFormats.length).toBe(0);
                });
            });

            describe('given GetExpectedPartitionValues responds with error', function()
            {
                beforeEach(function()
                {
                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });

                    ExpectedPartitionValuesService.getExpectedPartitionValues.and.returnValue(errorResponse());

                    this.controller.filter.businessObjectDefinitionName = "*";
                    this.controller.doFilter();
                });

                it('sets error message', function()
                {
                    expect(this.controller.errorMessage).toBe('Error retrieving expected partition values');
                });
            });

            describe('given GetNamespaceKeys responds with error', function()
            {
                beforeEach(function()
                {
                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });

                    NamespaceService.getNamespaceKeys.and.returnValue(errorResponse());

                    this.controller.filter.businessObjectDefinitionName = "*";
                    this.controller.doFilter();
                });

                it('sets error message', function()
                {
                    expect(this.controller.errorMessage).toBe('Error retrieving list of namespaces');
                });
            });

            describe('given GetBusinessObjectDefinitionKeys responds with error', function()
            {
                beforeEach(function()
                {
                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });

                    BusinessObjectDefinitionService.getBusinessObjectDefinitionKeys.and.returnValue(errorResponse());

                    this.controller.filter.businessObjectDefinitionName = "*";
                    this.controller.doFilter();
                });

                it('adds business object format in error state', function()
                {
                    expect(this.controller.businessObjectFormats.length).toBe(1);
                    expect(this.controller.businessObjectFormats[0]).toEqual({
                        namespace : MOCK_RESPONSE_NAMESPACE_KEY.namespaceCode,
                        isError : true
                    });
                });
            });

            describe('given GetBusinessObjectFormatKeys responds with error', function()
            {
                beforeEach(function()
                {
                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });

                    BusinessObjectFormatService.getBusinessObjectFormatKeys.and.returnValue(errorResponse());

                    this.controller.filter.businessObjectDefinitionName = "*";
                    this.controller.doFilter();
                });

                it('adds business object format in error state', function()
                {
                    expect(this.controller.businessObjectFormats.length).toBe(1);
                    expect(this.controller.businessObjectFormats[0]).toEqual({
                        namespace : MOCK_RESPONSE_DEFINITION_KEY.namespace,
                        businessObjectDefinitionName : MOCK_RESPONSE_DEFINITION_KEY.businessObjectDefinitionName,
                        isError : true
                    });
                });
            });

            describe('given GetBusinessObjectFormat responds with error', function()
            {
                beforeEach(function()
                {
                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });

                    BusinessObjectFormatService.getBusinessObjectFormat.and.returnValue(errorResponse());

                    this.controller.filter.businessObjectDefinitionName = "*";
                    this.controller.doFilter();
                });

                it('adds business object format in error state', function()
                {
                    expect(this.controller.businessObjectFormats.length).toBe(1);
                    expect(this.controller.businessObjectFormats[0]).toEqual({
                        namespace : MOCK_RESPONSE_FORMAT_KEY.namespace,
                        businessObjectDefinitionName : MOCK_RESPONSE_FORMAT_KEY.businessObjectDefinitionName,
                        businessObjectFormatUsage : MOCK_RESPONSE_FORMAT_KEY.businessObjectFormatUsage,
                        businessObjectFormatFileType : MOCK_RESPONSE_FORMAT_KEY.businessObjectFormatFileType,
                        isError : true
                    });
                });
            });

            describe('given GetBusinessObjectDataAvailability responds with error', function()
            {
                beforeEach(function()
                {
                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });

                    BusinessObjectDataAvailabilityService.getBusinessObjectDataAvailability.and.returnValue(errorResponse());

                    this.controller.filter.businessObjectDefinitionName = "*";
                    this.controller.doFilter();
                });

                it('adds businessObjectDataAvailability in error state', function()
                {
                    expect(this.controller.businessObjectDataAvailabilities.length).toBe(1);
                    expect(this.controller.businessObjectDataAvailabilities[0]).toEqual({
                        namespace : MOCK_RESPONSE_FORMAT.namespace,
                        businessObjectDefinitionName : MOCK_RESPONSE_FORMAT.businessObjectDefinitionName,
                        businessObjectFormatUsage : MOCK_RESPONSE_FORMAT.businessObjectFormatUsage,
                        businessObjectFormatFileType : MOCK_RESPONSE_FORMAT.businessObjectFormatFileType,
                        businessObjectFormatVersion : MOCK_RESPONSE_FORMAT.businessObjectFormatVersion,
                        partitionValueFilters : [ {
                            partitionKey : MOCK_RESPONSE_FORMAT.partitionKey,
                            partitionValueRange : {
                                startPartitionValue : '1234-12-21',
                                endPartitionValue : '1234-12-23'
                            }
                        } ],
                        storageName : this.controller.filter.storageName,
                        isError : true
                    });
                });
            });
        });

        describe('calendarBack()', function()
        {
            describe('given defaults and filtered at least once', function()
            {
                beforeEach(function()
                {
                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });

                    this.controller.filter.businessObjectDefinitionName = '*';
                    this.controller.doFilter();
                });

                describe('given no formats in error', function()
                {
                    beforeEach(function()
                    {
                        BusinessObjectDataAvailabilityService.getBusinessObjectDataAvailability.calls.reset();
                        this.controller.calendarBack();
                    });

                    it('decrements start and end partition values by configured number of days', function()
                    {
                        expect(this.controller.filter.startPartitionValue).toEqual(moment(defaultFilter.startPartitionValue).subtract(1, 'days').toDate());
                        expect(this.controller.filter.endPartitionValue).toEqual(moment(defaultFilter.endPartitionValue).subtract(1, 'days').toDate());
                    });

                    it('calls BusinessObjectDataAvailabilityService.getBusinessObjectDataAvailability', function()
                    {
                        expect(BusinessObjectDataAvailabilityService.getBusinessObjectDataAvailability).toHaveBeenCalled();
                    });
                });

                describe('given errors in formats', function()
                {
                    beforeEach(function()
                    {
                        this.controller.businessObjectFormats[0].isError = true;
                        BusinessObjectDataAvailabilityService.getBusinessObjectDataAvailability.calls.reset();
                        this.controller.calendarBack();
                    });

                    it('does not BusinessObjectDataAvailabilityService.getBusinessObjectDataAvailability', function()
                    {
                        expect(BusinessObjectDataAvailabilityService.getBusinessObjectDataAvailability).not.toHaveBeenCalled();
                    });
                });
            });
        });

        describe('calendarForward()', function()
        {
            describe('given defaults and filtered at least once', function()
            {
                beforeEach(function()
                {
                    this.controller = $controller('ObjectStatusController', {
                        $scope : $rootScope.$new()
                    });

                    this.controller.filter.businessObjectDefinitionName = '*';
                    this.controller.doFilter();
                    this.controller.calendarForward();
                });

                it('increments start and end partition values by configured number of days', function()
                {
                    expect(this.controller.filter.startPartitionValue).toEqual(moment(defaultFilter.startPartitionValue).add(1, 'days').toDate());
                    expect(this.controller.filter.endPartitionValue).toEqual(moment(defaultFilter.endPartitionValue).add(1, 'days').toDate());
                });
            });
        });
    });

    function successResponse(data)
    {
        return {
            then : function(success)
            {
                success({
                    data : data
                });
            }
        };
    }

    function errorResponse()
    {
        return {
            then : function(success, error)
            {
                error();
            }
        };
    }
})();