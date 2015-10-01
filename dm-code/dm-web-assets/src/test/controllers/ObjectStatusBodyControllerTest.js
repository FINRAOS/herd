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

    describe('ObjectStatusBodyController', function()
    {
        var $rootScope;
        var $controller;

        beforeEach(module('dm', function($provide)
        {
            $provide.constant('properties', {
                'opsStatus.status.image.available' : 'test_image'
            });
        }));

        beforeEach(inject(function($injector)
        {
            $rootScope = $injector.get('$rootScope');
            $controller = $injector.get('$controller');
        }));

        describe('initialize()', function()
        {
            var $scope;
            var ObjectStatusBodyController;

            beforeEach(function()
            {
                $scope = $rootScope.$new();
                ObjectStatusBodyController = $controller('ObjectStatusBodyController', {
                    $scope : $scope
                });
            });

            describe('given businessObjectFormats and partitionValues is set;'
                + ' expectedPartitionValues is set and expectedPartitionValuesChanged event was fired', function()
            {
                beforeEach(function()
                {
                    ObjectStatusBodyController.businessObjectFormats = [ {
                        namespace : 'test_namespace1',
                        businessObjectDefinitionName : 'test_businessObjectDefinitionName',
                        businessObjectFormatUsage : 'test_businessObjectFormatUsage',
                        businessObjectFormatFileType : 'test_businessObjectFormatFileType',
                        isError : false
                    }, {
                        namespace : 'test_namespace2',
                        businessObjectDefinitionName : 'test_businessObjectDefinitionName',
                        businessObjectFormatUsage : 'test_businessObjectFormatUsage',
                        businessObjectFormatFileType : 'test_businessObjectFormatFileType',
                        isError : true
                    } ];

                    ObjectStatusBodyController.partitionValues = [ 'a', 'b' ];

                    ObjectStatusBodyController.expectedPartitionValues = [ 'a' ];

                    $scope.$broadcast('expectedPartitionValuesChanged');
                });

                describe('when businessObjectFormatsChanged event is fired', function()
                {
                    beforeEach(function()
                    {
                        $scope.$broadcast('businessObjectFormatsChanged');
                    });

                    it('sets rows with statuses populated with correct isNotExpected flag', function()
                    {
                        expect(ObjectStatusBodyController.rows).toEqual([ {
                            'id' : 'test_namespace1:test_businessObjectDefinitionName:test_businessObjectFormatUsage:test_businessObjectFormatFileType',
                            'namespace' : 'test_namespace1',
                            'businessObjectDefinitionName' : 'test_businessObjectDefinitionName',
                            'statuses' : [ {
                                'id' : 'a',
                                'isNotExpected' : false
                            }, {
                                'id' : 'b',
                                'isNotExpected' : true
                            } ],
                            'isError' : false
                        }, {
                            'id' : 'test_namespace2:test_businessObjectDefinitionName:test_businessObjectFormatUsage:test_businessObjectFormatFileType',
                            'namespace' : 'test_namespace2',
                            'businessObjectDefinitionName' : 'test_businessObjectDefinitionName',
                            'statuses' : [ {
                                'id' : 'a',
                                'isNotExpected' : false
                            }, {
                                'id' : 'b',
                                'isNotExpected' : true
                            } ],
                            'isError' : true
                        } ]);
                    });
                });
            });

            describe('given businessObjectFormats, businessObjectDataAvailabilities and partitionValues is set; '
                + 'businessObjectFormatsChanged event is fired', function()
            {
                beforeEach(function()
                {
                    ObjectStatusBodyController.businessObjectFormats = [ {
                        namespace : 'test_namespace1',
                        businessObjectDefinitionName : 'test_businessObjectDefinitionName',
                        businessObjectFormatUsage : 'test_businessObjectFormatUsage',
                        businessObjectFormatFileType : 'test_businessObjectFormatFileType',
                        isError : false
                    }, {
                        namespace : 'test_namespace2',
                        businessObjectDefinitionName : 'test_businessObjectDefinitionName',
                        businessObjectFormatUsage : 'test_businessObjectFormatUsage',
                        businessObjectFormatFileType : 'test_businessObjectFormatFileType',
                        isError : false
                    } ];

                    ObjectStatusBodyController.businessObjectDataAvailabilities = [ {
                        namespace : 'test_namespace1',
                        businessObjectDefinitionName : 'test_businessObjectDefinitionName',
                        businessObjectFormatUsage : 'test_businessObjectFormatUsage',
                        businessObjectFormatFileType : 'test_businessObjectFormatFileType',
                        availableStatuses : [ {
                            partitionValue : 'a'
                        }, {
                            partitionValue : 'c'
                        } ],
                        isError : false
                    }, {
                        namespace : 'does not exist',
                        businessObjectDefinitionName : 'test_businessObjectDefinitionName',
                        businessObjectFormatUsage : 'test_businessObjectFormatUsage',
                        businessObjectFormatFileType : 'test_businessObjectFormatFileType',
                        availableStatuses : [ {
                            partitionValue : 'a'
                        }, {
                            partitionValue : 'c'
                        } ],
                        isError : false
                    }, {
                        namespace : 'test_namespace2',
                        businessObjectDefinitionName : 'test_businessObjectDefinitionName',
                        businessObjectFormatUsage : 'test_businessObjectFormatUsage',
                        businessObjectFormatFileType : 'test_businessObjectFormatFileType',
                        isError : true
                    } ];

                    ObjectStatusBodyController.partitionValues = [ 'a', 'b' ];

                    $scope.$broadcast('businessObjectFormatsChanged');
                });

                var expectedRows = [ {
                    'id' : 'test_namespace1:test_businessObjectDefinitionName:test_businessObjectFormatUsage:test_businessObjectFormatFileType',
                    'namespace' : 'test_namespace1',
                    'businessObjectDefinitionName' : 'test_businessObjectDefinitionName',
                    'statuses' : [ {
                        'id' : 'a',
                        'isNotExpected' : true,
                        'imageUrl' : 'test_image'
                    }, {
                        'id' : 'b',
                        'isNotExpected' : true,
                        'imageUrl' : null
                    } ],
                    'isError' : false
                }, {
                    'id' : 'test_namespace2:test_businessObjectDefinitionName:test_businessObjectFormatUsage:test_businessObjectFormatFileType',
                    'namespace' : 'test_namespace2',
                    'businessObjectDefinitionName' : 'test_businessObjectDefinitionName',
                    'statuses' : [ {
                        'id' : 'a',
                        'isNotExpected' : true
                    }, {
                        'id' : 'b',
                        'isNotExpected' : true
                    } ],
                    'isError' : true
                } ];

                describe('when partitionValuesChanged event is fired', function()
                {
                    beforeEach(function()
                    {
                        $scope.$broadcast('partitionValuesChanged');
                    });

                    it('populates rows with statuses', function()
                    {
                        expect(ObjectStatusBodyController.rows).toEqual(expectedRows);
                    });
                });

                describe('when businessObjectDataAvailabilitiesChanged event is fired', function()
                {
                    beforeEach(function()
                    {
                        $scope.$broadcast('businessObjectDataAvailabilitiesChanged');
                    });

                    it('populates rows with statuses', function()
                    {
                        expect(ObjectStatusBodyController.rows).toEqual(expectedRows);
                    });
                });
            });
        });
    });
})();