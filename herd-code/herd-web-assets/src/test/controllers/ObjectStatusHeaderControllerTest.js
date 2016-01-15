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

    describe('ObjectStatusHeaderController', function()
    {
        var $scope;
        var ObjectStatusHeaderController;

        beforeEach(module('herd'));
        beforeEach(inject(function($injector)
        {
            var $rootScope = $injector.get('$rootScope');
            var $controller = $injector.get('$controller');

            $scope = $rootScope.$new();
            ObjectStatusHeaderController = $controller('ObjectStatusHeaderController', {
                $scope : $scope
            });
        }));

        describe('initialize()', function()
        {
            describe('given partitionValues and expectedPartitionValues are populated', function()
            {
                beforeEach(function()
                {
                    ObjectStatusHeaderController.partitionValues = [ '2000-01-01', '2000-01-02', '2000-01-03', '2000-02-01' ];
                    ObjectStatusHeaderController.expectedPartitionValues = [ '2000-01-01', '2000-01-03' ];

                    $scope.$broadcast('expectedPartitionValuesChanged');
                    
                    this.expectedMonthHeaders = [ {
                        id : '2000-01',
                        content : 'Jan',
                        colspan : 3
                    }, {
                        id : '2000-02',
                        content : 'Feb',
                        colspan : 1
                    } ];
                    
                    this.expectedDateHeaders = [ {
                        id : '2000-01-01',
                        content : '1',
                        isNotExpected : false
                    }, {
                        id : '2000-01-02',
                        content : '2',
                        isNotExpected : true
                    }, {
                        id : '2000-01-03',
                        content : '3',
                        isNotExpected : false
                    }, {
                        id : '2000-02-01',
                        content : '1',
                        isNotExpected : true
                    } ];
                });

                it('updates headers on partitionValuesChanged event', function()
                {
                    $scope.$broadcast('partitionValuesChanged');

                    expect(ObjectStatusHeaderController.monthHeaders).toEqual(this.expectedMonthHeaders);
                    expect(ObjectStatusHeaderController.dateHeaders).toEqual(this.expectedDateHeaders);
                });

                it('updates headers on expectedPartitionValuesChanged event', function()
                {
                    $scope.$broadcast('expectedPartitionValuesChanged');

                    expect(ObjectStatusHeaderController.monthHeaders).toEqual(this.expectedMonthHeaders);
                    expect(ObjectStatusHeaderController.dateHeaders).toEqual(this.expectedDateHeaders);
                });
            });
        });
    });
})();