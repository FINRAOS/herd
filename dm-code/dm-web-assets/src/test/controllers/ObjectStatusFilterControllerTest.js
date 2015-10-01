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

    describe('ObjectStatusFilterController', function()
    {
        var $rootScope;
        var $controller;

        beforeEach(module('dm'));
        beforeEach(inject(function($injector)
        {
            $rootScope = $injector.get('$rootScope');
            $controller = $injector.get('$controller');
        }));

        describe('given filter is set', function()
        {
            var ObjectStatusFilterController;
            var $scope;

            beforeEach(function()
            {
                $scope = $rootScope.$new();
                ObjectStatusFilterController = $controller('ObjectStatusFilterController', {
                    $scope : $scope
                });

                ObjectStatusFilterController.filter = {};
            });

            describe('when filter.startPartitionValue is changed', function()
            {
                beforeEach(function()
                {
                    ObjectStatusFilterController.filter.startPartitionValue = 'test_startPartitionValue';
                    $scope.$digest();
                });

                it('sets partitionValueRange.startDate', function()
                {
                    expect(ObjectStatusFilterController.partitionValueRange.startDate).toBe('test_startPartitionValue');
                });
            });

            describe('when filter.endPartitionValue is changed', function()
            {
                beforeEach(function()
                {
                    ObjectStatusFilterController.filter.endPartitionValue = 'test_endPartitionValue';
                    $scope.$digest();
                });

                it('sets partitionValueRange.endDate', function()
                {
                    expect(ObjectStatusFilterController.partitionValueRange.endDate).toBe('test_endPartitionValue');
                });
            });
        });

        describe('given partitionValueRange is set, filter is defined', function()
        {
            var ObjectStatusFilterController;

            beforeEach(function()
            {
                var $scope = $rootScope.$new();
                ObjectStatusFilterController = $controller('ObjectStatusFilterController', {
                    $scope : $scope
                });

                ObjectStatusFilterController.filter = {};
                ObjectStatusFilterController.partitionValueRange = {
                    startDate : 'test_startDate',
                    endDate : 'test_endDate'
                };
            });

            describe('onPartitionValueRangeChanged()', function()
            {
                beforeEach(function()
                {
                    ObjectStatusFilterController.onPartitionValueRangeChanged();
                });

                it('sets filter.startPartitionValue', function()
                {
                    expect(ObjectStatusFilterController.filter.startPartitionValue).toBe('test_startDate');
                });

                it('sets filter.endPartitionValue', function()
                {
                    expect(ObjectStatusFilterController.filter.endPartitionValue).toBe('test_endDate');
                });
            });
        });
    });
})();