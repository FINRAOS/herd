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

    describe('ObjectStatusHeaderDirective', function()
    {
        var $scope;
        var view;

        beforeEach(module('herd', function($controllerProvider)
        {
            $controllerProvider.register('ObjectStatusHeaderController', function()
            {

            });
        }));

        beforeEach(inject(function($injector)
        {
            var $rootScope = $injector.get('$rootScope');
            var $compile = $injector.get('$compile');

            var element = angular.element('<thead>');
            element.attr({
                'herd-object-status-header' : '',
                'partition-values' : 'partitionValues',
                'calendar-back' : 'calendarBack()',
                'calendar-forward' : 'calendarForward()',
                'expected-partition-values' : 'expectedPartitionValues'
            });

            $scope = $rootScope.$new();
            view = $compile(element)($scope);
            $scope.$digest();
        }));

        it('binds partitionValues correctly', function()
        {
            $scope.partitionValues = 'test_partitionValues';
            $scope.$digest();
            expect(view.isolateScope().vm.partitionValues).toBe('test_partitionValues');
        });

        it('binds expectedPartitionValues correctly', function()
        {
            $scope.expectedPartitionValues = 'test_expectedPartitionValues';
            $scope.$digest();
            expect(view.isolateScope().vm.expectedPartitionValues).toBe('test_expectedPartitionValues');
        });

        it('binds calendarBack() correctly', function()
        {
            $scope.calendarBack = function()
            {
            };
            spyOn($scope, 'calendarBack');
            view.isolateScope().vm.calendarBack();
            expect($scope.calendarBack).toHaveBeenCalled();
        });

        it('binds calendarForward() correctly', function()
        {
            $scope.calendarForward = function()
            {
            };
            spyOn($scope, 'calendarForward');
            view.isolateScope().vm.calendarForward();
            expect($scope.calendarForward).toHaveBeenCalled();
        });
    });
})();