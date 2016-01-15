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

    describe('ObjectStatusBodyDirective', function()
    {
        var $scope;
        var view;

        beforeEach(module('herd', function($controllerProvider)
        {
            $controllerProvider.register('ObjectStatusBodyController', function()
            {

            });
        }));

        beforeEach(inject(function($injector)
        {
            var $rootScope = $injector.get('$rootScope');
            var $compile = $injector.get('$compile');

            var element = angular.element('<tbody>');
            element.attr({
                'herd-object-status-body' : '',
                'partition-values' : 'partitionValues',
                'business-object-formats' : 'businessObjectFormats',
                'business-object-data-availabilities' : 'businessObjectDataAvailabilities',
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
            expect(view.isolateScope().vm.partitionValues).toEqual('test_partitionValues');
        });

        it('binds businessObjectFormats correctly', function()
        {
            $scope.businessObjectFormats = 'test_businessObjectFormats';
            $scope.$digest();
            expect(view.isolateScope().vm.businessObjectFormats).toEqual('test_businessObjectFormats');
        });

        it('binds businessObjectDataAvailabilities correctly', function()
        {
            $scope.businessObjectDataAvailabilities = 'test_businessObjectDataAvailabilities';
            $scope.$digest();
            expect(view.isolateScope().vm.businessObjectDataAvailabilities).toEqual('test_businessObjectDataAvailabilities');
        });

        it('binds expectedPartitionValues correctly', function()
        {
            $scope.expectedPartitionValues = 'test_expectedPartitionValues';
            $scope.$digest();
            expect(view.isolateScope().vm.expectedPartitionValues).toEqual('test_expectedPartitionValues');
        });
    });
})();