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

    describe('DateRangeDirective', function()
    {
        var $compile;
        var $rootScope;

        beforeEach(module('herd'));
        beforeEach(inject(function($injector)
        {
            $compile = $injector.get('$compile');
            $rootScope = $injector.get('$rootScope');
        }));

        describe('given some default parameters', function()
        {
            var $scope;
            var view;
            var vm = {};
            vm.testModel = null;
            vm.testOnChange = function()
            {

            };

            function getStartInput()
            {
                return view.find('.herd-date-range-start-input');
            }

            function getEndInput()
            {
                return view.find('.herd-date-range-end-input');
            }

            beforeEach(function()
            {
                $scope = $rootScope.$new();
                $scope.vm = vm;

                spyOn(vm, 'testOnChange');

                var template = '<herd-date-range model="vm.testModel" change="vm.testOnChange()"></herd-date-range>';
                view = $compile(template)($scope);
                $scope.$digest();
            });

            /*
             * This test was added in response to a bug in default angular
             * bootstrap datepicker where typing in an incomplete date which
             * starts with 0 would cause the model not to be updated.
             * 
             * A fix was implemented overriding the default parsing behavior of
             * the widget.
             */
            describe('when start input date is entered manually, but the date is incomplete', function()
            {
                beforeEach(function()
                {
                    getStartInput().val('2015-01-0').change();
                    getEndInput().val('2015-02-0').change();
                });

                it('updates the model', function()
                {
                    expect(vm.testModel.startDate).toEqual(moment('2014-12-31').toDate());
                    expect(vm.testModel.endDate).toEqual(moment('2015-01-31').toDate());
                });
            });

        });
    });
})();