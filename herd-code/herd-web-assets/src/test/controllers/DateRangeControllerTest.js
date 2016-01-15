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

    describe('DateRangeController', function()
    {
        var $scope;
        var DateRangeController;
        var $event;

        beforeEach(module('herd'));
        beforeEach(inject(function($injector)
        {
            var $rootScope = $injector.get('$rootScope');
            var $controller = $injector.get('$controller');

            $scope = $rootScope.$new();
            DateRangeController = $controller('DateRangeController', {
                $scope : $scope
            });

            $event = {
                preventDefault : function()
                {
                },
                stopPropagation : function()
                {
                }
            };
        }));

        describe('openStartDate()', function()
        {
            beforeEach(function()
            {
                spyOn($event, 'preventDefault');
                spyOn($event, 'stopPropagation');
                DateRangeController.openStartDate($event);
            });

            it('sets isStartDateOpen to "true"', function()
            {
                expect(DateRangeController.isStartDateOpen).toBe(true);
            });

            it('sets isEndDateOpen to "false"', function()
            {
                expect(DateRangeController.isEndDateOpen).toBe(false);
            });

            it('stops event propagation', function()
            {
                expect($event.preventDefault).toHaveBeenCalled();
                expect($event.stopPropagation).toHaveBeenCalled();
            });
        });

        describe('openEndDate()', function()
        {
            beforeEach(function()
            {
                spyOn($event, 'preventDefault');
                spyOn($event, 'stopPropagation');
                DateRangeController.openEndDate($event);
            });

            it('sets isStartDateOpen to "false"', function()
            {
                expect(DateRangeController.isStartDateOpen).toBe(false);
            });

            it('sets isEndDateOpen to "true"', function()
            {
                expect(DateRangeController.isEndDateOpen).toBe(true);
            });

            it('stops event propagation', function()
            {
                expect($event.preventDefault).toHaveBeenCalled();
                expect($event.stopPropagation).toHaveBeenCalled();
            });
        });

        describe('given change() is set', function()
        {
            beforeEach(function()
            {
                DateRangeController.change = function()
                {

                };

                spyOn(DateRangeController, 'change');
            });

            describe('onStartDateChanged()', function()
            {
                beforeEach(function()
                {
                    DateRangeController.onStartDateChanged();
                });

                it('calls change()', function()
                {
                    expect(DateRangeController.change).toHaveBeenCalled();
                });
            });

            describe('onEndDateChanged()', function()
            {
                beforeEach(function()
                {
                    DateRangeController.onEndDateChanged();
                });

                it('calls change()', function()
                {
                    expect(DateRangeController.change).toHaveBeenCalled();
                });
            });
        });

        describe('given change() is not set', function()
        {
            beforeEach(function()
            {
                DateRangeController.change = undefined;
            });

            describe('onStartDateChanged()', function()
            {
                beforeEach(function()
                {
                    DateRangeController.onStartDateChanged();
                });

                it('does nothing', function()
                {
                    // This event does nothing since no handler is set for this test
                });
            });

            describe('onEndDateChanged()', function()
            {
                beforeEach(function()
                {
                    DateRangeController.onEndDateChanged();
                });

                it('does nothing', function()
                {
                    // This event does nothing since no handler is set for this test
                });
            });
        });
    });
})();