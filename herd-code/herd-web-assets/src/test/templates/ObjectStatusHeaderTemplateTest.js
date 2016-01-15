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

    describe('ObjectStatusHeaderTemplate', function()
    {
        var $scope;
        var view;

        beforeEach(module('herd'));
        beforeEach(inject(function($injector)
        {
            var $rootScope = $injector.get('$rootScope');
            var $compile = $injector.get('$compile');
            var $templateCache = $injector.get('$templateCache');

            $scope = $rootScope.$new();
            view = $compile($templateCache.get('main/templates/ObjectStatusHeaderTemplate.html'))($scope);
        }));

        it('is listening to view-model change', function()
        {
            $scope.vm = {
                monthHeaders : [ {
                    id : 'test_monthHeaders_id',
                    colspan : 1234,
                    content : 'test_monthHeaders_content'
                } ],

                dateHeaders : [ {
                    id : 'test_dateHeaders_id_1',
                    isNotExpected : false,
                    content : 'test_dateHeaders_content_1'
                }, {
                    id : 'test_dateHeaders_id_2',
                    isNotExpected : true,
                    content : 'test_dateHeaders_content_2'
                } ]
            };

            $scope.$digest();

            var monthHeader = view.find('[ng-repeat="monthHeader in vm.monthHeaders track by monthHeader.id"]');
            expect(monthHeader.attr('colspan')).toBe('1234');
            expect(monthHeader.text()).toBe('test_monthHeaders_content');

            var dateHeaders = view.find('[ng-repeat="dateHeader in vm.dateHeaders track by dateHeader.id"]');

            expect(angular.element(dateHeaders[0]).hasClass('not-expected')).toBeFalsy();
            expect(angular.element(dateHeaders[0]).text()).toBe('test_dateHeaders_content_1');

            expect(angular.element(dateHeaders[1]).hasClass('not-expected')).toBeTruthy();
            expect(angular.element(dateHeaders[1]).text()).toBe('test_dateHeaders_content_2');
        });

        it('calls calendarBack()', function()
        {
            $scope.vm = {
                calendarBack : function()
                {
                }
            };

            spyOn($scope.vm, 'calendarBack');

            $scope.$digest();

            view.find('[ng-click="vm.calendarBack()"]').click();

            expect($scope.vm.calendarBack).toHaveBeenCalled();
        });

        it('calls calendarForward()', function()
        {
            $scope.vm = {
                calendarForward : function()
                {
                }
            };

            spyOn($scope.vm, 'calendarForward');

            $scope.$digest();

            view.find('[ng-click="vm.calendarForward()"]').click();

            expect($scope.vm.calendarForward).toHaveBeenCalled();
        });
    });
})();