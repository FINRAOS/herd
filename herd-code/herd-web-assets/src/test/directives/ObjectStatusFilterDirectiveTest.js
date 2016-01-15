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

    describe('ObjectStatusFilterDirective', function()
    {
        var $scope;
        var view;

        beforeEach(module('herd', function($controllerProvider)
        {
            $controllerProvider.register('ObjectStatusFilterController', function()
            {

            });
        }));

        beforeEach(inject(function($injector)
        {
            var $rootScope = $injector.get('$rootScope');
            var $compile = $injector.get('$compile');

            var element = angular.element('<herd-object-status-filter>');
            element.attr({
                'filter' : 'filter',
                'do-filter' : 'doFilter()'
            });

            $scope = $rootScope.$new();
            view = $compile(element)($scope);
            $scope.$digest();
        }));

        it('binds filter correctly', function()
        {
            $scope.filter = 'test_filter';
            $scope.$digest();
            expect(view.isolateScope().vm.filter).toBe('test_filter');
        });

        it('binds doFilter() correctly', function()
        {
            $scope.doFilter = function()
            {
                
            };
            spyOn($scope, 'doFilter');
            view.isolateScope().vm.doFilter();
            expect($scope.doFilter).toHaveBeenCalled();
        });
    });
})();