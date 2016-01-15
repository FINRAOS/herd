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

    describe('ObjectStatusBodyTemplate', function()
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
            /*
             * Special handling of template. The template is a repeat of tr
             * elements. It must be wrapped in a tbody element for $compile to
             * compile the template correctly.
             */
            view = $compile('<tbody>' + $templateCache.get('main/templates/ObjectStatusBodyTemplate.html') + '</tbody>')($scope);
        }));

        it('listens to view-model change', function()
        {
            $scope.vm = {
                properties : {
                    'contextRoot' : 'herd-app'
                },
                rows : [ {
                    id : 'a',
                    businessObjectDefinitionName : 'test_businessObjectDefinitionName',
                    statuses : [ {
                        id : 'a',
                        isNotExpected : true,
                        imageUrl : 'test_imageUrl'
                    }, {
                        id : 'b',
                        isNotExpected : false,
                        imageUrl : null
                    } ]
                } ]
            };

            $scope.$digest();

            expect(view.find('[ng-repeat="row in vm.rows track by row.id"] td.object-name').text()).toBe('test_businessObjectDefinitionName');

            var tds = view.find('[ng-repeat="status in row.statuses track by status.id"]');

            expect(angular.element(tds[0]).hasClass('not-expected')).toBeTruthy();
            expect(angular.element(tds[0]).find('img').attr('src')).toBe('/herd-app/images/test_imageUrl');

            expect(angular.element(tds[1]).hasClass('not-expected')).toBeFalsy();
            expect(angular.element(tds[1]).find('img').length).toBeFalsy();
        });
    });
})();