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

    var MOCK_FILETYPES = [ 'other_type1', 'test_businessObjectFormatFileType', 'other_type2' ];

    describe('ObjectStatusFilterTemplate', function()
    {
        var $rootScope;
        var $compile;
        var $templateCache;

        beforeEach(module('herd'));
        beforeEach(inject(function($injector)
        {
            $rootScope = $injector.get('$rootScope');
            $compile = $injector.get('$compile');
            $templateCache = $injector.get('$templateCache');
        }));

        it('is listening to view-model change', function()
        {
            var $scope = $rootScope.$new();
            var template = $templateCache.get('main/templates/ObjectStatusFilterTemplate.html');
            var view = $compile(template)($scope);

            $scope.vm = {
                filter : {
                    businessObjectDefinitionName : 'test_businessObjectDefinitionName',
                    businessObjectFormatFileType : 'test_businessObjectFormatFileType',
                    fileTypes : [ 'test_businessObjectFormatFileType', 'test_businessObjectFormatFileType2' ]
                },
                fileTypes : MOCK_FILETYPES
            };

            $scope.$digest();

            var filter = {
                businessObjectDefinitionName : view.find('[ng-model="vm.filter.businessObjectDefinitionName"]').val(),
                businessObjectFormatFileType : view.find('[ng-model="vm.filter.businessObjectFormatFileType"] option:selected').text()
            };

            expect(filter.businessObjectDefinitionName).toEqual($scope.vm.filter.businessObjectDefinitionName);
            expect(filter.businessObjectFormatFileType).toEqual($scope.vm.filter.businessObjectFormatFileType);
        });

        it('updates view-model', function()
        {
            var $scope = $rootScope.$new();
            var template = $templateCache.get('main/templates/ObjectStatusFilterTemplate.html');
            var view = $compile(template)($scope);

            $scope.vm = {
                filter : {
                    fileTypes : MOCK_FILETYPES
                }
            };

            $scope.$digest();

            var filter = {
                businessObjectDefinitionName : 'test_businessObjectDefinitionName',
                businessObjectFormatFileType : 'test_businessObjectFormatFileType'
            };

            view.find('[ng-model="vm.filter.businessObjectDefinitionName"]').val(filter.businessObjectDefinitionName).change();
            // prepend 'string:' before the actual value. Angular option value
            // does this.
            view.find('[ng-model="vm.filter.businessObjectFormatFileType"]').val('string:' + filter.businessObjectFormatFileType).change();

            expect($scope.vm.filter.businessObjectDefinitionName).toEqual(filter.businessObjectDefinitionName);
            expect($scope.vm.filter.businessObjectFormatFileType).toEqual(filter.businessObjectFormatFileType);
        });

        it('calls doFilter() on submit', function()
        {
            var $scope = $rootScope.$new();
            $scope.vm = {
                doFilter : function()
                {

                }
            };
            spyOn($scope.vm, 'doFilter');

            var template = $templateCache.get('main/templates/ObjectStatusFilterTemplate.html');
            var view = $compile(template)($scope);

            $scope.$digest();

            view.find('[type=submit]').click();

            $scope.$digest();

            expect($scope.vm.doFilter).toHaveBeenCalled();
        });
    });
})();