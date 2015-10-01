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

    describe('HeaderController', function()
    {
        var $rootScope;
        var $controller;
        var PageService;

        beforeEach(module('dm'));
        beforeEach(inject(function($injector)
        {
            $rootScope = $injector.get('$rootScope');
            $controller = $injector.get('$controller');
            PageService = $injector.get('PageService');
        }));

        describe('initialize()', function()
        {
            var $scope;
            var HeaderController;

            beforeEach(function()
            {
                $scope = $rootScope.$new();
                HeaderController = $controller('HeaderController', {
                    $scope : $scope
                });
            });

            describe('given PageService.title is set', function()
            {
                beforeEach(function()
                {
                    PageService.title = 'test_title';
                });

                describe('when a $digest happens', function()
                {
                    beforeEach(function()
                    {
                        $scope.$digest();
                    });

                    it('sets title from service', function()
                    {
                        expect(HeaderController.title).toBe('test_title');
                    });
                });
            });
        });
    });
})();