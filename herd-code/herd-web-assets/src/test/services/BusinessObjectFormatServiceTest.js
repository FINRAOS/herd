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

    /*
     * Test BusinessObjectFormatService
     */
    describe('BusinessObjectFormatService', function()
    {
        var BusinessObjectFormatService;
        var RestService;

        beforeEach(module('herd'));
        beforeEach(inject(function($injector)
        {
            BusinessObjectFormatService = $injector.get('BusinessObjectFormatService');
            RestService = $injector.get('RestService');

            spyOn(RestService, 'request').and.returnValue('testValue');
        }));

        /*
         * Test getBusinessObjectFormatKeys()
         */
        describe('getBusinessObjectFormatKeys()', function()
        {
            describe('when request\'s latestVersion is true', function()
            {
                beforeEach(function()
                {
                    this.returnValue = BusinessObjectFormatService.getBusinessObjectFormatKeys({
                        namespace : 'testNamespace',
                        businessObjectDefinitionName : 'testBusinessObjectDefinitionName',
                        latestVersion : true
                    });
                });

                it('calls RestService.request() with latestBusinessObjectFormatVersion true', function()
                {
                    expect(RestService.request).toHaveBeenCalledWith({
                        url : '/businessObjectFormats/namespaces/testNamespace/businessObjectDefinitionNames/testBusinessObjectDefinitionName',
                        params : {
                            latestBusinessObjectFormatVersion : true
                        },
                        method : 'GET'
                    });
                });

                it('returns the result or RestService.request()', function()
                {
                    expect(this.returnValue).toBe('testValue');
                });
            });

            describe('when request\'s latestVersion is false', function()
            {
                beforeEach(function()
                {
                    this.returnValue = BusinessObjectFormatService.getBusinessObjectFormatKeys({
                        namespace : 'testNamespace',
                        businessObjectDefinitionName : 'testBusinessObjectDefinitionName',
                        latestVersion : false
                    });
                });

                it('calls RestService.request() with latestBusinessObjectFormatVersion false', function()
                {
                    expect(RestService.request).toHaveBeenCalledWith({
                        url : '/businessObjectFormats/namespaces/testNamespace/businessObjectDefinitionNames/testBusinessObjectDefinitionName',
                        params : {
                            latestBusinessObjectFormatVersion : false
                        },
                        method : 'GET'
                    });
                });

                it('returns the result or RestService.request()', function()
                {
                    expect(this.returnValue).toBe('testValue');
                });
            });

            describe('when request\'s latestVersion is undefined', function()
            {
                beforeEach(function()
                {
                    this.returnValue = BusinessObjectFormatService.getBusinessObjectFormatKeys({
                        namespace : 'testNamespace',
                        businessObjectDefinitionName : 'testBusinessObjectDefinitionName'
                    });
                });

                it('calls RestService.request() with latestBusinessObjectFormatVersion false', function()
                {
                    expect(RestService.request).toHaveBeenCalledWith({
                        url : '/businessObjectFormats/namespaces/testNamespace/businessObjectDefinitionNames/testBusinessObjectDefinitionName',
                        params : {
                            latestBusinessObjectFormatVersion : false
                        },
                        method : 'GET'
                    });
                });

                it('returns the result or RestService.request()', function()
                {
                    expect(this.returnValue).toBe('testValue');
                });
            });
        });

        /*
         * Test getBusinessObjectFormat()
         */
        describe('getBusinessObjectFormat()', function()
        {
            describe('when all fields are given in the request', function()
            {
                beforeEach(function()
                {
                    this.returnValue = BusinessObjectFormatService.getBusinessObjectFormat({
                        namespace : 'testNamespace',
                        businessObjectDefinitionName : 'testBusinessObjectDefinitionName',
                        businessObjectFormatUsage : 'testBusinessObjectFormatUsage',
                        businessObjectFormatFileType : 'testBusinessObjectFormatFileType',
                        businessObjectFormatVersion : 1234
                    });
                });

                it('calls RestService.request() with correct options', function()
                {
                    var url = [
                            '/businessObjectFormats',
                            '/namespaces/testNamespace',
                            '/businessObjectDefinitionNames/testBusinessObjectDefinitionName',
                            '/businessObjectFormatUsages/testBusinessObjectFormatUsage',
                            '/businessObjectFormatFileTypes/testBusinessObjectFormatFileType' ].join('');

                    expect(RestService.request).toHaveBeenCalledWith({
                        url : url,
                        params : {
                            businessObjectFormatVersion : 1234
                        },
                        method : 'GET'
                    });
                });

                it('returns the result of RestService.request()', function()
                {
                    expect(this.returnValue).toBe('testValue');
                });
            });
        });
    });
})();