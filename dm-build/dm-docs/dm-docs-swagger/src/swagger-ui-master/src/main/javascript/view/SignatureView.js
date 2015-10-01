'use strict';

SwaggerUi.Views.SignatureView = Backbone.View.extend({
  events: {
    'click a.description-link'       : 'switchToDescription',
    'click a.snippet-link'           : 'switchToSnippet',
    'click a.exampleXML-link'        : 'switchToXMLExample', //WIDE: register click on example link reaction
    'mousedown .snippet'             : 'snippetToTextArea',
    'mousedown .exampleXML'          : 'exampleXMLToTextArea' //WIDE: register click on example reaction
  },

  initialize: function () {

  },

  render: function(){

    $(this.el).html(Handlebars.templates.signature(this.model));

    //WIDE: first show description
    this.switchToDescription();

    this.isParam = this.model.isParam;

    if (this.isParam) {
      $('.notice', $(this.el)).text('Click to set as parameter value');
    }

    return this;
  },

  // handler for show signature
  switchToDescription: function(e){
    if (e) { e.preventDefault(); }

    $('.snippet', $(this.el)).hide();
    $('.exampleXML',  $(this.el)).hide(); //WIDE: Hide also XML example
    $('.description', $(this.el)).show();
    $('.description-link', $(this.el)).addClass('selected');
    $('.snippet-link', $(this.el)).removeClass('selected');
    $('.exampleXML-link',  $(this.el)).removeClass('selected');  //WIDE: Hide also XML example
  },

  //WIDE: handler for show XML example
  switchToXMLExample: function(e){
    if (e) { e.preventDefault(); }

    $('.snippet',     $(this.el)).hide();
    $('.exampleXML',  $(this.el)).show();
    $('.description', $(this.el)).hide();
    $('.description-link', $(this.el)).removeClass('selected');
    $('.snippet-link',     $(this.el)).removeClass('selected');
    $('.exampleXML-link',  $(this.el)).   addClass('selected');
  },

  // handler for show sample
  switchToSnippet: function(e){
    if (e) { e.preventDefault(); }

    $('.description', $(this.el)).hide();
    $('.exampleXML',  $(this.el)).hide(); //WIDE: Hide also XML example
    $('.snippet', $(this.el)).show();
    $('.snippet-link', $(this.el)).addClass('selected');
    $('.exampleXML-link',  $(this.el)).removeClass('selected'); //WIDE: Hide also XML example
    $('.description-link', $(this.el)).removeClass('selected');
  },

  //WIDE: output sample text parameter to text area
  textToTextArea: function(e, sampleText) {
    if (this.isParam) {
      if (e) { e.preventDefault(); }

      var textArea = $('textarea', $(this.el.parentNode.parentNode.parentNode));
      if ($.trim(textArea.val()) === '') {

        textArea.val(sampleText);

        setTimeout(function() {
          textArea.height( textArea.prop('scrollHeight') );
        },1);
      }
    }
  },

  //WIDE: handler for snippet to text area; also switches application type to json
  snippetToTextArea: function(e) {
    this.textToTextArea(e, this.model.sampleJSON);
    $('div select[name=parameterContentType]', $(this.el.parentNode.parentNode.parentNode)).val('application/json');
  },

  //WIDE handler for XML example to text area; also switches application type to xml
  exampleXMLToTextArea: function(e) {
    this.textToTextArea(e, this.model.exampleXML);
    $('div select[name=parameterContentType]', $(this.el.parentNode.parentNode.parentNode)).val('application/xml');
  }
});