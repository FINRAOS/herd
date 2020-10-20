function initSwagger(url)
{
   window.swaggerUi = new SwaggerUi({
        url: url,
        apisSorter: "alpha",
        dom_id: "swagger-ui-container",
        validatorUrl: null,
        supportedSubmitMethods: ['get', 'post', 'put', 'delete'],
        onComplete: function (swaggerApi, swaggerUi)
        {
            if (typeof initOAuth == "function")
            {
                initOAuth({
                    clientId: "your-client-id",
                    clientSecret: "your-client-secret",
                    realm: "your-realms",
                    appName: "your-app-name",
                    scopeSeparator: ","
                });
            }

            $('pre code').each(function (i, e)
            {
                hljs.highlightBlock(e)
            });
        },
        onFailure: function (data)
        {
            log("Unable to Load SwaggerUI");
        },
        docExpansion: "none",
        apisSorter: "alpha",
        operationsSorter: "alpha",
        showRequestHeaders: false
    });

    var updateAuth = function ()
    {
        var auth = "Basic " + btoa($('#input_user')[0].value + ":" + $('#input_pass')[0].value);
        window.swaggerUi.api.clientAuthorizations.add("key", new SwaggerClient.ApiKeyAuthorization("Authorization", auth, "header"));
    };

    $('#input_user').change(updateAuth);
    $('#input_pass').change(updateAuth);

    window.swaggerUi.load();
}

function log()
{
    if ('console' in window)
    {
        console.log.apply(console, arguments);
    }
}
