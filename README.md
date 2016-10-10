This is a slightly tweaked version of authentication using SOAP to CRM
that was taken from [https://github.com/jlattimer/CRMSoapAuthJava.git](https://github.com/jlattimer/CRMSoapAuthJava.git)
and [https://blogs.msdn.microsoft.com/girishr/2011/02/04/connecting-to-crm-online-2011-web-services-using-soap-requests-only](https://blogs.msdn.microsoft.com/girishr/2011/02/04/connecting-to-crm-online-2011-web-services-using-soap-requests-only).

It was updated for scala and dispatch. Because the example now uses
scala's XML literal syntax, the SOAP templates are also much clearer.

The artifacts are not published to maven yet, so just publish
to your local repository: publish-local.

There is a main program you can use to run some specialized commands or you can use it as a library.

To create runnable packages (e.g. a that runs the main program) use: `sbt universal:packageBin`

CRM can be quite slow when returing results from online, if the info log level is set,
which is the default, then you can tail the mscrm-auth.log file to watch the incremental
data and results being requested and fetched. The debug log level is very verbose and
not suitable for normal use.


