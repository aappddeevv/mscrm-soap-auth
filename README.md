##Overview
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

The library is designed for high performance data dumping. The command line
options are a bit cumbersome so you can control the performance envelope more
succintly.

##Capabilities

metadata
* Download an org's metadata.
* Download your discovery WSDL.

auth
* Run a whoami
* Show all possible orgs for a given username/password and region.
* Find a given org data services SOAP URL given a username/password and region.

query
* Count all entities in an org.
* Dump attributes for an entity to a file, basically an extract to CSV. The download can
run very fast if you first cerate a "key" partition, it shoud be the fastest downloader
anywhere that is only constrained by memory size.

other
* Run --help to print show other capabilites.


Many thanks to contributors, a particular person in general recently who remains anonymous.
