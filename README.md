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
succinctly. Some command line options do not work yet, but I'll be updating
them shortly.

##Capabilities

metadata
* Download an org's metadata describing the entities.

discovery
* Download your discovery WSDL.
* List endpoints given your userid, password and region.
* Find a given org data services SOAP URL given an username/password and region.

auth
* Run a whoami to confirm your ability to login.

query
* Count all entities in an org.
* Dump attributes for an entity to a file, basically an extract to CSV. The download can
run very fast if you first create a "key" partition, it shoud be the fastest downloader
that is only constrained by memory size. Formatted values can be downloaded and some
types of CRM values are "expanded" automatically into multiple attributes. You can
restrict the attributes downloaded through an attributes file (see metadata).
* Create a spreadsheet friendly list of enities and attribute for customizing the download.

other
* Run --help to print show other capabilities.


Many thanks to contributors, a particular person in general recently who remains anonymous
but was instrumental in pushing this application forward.

