## Overview
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

The library is designed to be a swiss army knife. It can be used for high performance data dumping, obtaining WSDL
files from your org service or listing all the endpoints available to you in your region. It can also
help with unit testing. The command line
options are a bit cumbersome so you can succinctly control the performance envelope. Some command line options
do not work yet, but I'll be updating them shortly.

## Capabilities
metadata
* Download an org's metadata describing the entities.

discovery
* Download your discovery WSDL.
* List endpoints given your userid, password and region.
* Find a given org data services SOAP URL given an username/password and web app URL.

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

entity
* Run a json entity script that allows you to create and modify entities as well as
run user-defined callbacks in between. This helps you programatically and consistently
create and change data while performing checks on the data in between to validate that
what you expected to happen, happened. The scripts are json oriented so you can directly
type them in. This takes the place of Excel loading and manual data management for
unit tests.

other


copy/replication
* Create a copy on an RDBMS and keep updating it using simple CDC. Only a few
steps are needed to create a local RDBMS schema using this program, copy down
the data then keep the local copy updated. The RDBMS copy is designed to be
a REPL that you draw from locally versus a targeted application database
schema e.g. a data warehouse or reporting database. The generated schema does
not enforce referential integrity in order to make it easy to load and manipulate.

Other
* Run --help to print show other capabilities.



## Thanks
Many thanks to contributors, a particular person in general recently who remains anonymous
but was instrumental in pushing this application forward. This person found many
errors and and built out some nascent capabilities.


## Notes

Miscellaneous notes:
* DDL generation library: Looked at empire-db and ddlutils but both are embedded and
specialized for their respective parent packages. Other packages
are available but are not cleanly separable from their parent packages. Had to role my own. 

