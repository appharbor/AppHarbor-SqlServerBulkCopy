# SQL Server Bulk Copy console app

This is a C# console app that copies data between Microsoft SQL Server databases. Example use:

    AppHarbor.SqlServerBulkCopy.exe --srcserver=db000.appharbor.net --srcusername=dbxx --srcpassword=srcpassword --srcdatabasename=dbxx --dstserver=.\SQLEXPRESSADV --dstusername=dbyy --dstpassword=dstpassword --dstdatabasename=dbyy --ignoretables=TableToIgnore --cleardstdatabase

For trusted connections, just skip both the username and password parameters.

Use option --checkidentityexists if you are getting sql exceptions ([table] does not contain an identity column.) when trying to clear tables without identity columns.