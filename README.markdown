# SQL Server Bulk Copy console app

This is a C# console app that copies data between Microsoft SQL Server databases. Example use:

    AppHarbor.SqlServerBulkCopy.exe --srcserver=db000.appharbor.net --srcusername=dbxx --srcpassword=srcpassword --srcdatabasename=dbxx --dstserver=.\SQLEXPRESSADV --dstusername=dbyy --dstpassword=dstpassword --dstdatabasename=dbyy --ignoretables=TableToIgnore

For trusted connections, just skip both the username and password parameters.