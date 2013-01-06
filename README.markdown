# SQL Server Bulk Copy console app

This is a C# console app that copies data between Microsoft SQL Server databases. Example use:

    AppHarbor.SqlServerBulkCopy.exe --srcserver=db000.appharbor.net --srcusername=dbxx --srcpassword=srcpassword --srcdatabasename=dbxx --dstserver=.\SQLEXPRESSADV --dstusername=dbyy --dstpassword=dstpassword --dstdatabasename=dbyy --ignoretables=TableToIgnore --cleardstdatabase

For trusted connections, just skip both the username and password parameters.

Use option --checkidentityexists if you are getting sql exceptions ([table] does not contain an identity column.) when trying to clear tables without identity columns.

The SMO dlls cannot be distributed with the source code due to licensing restrictions. You need to get Microsoft SQL Server 2008 R2 Shared Management Objects from the [Microsoft Download Center](http://www.microsoft.com/download/en/details.aspx?id=16978#SMO). Place `Microsoft.SqlServer.ConnectionInfo.dll`, `Microsoft.SqlServer.Management.Sdk.Sfc.dll`, `Microsoft.SqlServer.Smo.dll` and `Microsoft.SqlServer.SqlEnum.dll` in the `lib\SqlServerManagementObjects` directory.
