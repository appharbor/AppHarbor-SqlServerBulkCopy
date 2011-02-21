using System;
using System.Data.SqlClient;
using System.Linq;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using NDesk.Options;
using System.Diagnostics;

namespace AppHarbor.SqlServerBulkCopy
{
	class Program
	{
		static void Main(string[] args)
		{
			string sourceServerName = null, sourceUsername = null, sourcePassword = null,
				sourceDatabaseName = null, destinationServerName = null, destinationUsername = null,
				destinationPassword = null, destinationDatabaseName = null;

			var optionSet = new OptionSet() {
				{ "srcserver=", "", x => sourceServerName = x },
				{ "srcusername=", "", x => sourceUsername = x },
				{ "srcpassword=", "", x => sourcePassword = x },
				{ "srcdatabasename=", "", x => sourceDatabaseName = x },
				{ "dstserver=", "", x => destinationServerName = x },
				{ "dstusername=", "", x => destinationUsername = x },
				{ "dstpassword=", "", x => destinationPassword = x },
				{ "dstdatabasename=", "", x => destinationDatabaseName = x },
			};

			optionSet.Parse(args);

			var sourceConnection = new ServerConnection(sourceServerName, sourceUsername, sourcePassword);
			var sourceServer = new Server(sourceConnection);
			var sourceDatabase = sourceServer.Databases[sourceDatabaseName];

			var connectionstringFormatString = "Server={0};Database={1};User ID={2};Password={3};";

			var destinationConnectionString = string.Format(connectionstringFormatString,
				destinationServerName, destinationDatabaseName, destinationUsername,
				destinationPassword);
			
			var sourceConnectionString = string.Format(connectionstringFormatString,
				sourceServerName, sourceDatabaseName, sourceUsername,
				sourcePassword);

			var watch = Stopwatch.StartNew();

			foreach (Table table in sourceDatabase.Tables
				.OfType<Table>()
				.Where(x => !x.IsSystemObject))
			{
				Console.WriteLine(string.Format("Copying {0}", table.Name));

				using (var connection = new SqlConnection(sourceConnectionString))
				{
					var sqlCommand = new SqlCommand(string.Format("select * from {0}", table.Name), connection);
					connection.Open();
					var reader = sqlCommand.ExecuteReader();

					using (var bulkCopy = new SqlBulkCopy(destinationConnectionString))
					{
						bulkCopy.DestinationTableName = table.Name;
						bulkCopy.BatchSize = 10000;
						bulkCopy.BulkCopyTimeout = int.MaxValue;
						bulkCopy.WriteToServer(reader);
					}
				}
			}
			watch.Stop();
			Console.WriteLine("Copy complete, total time {0}", watch.ElapsedMilliseconds);
		}
	}
}
