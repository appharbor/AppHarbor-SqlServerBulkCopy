using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using NDesk.Options;

namespace AppHarbor.SqlServerBulkCopy
{
	class Program
	{
		static void Main(string[] args)
		{
			bool showHelp = false;

			string sourceServerName = null, sourceUsername = null, sourcePassword = null,
				sourceDatabaseName = null, destinationServerName = null, destinationUsername = null,
				destinationPassword = null, destinationDatabaseName = null;

			var optionSet = new OptionSet() {
				{ "h|help", "show this message and exit", x => showHelp = x != null},
				{ "srcserver=", "source server (eg. db000.appharbor.net)", x => sourceServerName = x },
				{ "srcusername=", "username on source server", x => sourceUsername = x },
				{ "srcpassword=", "password on source server", x => sourcePassword = x },
				{ "srcdatabasename=", "source database name", x => sourceDatabaseName = x },
				{ "dstserver=", "destination server", x => destinationServerName = x },
				{ "dstusername=", "username on destination server", x => destinationUsername = x },
				{ "dstpassword=", "password on destination server", x => destinationPassword = x },
				{ "dstdatabasename=", "destination database name", x => destinationDatabaseName = x },
			};

			try
			{
				optionSet.Parse(args);
				if (sourceServerName == null)
				{
					throw new OptionException("source server not specified", "srcserver");
				}
				if (sourceUsername == null)
				{
					throw new OptionException("source username not specified", "srcserver");
				}
				if (sourcePassword == null)
				{
					throw new OptionException("source password not specified", "srcserver");
				}
				if (sourceDatabaseName == null)
				{
					throw new OptionException("source database name not specified", "srcserver");
				}
				if (destinationServerName == null)
				{
					throw new OptionException("destination server not specified", "srcserver");
				}
				if (destinationUsername == null)
				{
					throw new OptionException("destination username not specified", "srcserver");
				}
				if (destinationPassword == null)
				{
					throw new OptionException("destination password not specified", "srcserver");
				}
				if (destinationDatabaseName == null)
				{
					throw new OptionException("destination database name not specified", "srcserver");
				}
			}
			catch (OptionException exception)
			{
				Console.Write(string.Format("{0}: ", AppDomain.CurrentDomain.FriendlyName));
				Console.WriteLine(exception.Message);
				Console.WriteLine("Try {0} --help for more information", AppDomain.CurrentDomain.FriendlyName);
				return;
			}

			if (showHelp)
			{
				ShowHelp(optionSet);
				return;
			}

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

		private static void ShowHelp(OptionSet optionSet)
		{
			Console.WriteLine("Copy data between Microsoft SQL Server databases");
			Console.WriteLine();
			Console.WriteLine("Options:");
			optionSet.WriteOptionDescriptions(Console.Out);
		}
	}
}
