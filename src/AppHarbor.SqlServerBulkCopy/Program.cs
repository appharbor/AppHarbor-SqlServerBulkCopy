using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using NDesk.Options;
using System.Collections.Generic;

namespace AppHarbor.SqlServerBulkCopy
{
	class Program
	{
		static void Main(string[] args)
		{
			double batchDataSize = 20000; //kB
			bool showHelp = false;
			string sourceServerName = null, sourceUsername = null, sourcePassword = null,
				sourceDatabaseName = null, destinationServerName = null, destinationUsername = null,
				destinationPassword = null, destinationDatabaseName = null;

			IEnumerable<string> ignoredTables = Enumerable.Empty<string>();

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
				{ "ignoretables=", "names of tables not to copy", x => ignoredTables = x.Split(',') },
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
					throw new OptionException("source username not specified", "srcusername");
				}
				if (sourcePassword == null)
				{
					throw new OptionException("source password not specified", "srcpassword");
				}
				if (sourceDatabaseName == null)
				{
					throw new OptionException("source database name not specified", "srcdatabasename");
				}
				if (destinationServerName == null)
				{
					throw new OptionException("destination server not specified", "dstserver");
				}
				if (destinationUsername == null)
				{
					throw new OptionException("destination username not specified", "dstusername");
				}
				if (destinationPassword == null)
				{
					throw new OptionException("destination password not specified", "dstpassword");
				}
				if (destinationDatabaseName == null)
				{
					throw new OptionException("destination database name not specified", "dstdatabasename");
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

			var destinationConnectionString = GetConnectionString(destinationServerName,
				destinationDatabaseName, destinationUsername, destinationPassword);

			var sourceConnectionString = GetConnectionString(sourceServerName,
				sourceDatabaseName, sourceUsername, sourcePassword);

			var tables = sourceDatabase.Tables
				.OfType<Table>()
				.Where(x => !x.IsSystemObject)
				.Select(x => x.Name);

			var actualExcludedTables = tables.Intersect(ignoredTables);
			if (actualExcludedTables.Any())
			{
				Console.WriteLine(string.Format("Ignoring: {0}", string.Join(",", actualExcludedTables)));
			}

			tables = tables.Except(ignoredTables);
			Console.WriteLine(string.Format("Copying {0} tables: {1}", tables.Count(), string.Join(",", tables)));

			var watch = Stopwatch.StartNew();

			foreach (var table in tables)
			{
				using (var connection = new SqlConnection(sourceConnectionString))
				{
					double rowBatchSize = 10000;
					double rows = 0;
					double dataSize;
					connection.Open();
					using (var command = connection.CreateCommand())
					{
						command.CommandText = string.Format("exec sp_spaceused '{0}'", table);
						using (var reader = command.ExecuteReader())
						{
							reader.Read();
							var rowString = (string)reader["rows"];
							rows = double.Parse(rowString);
							var dataSizeString = (string)reader["data"];
							dataSize = double.Parse(dataSizeString.Split(' ').First()); //kB
							if (rows > 0 && dataSize > 0)
							{
								double rowSize = dataSize / rows;
								rowBatchSize = (int)(batchDataSize / rowSize);
							}
						}
					}

					if (rows > 0)
					{
						Console.Write(string.Format("Copying {0} - {1} rows, {2:0.00} MB: ", table, rows, dataSize/1024));
						using (var command = connection.CreateCommand())
						{
							command.CommandText = string.Format("select * from [{0}]", table);
							using (var reader = command.ExecuteReader())
							{
								using (var bulkCopy = new SqlBulkCopy(destinationConnectionString))
								{
									bulkCopy.NotifyAfter = Math.Max((int)rows / 10, 1);
									bulkCopy.SqlRowsCopied += new SqlRowsCopiedEventHandler(SqlRowsCopied);
									bulkCopy.DestinationTableName = string.Format("[{0}]", table);
									bulkCopy.BatchSize = (int)rowBatchSize;
									bulkCopy.BulkCopyTimeout = int.MaxValue;
									bulkCopy.WriteToServer(reader);
								}
							}
						}
						Console.WriteLine();
					}
					else
					{
						Console.WriteLine(string.Format("{0} had no rows", table));
					}
				}
			}
			watch.Stop();
			Console.WriteLine("Copy complete, total time {0} s", watch.ElapsedMilliseconds/1000);
		}

		private static void SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
		{
			Console.Write(".");
		}

		private static void ShowHelp(OptionSet optionSet)
		{
			Console.WriteLine("Copy data between Microsoft SQL Server databases");
			Console.WriteLine();
			Console.WriteLine("Options:");
			optionSet.WriteOptionDescriptions(Console.Out);
		}

		private static string GetConnectionString(string serverName, string databaseName, string username,
			string password)
		{
			return string.Format("Server={0};Database={1};User ID={2};Password={3};",
				serverName, databaseName, username, password);
		}
	}
}
