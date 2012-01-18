using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using NDesk.Options;

namespace AppHarbor.SqlServerBulkCopy
{
	class Program
	{
		static void Main(string[] args)
		{
			double batchDataSize = 100000; //kB
			bool showHelp = false;
			string sourceServerName = null, sourceUsername = null, sourcePassword = null,
				sourceDatabaseName = null, destinationServerName = null, destinationUsername = null,
				destinationPassword = null, destinationDatabaseName = null;
			bool clearDestinationDatabase = false, checkIdentityExists = false;

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
				{ "cleardstdatabase", "clears the destination database before copying the data", x => clearDestinationDatabase = x != null },
				{ "checkidentityexists", "only reseed identity if table has identity column", x => checkIdentityExists = x != null }
			};

			try
			{
				optionSet.Parse(args);
				if (showHelp)
				{
					ShowHelp(optionSet);
					return;
				}
				if (sourceServerName == null)
				{
					throw new OptionException("source server not specified", "srcserver");
				}
				if (sourceUsername == null && sourcePassword != null)
				{
					throw new OptionException("source username not specified", "srcusername");
				}
				if (sourcePassword == null && sourceUsername != null)
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
				if (destinationUsername == null && destinationPassword != null)
				{
					throw new OptionException("destination username not specified", "dstusername");
				}
				if (destinationPassword == null && destinationUsername != null)
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

			Console.WriteLine("Retrieving source database table information...");

			var usingTrustedConnection = string.IsNullOrEmpty(sourceUsername) && string.IsNullOrEmpty(sourcePassword);
			var sourceConnection = usingTrustedConnection
				? new ServerConnection(sourceServerName) { LoginSecure = true }
				: new ServerConnection(sourceServerName, sourceUsername, sourcePassword);
			var sourceServer = new Server(sourceConnection);
			var sourceDatabase = sourceServer.Databases[sourceDatabaseName];

			var tables = sourceDatabase.Tables
				.OfType<Table>()
				.Where(x => !x.IsSystemObject)
				.Select(x => '[' + x.Schema + ']' + ".[" + x.Name + ']')
				.ToList();

			var actualExcludedTables = tables.Intersect(ignoredTables);
			if (actualExcludedTables.Any())
			{
				Console.WriteLine(string.Format("Ignoring: {0}", string.Join(",", actualExcludedTables)));
			}

			tables = tables.Except(ignoredTables).ToList();
			Console.WriteLine(string.Format("Copying {0} tables: {1}", tables.Count(), string.Join(",", tables)));

			var destinationConnectionString = GetConnectionString(destinationServerName,
				destinationDatabaseName, destinationUsername, destinationPassword);

			var sourceConnectionString = GetConnectionString(sourceServerName,
				sourceDatabaseName, sourceUsername, sourcePassword);

			var watch = Stopwatch.StartNew();

			// clear the data before copying
			if (clearDestinationDatabase)
			{
				using (var connection = new SqlConnection(destinationConnectionString))
				{
					using (SqlCommand command = connection.CreateCommand())
					{
						// http://stackoverflow.com/questions/155246/how-do-you-truncate-all-tables-in-a-database-using-tsql/156813#156813
						StringBuilder commandBuilder = new StringBuilder();
						commandBuilder.Append(
							@"
							-- disable all constraints
							EXEC sp_msforeachtable ""ALTER TABLE ? NOCHECK CONSTRAINT all""

							-- delete data in all tables
							EXEC sp_msforeachtable ""DELETE FROM ?""

							-- enable all constraints
							exec sp_msforeachtable ""ALTER TABLE ? WITH CHECK CHECK CONSTRAINT all""
						");

						if (checkIdentityExists)
						{
							// http://stackoverflow.com/questions/6542061/reseed-sql-server-identity-columns
							commandBuilder.Append(
								@"-- reseed (auto increment to 0) on user tables with identity column
								DECLARE @reseedSql NVARCHAR(MAX);
								SET @reseedSql = N'';

								SELECT @reseedSql = @reseedSql + N'DBCC CHECKIDENT(''' 
									+ QUOTENAME(OBJECT_SCHEMA_NAME(col.[object_id]))
									+ '.' + QUOTENAME(OBJECT_NAME(col.[object_id])) 
									+ ''', RESEED, 0);' + CHAR(13) + CHAR(10)
									FROM sys.columns as col
									JOIN sys.tables as tbl
									ON col.[object_id] = tbl.[object_id]
									WHERE tbl.[type] = 'U'
									AND col.[is_identity] = 1;

								EXEC sp_executesql @reseedSql;");
						}
						else
						{
							commandBuilder.Append(@"
								-- reseed (auto increment to 0)
								EXEC sp_msforeachtable ""DBCC CHECKIDENT ( '?', RESEED, 0)""
							");
						}

						command.CommandText = commandBuilder.ToString();

						Console.WriteLine("Clearing the destination database");
						connection.Open();
						command.ExecuteNonQuery();
					}
				}
			}

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
						var columns = GetColumnNames(connection, table);

						Console.Write(string.Format("Copying {0} - {1} rows, {2:0.00} MB: ", table, rows, dataSize/1024));
						using (var command = connection.CreateCommand())
						{
							command.CommandText = string.Format("select * from {0}", table);
							using (var reader = command.ExecuteReader())
							{
								using (var bulkCopy = new SqlBulkCopy(
									destinationConnectionString, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock))
								{
									bulkCopy.NotifyAfter = Math.Max((int)rows / 10, 1);
									bulkCopy.SqlRowsCopied += new SqlRowsCopiedEventHandler(SqlRowsCopied);
									bulkCopy.DestinationTableName = table;
									bulkCopy.BatchSize = (int)rowBatchSize;
									bulkCopy.BulkCopyTimeout = int.MaxValue;
									foreach (var columnName in columns) {
										bulkCopy.ColumnMappings.Add(columnName, columnName);
									}

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

		private static List<string> GetColumnNames(SqlConnection connection, string tableName)
		{
			var sql =
				@"select column_name
				from information_schema.columns 
				where table_name = @tablename
				and columnproperty(object_id(@tablename),column_name,'iscomputed') != 1";

			using (var command = connection.CreateCommand()) {
				command.CommandText = sql;
				command.Parameters.Add(new SqlParameter("@tablename", tableName));

				var cnames = new List<string>();
				using (var reader = command.ExecuteReader()) {
					while (reader.Read()) {
						cnames.Add((string)reader[0]);
					}
				}

				return cnames;
			}
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

		static string GetConnectionString(string serverName, string databaseName, string username, string password)
		{
			var usingTrustedConnection =
				string.IsNullOrEmpty(username) &&
				string.IsNullOrEmpty(password);

			var connectionStringFormat = usingTrustedConnection
				? "Server={0};Database={1};Trusted_Connection=True;"
				: "Server={0};Database={1};User ID={2};Password={3};";

			return string.Format(connectionStringFormat, serverName, databaseName, username, password);
		}
	}
}
