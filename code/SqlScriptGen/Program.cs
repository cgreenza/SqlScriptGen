using System;
using System.Collections.Generic;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using System.IO;
using Microsoft.Data.SqlClient;
using System.Xml;
using System.Text.RegularExpressions;

namespace SqlScriptGen
{
	class Program
	{
			static void PrintUsage()
		{
			Console.WriteLine("ScriptGen.exe optionsFile sourceServer sourceDatabase outputPath");
			Console.WriteLine("ScriptGen.exe optionsFile");
			Console.WriteLine("optionsFile: xml scripting options file");
			Console.WriteLine("sourceServer: server name of SQL server");
			Console.WriteLine("sourceDatabase: database name");
			Console.WriteLine("outputPath: output path for scripts");
			Console.WriteLine();
			Console.WriteLine("Example:");
			Console.WriteLine("ScriptGen.exe options.xml server1 db1 .\\");
			Console.WriteLine();
			Console.WriteLine("Press any key to continue");
			Console.ReadKey();
		}

		static void Main(string[] args)
		{
			try
			{
                System.Threading.Thread.CurrentThread.CurrentCulture = System.Globalization.CultureInfo.InvariantCulture;

                string sServer = null;
				string sDb = null;
				string sOutPath;

				if (args.Length == 4)
				{
					sServer = args[1];
					sDb = args[2];
					sOutPath = args[3];
				}
				else if (args.Length == 2)
				{
					sOutPath = args[1];
				}
				else if (args.Length == 1)
				{
					sOutPath = ".\\";
				}
				else
				{
					PrintUsage();
					return;
				}


				XmlDocument xmlOptions = new XmlDocument();
				xmlOptions.Load(args[0]);

				string user = null;
				string password = null;
				{
					var serverNode = xmlOptions.SelectSingleNode("/scriptOptions/@server");
					if (serverNode != null)
						sServer = serverNode.Value;

					var databaseNode = xmlOptions.SelectSingleNode("/scriptOptions/@database");
					if (databaseNode != null)
						sDb = databaseNode.Value;

					var userNode = xmlOptions.SelectSingleNode("/scriptOptions/@user");
					if (userNode != null)
						user = userNode.Value;

					var passwordNode = xmlOptions.SelectSingleNode("/scriptOptions/@password");
					if (passwordNode != null)
						password = passwordNode.Value;
				}
					
				if (sServer == null)
				{
					Console.WriteLine("server not specified");
					Console.ReadKey();
					return;
				}
				
				if (sDb == null)
				{
					Console.WriteLine("database not specified");
					Console.ReadKey();
					return;
				}

				SqlServerVersion targetServerVersion;
				{
					XmlNode sqlVersion = xmlOptions.SelectSingleNode("/scriptOptions/@sqlVersion");
					if (sqlVersion != null)
					{
						if (sqlVersion.Value == "SQL2000")
							targetServerVersion = SqlServerVersion.Version80;
						else if (sqlVersion.Value == "SQL2005")
							targetServerVersion = SqlServerVersion.Version90;
						else if (sqlVersion.Value == "SQL2008")
							targetServerVersion = SqlServerVersion.Version100;
						else if (sqlVersion.Value == "SQL2008R2")
							targetServerVersion = SqlServerVersion.Version105;
						else if(!Enum.TryParse(sqlVersion.Value, out targetServerVersion))
						{
							Console.WriteLine("Invalid target server version specified");
							PrintUsage();
							return;
						}
					}
					else
						targetServerVersion = SqlServerVersion.Version90;
				}


				Server theServer;
				SqlConnection cn;
				if (!string.IsNullOrEmpty(user) || !string.IsNullOrEmpty(password))
				{
					theServer = new Server(new ServerConnection(sServer, user, password));
					cn = new SqlConnection(String.Format("server={0};database={1};uid={2};pwd={3}", sServer, sDb, user, password));
				}
				else
				{
					theServer = new Server(new ServerConnection(sServer));
					cn = new SqlConnection(String.Format("server={0};database={1};Integrated Security=SSPI", sServer, sDb));
				}

				theServer.SetDefaultInitFields(typeof(Table), "IsSystemObject");
				theServer.SetDefaultInitFields(typeof(Trigger), "IsSystemObject");
				theServer.SetDefaultInitFields(typeof(View), "IsSystemObject");
				theServer.SetDefaultInitFields(typeof(StoredProcedure), "IsSystemObject");

				Database db = theServer.Databases[sDb];

                if (db == null)
                {
                    throw new Exception(string.Format("Database[{0}] incorrect.", sDb));
                }

				Console.WriteLine("Scripting Tables...");
				ScriptTables(theServer, db, xmlOptions, targetServerVersion, sOutPath);

				Console.WriteLine("Scripting Triggers...");
				ScriptTriggers(theServer, db, xmlOptions, targetServerVersion, sOutPath);

				Console.WriteLine("User defined types...");
				ScriptUserDefinedTypes(theServer, db, xmlOptions, targetServerVersion, sOutPath);

				Console.WriteLine("Scripting Procs...");
				ScriptProcs(theServer, db, xmlOptions, targetServerVersion, sOutPath);

				Console.WriteLine("Scripting Views...");
				ScriptViews(theServer, db, xmlOptions, targetServerVersion, sOutPath);

				Console.WriteLine("Scripting UDFs...");
				ScriptUDF(theServer, db, xmlOptions, targetServerVersion, sOutPath);

				Console.WriteLine("Scripting Values...");
				using (cn)
				{
					cn.Open();
					ScriptValues(cn, theServer, db, xmlOptions, targetServerVersion, sOutPath);
				}

				Console.WriteLine("Done.");
			}
			catch (Exception ex)
			{
				Console.WriteLine("Error:");
				Console.WriteLine(ex.ToString());
				Console.ReadKey();
			}
		}

		private static void ScriptTables(Server theServer, Database db, XmlDocument xmlOptions, SqlServerVersion targetServerVersion, string sOutPath)
		{
			foreach (XmlNode xmlScriptTables in xmlOptions.SelectNodes("/scriptOptions/scriptTables"))
			{
				List<Regex> includeMatchList = new List<Regex>();
				foreach (XmlNode xmlInclude in xmlScriptTables.SelectNodes("include/@match"))
				{
					string sRegEx = xmlInclude.Value;
					try
					{
						Regex regEx = new System.Text.RegularExpressions.Regex(sRegEx);
						includeMatchList.Add(regEx);
					}
					catch (ArgumentException ex)
					{
						throw new Exception(String.Format("Invalid RegEx: {0}", sRegEx), ex);
					}
				}

				List<Regex> excludeMatchList = new List<Regex>();
				foreach (XmlNode xmlExclude in xmlScriptTables.SelectNodes("exclude/@match"))
				{
					string sRegEx = xmlExclude.Value;
					try
					{
						Regex regEx = new System.Text.RegularExpressions.Regex(sRegEx);
						excludeMatchList.Add(regEx);
					}
					catch (ArgumentException ex)
					{
						throw new Exception(String.Format("Invalid RegEx: {0}", sRegEx), ex);
					}
				}

				string outputFile = xmlScriptTables.SelectSingleNode("@outputFile").Value;
				using (FileStream outputStream = new FileStream(Path.Combine(sOutPath, outputFile), FileMode.Create))
				{
					ScriptUtil.ScriptTables(theServer, db, targetServerVersion, outputStream, delegate(string tableName)
					{
						if (includeMatchList.Count != 0 && !HasMatch(includeMatchList, tableName))
							return false; // not in match list

						if (HasMatch(excludeMatchList, tableName))
							return false; // on explicit exclude list

						return true; // script this table
					});
				}
			}
		}

		private static void ScriptTriggers(Server theServer, Database db, XmlDocument xmlOptions, SqlServerVersion targetServerVersion, string sOutPath)
		{
			foreach (XmlNode xmlScriptTables in xmlOptions.SelectNodes("/scriptOptions/scriptTriggers"))
			{
				string outputFile = xmlScriptTables.SelectSingleNode("@outputFile").Value;
				using (FileStream outputStream = new FileStream(Path.Combine(sOutPath, outputFile), FileMode.Create))
				{
					ScriptUtil.ScriptTriggers(theServer, db, targetServerVersion, outputStream);
				}
			}
		}

		private static void ScriptUDF(Server theServer, Database db, XmlDocument xmlOptions, SqlServerVersion targetServerVersion, string sOutPath)
		{
			foreach (XmlNode xmlScriptTables in xmlOptions.SelectNodes("/scriptOptions/scriptUDFs"))
			{
				string outputFile = xmlScriptTables.SelectSingleNode("@outputFile").Value;
				using (FileStream outputStream = new FileStream(Path.Combine(sOutPath, outputFile), FileMode.Create))
				{
					ScriptUtil.ScriptUDFs(theServer, db, targetServerVersion, outputStream);
				}
			}
		}

		private static void ScriptViews(Server theServer, Database db, XmlDocument xmlOptions, SqlServerVersion targetServerVersion, string sOutPath)
		{
			foreach (XmlNode xmlScriptTables in xmlOptions.SelectNodes("/scriptOptions/scriptViews"))
			{
				string outputFile = xmlScriptTables.SelectSingleNode("@outputFile").Value;
				using (FileStream outputStream = new FileStream(Path.Combine(sOutPath, outputFile), FileMode.Create))
				{
					ScriptUtil.ScriptViews(theServer, db, targetServerVersion, outputStream);
				}
			}
		}

		private static void ScriptProcs(Server theServer, Database db, XmlDocument xmlOptions, SqlServerVersion targetServerVersion, string sOutPath)
		{
			foreach (XmlNode xmlScriptTables in xmlOptions.SelectNodes("/scriptOptions/scriptProcs"))
			{
				string outputFile = xmlScriptTables.SelectSingleNode("@outputFile").Value;
				using (FileStream outputStream = new FileStream(Path.Combine(sOutPath, outputFile), FileMode.Create))
				{
					ScriptUtil.ScriptProcs(theServer, db, targetServerVersion, outputStream);
				}
			}
		}

		private static void ScriptUserDefinedTypes(Server theServer, Database db, XmlDocument xmlOptions, SqlServerVersion targetServerVersion, string sOutPath)
		{
			foreach (XmlNode xmlScriptTables in xmlOptions.SelectNodes("/scriptOptions/scriptUserDefinedTypes"))
			{
				string outputFile = xmlScriptTables.SelectSingleNode("@outputFile").Value;
				using (FileStream outputStream = new FileStream(Path.Combine(sOutPath, outputFile), FileMode.Create))
				{
					ScriptUtil.ScriptUserDefinedTypes(theServer, db, targetServerVersion, outputStream);
				}
			}
		}

		private static void ScriptValues(SqlConnection cn, Server theServer, Database db, XmlDocument xmlOptions, SqlServerVersion targetServerVersion, string sOutPath)
		{
			foreach (XmlNode xmlScriptTables in xmlOptions.SelectNodes("/scriptOptions/scriptValues"))
			{
				List<ScriptUtil.TableOption> tableList = new List<ScriptUtil.TableOption>();
				foreach (XmlNode xmlTable in xmlScriptTables.SelectNodes("table"))
				{
					XmlNode xmlTableName = xmlTable.SelectSingleNode("@name");
					XmlNode xmlTop = xmlTable.SelectSingleNode("@top");
					XmlNode xmlWhere = xmlTable.SelectSingleNode("@where");

					string tableName = xmlTableName.Value;
					Table t = FindTable(db.Tables, tableName);
					if (t == null)
						throw new Exception(String.Format("Can't find table {0}", tableName));
					ScriptUtil.TableOption to = new ScriptUtil.TableOption();
					to.Table = t;

					if (xmlTop == null)
					{
						to.TopX = 0;
					}
					else
					{
						to.TopX = int.Parse(xmlTop.Value);
					}

					if (xmlWhere != null)
						to.WhereClause = xmlWhere.Value;

					tableList.Add(to);
				}

				string outputFile = xmlScriptTables.SelectSingleNode("@outputFile").Value;
				using (FileStream outputStream = new FileStream(Path.Combine(sOutPath, outputFile), FileMode.Create))
				{
					ScriptUtil.ScriptValues(cn, theServer, db, targetServerVersion, outputStream, tableList);
				}
			}
		}

		private static bool HasMatch(List<Regex> matchList, string value)
		{
			foreach (Regex ex in matchList)
			{
				if (ex.IsMatch(value))
					return true;
			}
			return false;
		}

		public static Table FindTable(TableCollection list, string sTableName)
		{
			for (int i = 0; i < list.Count; i++)
			{
				if (string.Compare(list[i].Name, sTableName, true) == 0)
					return list[i];
			}
			return null;
		}

	}
}
