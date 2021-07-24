using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.Smo;
using System.IO;
using System.Collections.Specialized;
using Microsoft.Data.SqlClient;
using Index = Microsoft.SqlServer.Management.Smo.Index;

namespace SqlScriptGen
{
	static class ScriptUtil
	{

		public static void ScriptTables(
			Server server,
			Database db,
			SqlServerVersion serverVersion,
			Stream outputStream,
			Predicate<string> tableIncludePredicate)
		{
			List<Table> tableList = new List<Table>();

			foreach (Table t in db.Tables)
			{
				if (t.IsSystemObject)
					continue;

				if (tableIncludePredicate(t.Name))
					tableList.Add(t);
			}

			List<Index> indexList = new List<Index>();
			List<ForeignKey> foreignKeyList = new List<ForeignKey>();
			List<Check> checksList = new List<Check>();

			foreach (Table t in tableList)
			{
				foreach (Index o in t.Indexes)
					indexList.Add(o);

				foreach (ForeignKey o in t.ForeignKeys)
					foreignKeyList.Add(o);

				foreach (Check o in t.Checks)
					checksList.Add(o);
			}

			tableList.Sort(delegate(Table x, Table y) { return String.Compare(x.Name, y.Name); });
			indexList.Sort(delegate(Index x, Index y) { return String.Compare(x.Name, y.Name); });
			foreignKeyList.Sort(delegate(ForeignKey x, ForeignKey y) { return String.Compare(x.Name, y.Name); });
			checksList.Sort(delegate(Check x, Check y) { return String.Compare(x.Name, y.Name); });

			ScriptingOptions opt = new ScriptingOptions();
			opt.AllowSystemObjects = false;
			opt.ClusteredIndexes = false;
			//	opt.Default = false;
			opt.DriAll = false;
			opt.IncludeIfNotExists = false;
			opt.Indexes = false;
			opt.NoCollation = true;
			opt.NonClusteredIndexes = false;
			opt.Permissions = false;
			opt.Triggers = false;
			opt.WithDependencies = false;
			opt.IncludeDatabaseContext = false;
			opt.TargetServerVersion = serverVersion;
			opt.NoFileGroup = true;

			Scripter oScripter = new Scripter();
			oScripter.Options = opt;
			oScripter.Server = server;

			// script out tables
			using (StreamWriter sw = new StreamWriter(outputStream))
			{
				opt.Default = true;
				opt.DriDefaults = true;
				sw.WriteLine("-- TABLES");
				sw.WriteLine();
				WriteColl(sw, oScripter.Script(tableList.ToArray()));
				opt.Default = false;
				opt.DriDefaults = false;

				opt.DriAllConstraints = true;
				opt.DriAllKeys = true;
				opt.DriChecks = true;
				opt.DriClustered = true;
				opt.DriIndexes = true;
				opt.DriNonClustered = true;
				opt.Indexes = true;
				opt.ClusteredIndexes = true;
				opt.NonClusteredIndexes = true;

				sw.WriteLine();
				sw.WriteLine("-- INDEXES");
				sw.WriteLine();
				WriteColl(sw, oScripter.Script(indexList.ToArray()));

				sw.WriteLine();
				sw.WriteLine("-- FOREIGN KEYS");
				sw.WriteLine();
				WriteColl(sw, oScripter.Script(foreignKeyList.ToArray()));

				sw.WriteLine();
				sw.WriteLine("-- CHECKS");
				sw.WriteLine();
				WriteColl(sw, oScripter.Script(checksList.ToArray()));

				opt.DriAllConstraints = false;
				opt.DriAllKeys = false;
				opt.DriChecks = false;
				opt.DriClustered = false;
				opt.DriIndexes = false;
				opt.DriNonClustered = false;
				opt.Indexes = false;
				opt.ClusteredIndexes = false;
				opt.NonClusteredIndexes = false;
			}
		}

		public static void ScriptTriggers(
			Server server,
			Database db,
			SqlServerVersion serverVersion,
			Stream outputStream)
		{
			List<Table> tableList = new List<Table>();

			foreach (Table t in db.Tables)
			{
				if (t.IsSystemObject)
					continue;

				tableList.Add(t);
			}

			List<Trigger> triggerList = new List<Trigger>();

			foreach (Table t in tableList)
			{
				foreach (Trigger o in t.Triggers)
					triggerList.Add(o);
			}

			if (server.Information.Version.Major >= 9)
			{
				foreach (Trigger o in db.Triggers)
				{
					if (o.IsSystemObject)
						continue;
					triggerList.Add(o);
				}
			}

			triggerList.Sort(delegate(Trigger x, Trigger y) { return String.Compare(x.Name, y.Name); });

			ScriptingOptions opt = new ScriptingOptions();
			opt.AllowSystemObjects = false;
			opt.ClusteredIndexes = false;
			//	opt.Default = false;
			opt.DriAll = false;
			opt.IncludeIfNotExists = false;
			opt.Indexes = false;
			opt.NoCollation = true;
			opt.NonClusteredIndexes = false;
			opt.Permissions = false;
			opt.Triggers = false;
			opt.WithDependencies = false;
			opt.IncludeDatabaseContext = false;
			opt.TargetServerVersion = serverVersion;

			Scripter oScripter = new Scripter();
			oScripter.Options = opt;
			oScripter.Server = server;

			// script out triggers
			using (StreamWriter sw = new StreamWriter(outputStream))
			{
				sw.WriteLine("-- DELETE OLD");
				sw.WriteLine();
				foreach (Trigger oTrigger in triggerList)
					sw.WriteLine("IF EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[{0}].[{1}]'))\r\n\tDROP TRIGGER [{0}].[{1}]\r\nGO\r\n", "dbo", oTrigger.Name);

				sw.WriteLine();
				sw.WriteLine("-- CREATE NEW");
				sw.WriteLine();
				WriteColl(sw, oScripter.Script(triggerList.ToArray()));
			}
		}

		internal static void ScriptUDFs(Server theServer, Database db, SqlServerVersion targetServerVersion, FileStream outputStream)
		{
			List<UserDefinedFunction> alUDF = new List<UserDefinedFunction>();

			foreach (UserDefinedFunction o in db.UserDefinedFunctions)
			{
				if (o.IsSystemObject)
					continue;
				alUDF.Add(o);
			}

			alUDF.Sort(delegate(UserDefinedFunction x, UserDefinedFunction y) { return String.Compare(x.Name, y.Name); });

			ScriptingOptions opt = new ScriptingOptions();
			opt.AllowSystemObjects = false;
			opt.ClusteredIndexes = false;
			//	opt.Default = false;
			opt.DriAll = false;
			opt.IncludeIfNotExists = false;
			opt.Indexes = false;
			opt.NoCollation = true;
			opt.NonClusteredIndexes = false;
			opt.Permissions = false;
			opt.Triggers = false;
			opt.WithDependencies = false;
			opt.IncludeDatabaseContext = false;
			opt.TargetServerVersion = targetServerVersion;

			Scripter oScripter = new Scripter();
			oScripter.Options = opt;
			oScripter.Server = theServer;

			using (StreamWriter sw = new StreamWriter(outputStream))
			{
				sw.WriteLine("-- DELETE OLD");
				sw.WriteLine();

				foreach (UserDefinedFunction oUDF in alUDF)
				{
					sw.WriteLine("IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{0}].[{1}]') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))\r\n\tDROP FUNCTION [{0}].[{1}]\r\nGO\r\n", oUDF.Schema, oUDF.Name);
				}

				opt.IncludeIfNotExists = false;
				sw.WriteLine();
				sw.WriteLine("-- CREATE NEW");
				sw.WriteLine();

				foreach (UserDefinedFunction oUDF in alUDF)
				{
					foreach (string sLine in oUDF.Script())
					{
						sw.WriteLine(sLine);
						sw.WriteLine("GO");
					}
				}
			}
		}

		internal static void ScriptViews(Server theServer, Database db, SqlServerVersion targetServerVersion, FileStream outputStream)
		{
			List<View> alViews = new List<View>();

			foreach (View o in db.Views)
			{
				if (o.IsSystemObject)
					continue;
				alViews.Add(o);
			}
			alViews.Sort(delegate(View x, View y) { return String.Compare(x.Name, y.Name); });

			ScriptingOptions opt = new ScriptingOptions();
			opt.AllowSystemObjects = false;
			opt.ClusteredIndexes = false;
			//	opt.Default = false;
			opt.DriAll = false;
			opt.IncludeIfNotExists = false;
			opt.Indexes = false;
			opt.NoCollation = true;
			opt.NonClusteredIndexes = false;
			opt.Permissions = false;
			opt.Triggers = false;
			opt.WithDependencies = false;
			opt.IncludeDatabaseContext = false;
			opt.TargetServerVersion = targetServerVersion;

			Scripter oScripter = new Scripter();
			oScripter.Options = opt;
			oScripter.Server = theServer;

			using (StreamWriter sw = new StreamWriter(outputStream))
			{
				sw.WriteLine("-- DELETE OLD");
				sw.WriteLine();

				foreach (View oView in alViews)
					sw.WriteLine("IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[{0}].[{1}]') AND OBJECTPROPERTY(id, N'IsView') = 1)\r\n\tDROP VIEW [{0}].[{1}]\r\nGO\r\n", oView.Schema, oView.Name);

				opt.IncludeIfNotExists = true;
				sw.WriteLine();
				sw.WriteLine("-- CREATE NEW");
				sw.WriteLine();

				foreach (View oView in alViews)
				{
					foreach (string sLine in oView.Script())
					{
						sw.WriteLine(sLine);
						sw.WriteLine("GO");
					}
				}
			}
		}

		internal static void ScriptProcs(Server theServer, Database db, SqlServerVersion targetServerVersion, FileStream outputStream)
		{
			List<StoredProcedure> alStoredProcedures = new List<StoredProcedure>();

			foreach (StoredProcedure o in db.StoredProcedures)
			{
				if (o.IsSystemObject)
					continue;
				alStoredProcedures.Add(o);
			}
			alStoredProcedures.Sort(delegate(StoredProcedure x, StoredProcedure y) { return String.Compare(x.Name, y.Name); });

			ScriptingOptions opt = new ScriptingOptions();
			opt.AllowSystemObjects = false;
			opt.ClusteredIndexes = false;
			//	opt.Default = false;
			opt.DriAll = false;
			opt.IncludeIfNotExists = false;
			opt.Indexes = false;
			opt.NoCollation = true;
			opt.NonClusteredIndexes = false;
			opt.Permissions = false;
			opt.Triggers = false;
			opt.WithDependencies = false;
			opt.IncludeDatabaseContext = false;
			opt.TargetServerVersion = targetServerVersion;

			Scripter oScripter = new Scripter();
			oScripter.Options = opt;
			oScripter.Server = theServer;

			using (StreamWriter sw = new StreamWriter(outputStream))
			{
				sw.WriteLine("-- DELETE OLD");
				sw.WriteLine();
				foreach (StoredProcedure oProc in alStoredProcedures)
					sw.WriteLine("IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'[{0}].[{1}]') AND OBJECTPROPERTY(id, N'IsProcedure') = 1 )\r\n\tDROP PROC [{0}].[{1}]\r\nGO\r\n", oProc.Schema, oProc.Name);

				sw.WriteLine();
				sw.WriteLine("-- CREATE NEW");
				sw.WriteLine();
				WriteColl(sw, oScripter.Script(alStoredProcedures.ToArray()));
			}
		}

		internal static void ScriptUserDefinedTypes(Server theServer, Database db, SqlServerVersion targetServerVersion, FileStream outputStream)
		{
			var alUserDefinedTypes = db.UserDefinedTypes.Cast<UserDefinedType>().OrderBy(x => x.Name);
			var alUserDefinedDataTypes = db.UserDefinedDataTypes.Cast<UserDefinedDataType>().OrderBy(x => x.Name);
			var alUserDefinedTableTypes = db.UserDefinedTableTypes.Cast<UserDefinedTableType>().OrderBy(x => x.Name);
			
			ScriptingOptions opt = new ScriptingOptions();
			opt.AllowSystemObjects = false;
			opt.ClusteredIndexes = false;
			//	opt.Default = false;
			opt.DriAll = false;
			opt.IncludeIfNotExists = false;
			opt.Indexes = false;
			opt.NoCollation = true;
			opt.NonClusteredIndexes = false;
			opt.Permissions = false;
			opt.Triggers = false;
			opt.WithDependencies = false;
			opt.IncludeDatabaseContext = false;
			opt.TargetServerVersion = targetServerVersion;

			Scripter oScripter = new Scripter();
			oScripter.Options = opt;
			oScripter.Server = theServer;

			using (StreamWriter sw = new StreamWriter(outputStream))
			{
				sw.WriteLine("-- User Defined Types");
				sw.WriteLine();
				WriteColl(sw, oScripter.Script(alUserDefinedTypes.ToArray()));
				sw.WriteLine();

				sw.WriteLine("-- User Defined Data Types");
				sw.WriteLine();
				WriteColl(sw, oScripter.Script(alUserDefinedDataTypes.ToArray()));
				sw.WriteLine();

				sw.WriteLine("-- User Defined Table Types");
				sw.WriteLine();
				WriteColl(sw, oScripter.Script(alUserDefinedTableTypes.ToArray()));
				sw.WriteLine();
			}
		}

		public class TableOption
		{
			public Table Table;
			public int TopX;
			public string WhereClause;
		}

		internal static void ScriptValues(
			SqlConnection cn,
			Server theServer, Database db, SqlServerVersion targetServerVersion, FileStream outputStream, List<TableOption> tableList)
		{
			SqlDataReader dr = null;
			StreamWriter sw = null;

			try
			{
				sw = new StreamWriter(outputStream);

				SqlCommand cmd = new SqlCommand();
				cmd.Connection = cn;

				foreach (TableOption table in tableList)
				{
					string sTable = table.Table.Name;
					bool fHasIdentityColumn = TableHasIdentity(table.Table);

					if (table.TopX > 0)
					{
						sw.WriteLine("-- {0} x {1}", sTable, table.TopX);
					}
					else
					{
						sw.WriteLine("-- {0}", sTable);
					}

					if (fHasIdentityColumn) { sw.WriteLine(string.Format("SET IDENTITY_INSERT {0} ON", sTable)); sw.WriteLine("GO"); }

					if (table.TopX > 0)
					{
						cmd.CommandText = String.Format("select top {2} {1} from {0}", sTable, GetColumnNames(table.Table, true, false), table.TopX);
					}
					else
					{
						cmd.CommandText = String.Format("select {1} from {0}", sTable, GetColumnNames(table.Table, true, false));
					}

					if (!string.IsNullOrEmpty(table.WhereClause))
					{
						cmd.CommandText += string.Format(" WHERE ({0})", table.WhereClause);
					}

					dr = cmd.ExecuteReader();

					StringBuilder sbInsert = new StringBuilder();
					sbInsert.AppendFormat("insert into [{0}] (", sTable);

					for (int iColIdx = 0; iColIdx < dr.FieldCount; iColIdx++)
					{
						if (iColIdx != 0)
							sbInsert.Append(", ");
						sbInsert.AppendFormat("[{0}]", dr.GetName(iColIdx));
					}
					sbInsert.Append(") values (");
					string sInsert = sbInsert.ToString();

					while (dr.Read())
					{
						sw.Write(sInsert);

						for (int iColIdx = 0; iColIdx < dr.FieldCount; iColIdx++)
						{
							if (iColIdx != 0)
								sw.Write(", ");
							object oVal = dr[iColIdx];
							if (oVal == DBNull.Value)
								sw.Write("null");
							else if (oVal is string)
								sw.Write("'{0}'", ((string)oVal).Replace("'", "''"));
							else if (oVal is DateTime)
								sw.Write("'{0:yyyy-MM-dd HH:mm:ss}'", oVal);
							else if (oVal is bool)
								sw.Write(((bool)oVal) ? "1" : "0");
							else if (oVal is byte[])
								sw.Write(ToHex((byte[])oVal));
							else
								sw.Write(oVal.ToString());
						}
						sw.WriteLine(")");
					}

					sw.WriteLine("GO");
					if (fHasIdentityColumn) { sw.WriteLine(string.Format("SET IDENTITY_INSERT {0} OFF", sTable)); sw.WriteLine("GO"); }
					sw.WriteLine();

					dr.Close();
					dr = null;

				}
			}
			finally
			{
				if (dr != null)
					dr.Close();
				if (sw != null)
					sw.Close();
			}
		}

		private static void WriteColl(StreamWriter sw, StringCollection sc)
		{
			foreach (string s in sc)
			{
				sw.WriteLine(s.Trim());
				sw.WriteLine("GO");
				sw.WriteLine();
			}
		}
		public static bool TableHasIdentity(Table tbl)
		{
			for (int i = 0; i < tbl.Columns.Count; i++)
			{
				if (tbl.Columns[i].Identity)
					return true;
			}
			return false;
		}

		public static string GetColumnNames(Table tbl, bool fIncludeIdentity, bool fIncludeTimeStamp)
		{
			StringBuilder sbCols = new StringBuilder();
			bool fFirst = true;

			foreach (Column col in tbl.Columns)
			{
				if (col.Identity && !fIncludeIdentity)
					continue;
				if (col.DataType.SqlDataType == SqlDataType.Timestamp && !fIncludeTimeStamp)
					continue;
				if (fFirst)
				{
					sbCols.Append(col.Name);
					fFirst = false;
				}
				else
					sbCols.AppendFormat(", {0}", col.Name);
			}

			return sbCols.ToString();
		}

		public static string ToHex(byte[] buffer)
		{
			if (buffer == null)
				return "null";

			StringBuilder sb = new StringBuilder("0x");
			for (int i = 0; i < buffer.Length; i++)
			{
				sb.AppendFormat("{0:X2}", buffer[i]);
			}

			return sb.ToString();
		}
	}
}
