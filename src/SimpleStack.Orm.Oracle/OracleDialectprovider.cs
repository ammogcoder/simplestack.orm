using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Text;
using Dapper;
using Oracle.ManagedDataAccess.Client;
using SimpleStack.Orm;
using SimpleStack.Orm.Expressions;

namespace SimpleStack.Orm.Oracle
{
    /// <summary>An oracle ORM lite dialect provider.</summary>
    public class OracleDialectProvider : DialectProviderBase<OracleDialectProvider>
    {
        /// <summary>The reserved.</summary>
		private readonly List<string> RESERVED = new List<string>(new[] {
            "USER","ORDER","PASSWORD", "ACTIVE","LEFT","DOUBLE", "FLOAT", "DECIMAL","STRING", "DATE","DATETIME", "TYPE","TIMESTAMP", "COMMENT"
        });

        /// <summary>Gets or sets the identifier of the last insert.</summary>
        /// <value>The identifier of the last insert.</value>
		internal long LastInsertId { get; set; }

        /// <summary>true to compact unique identifier.</summary>
		protected bool CompactGuid;

        /// <summary>The default unique identifier definition.</summary>
		internal const string DefaultGuidDefinition = "VARCHAR(37)";

        /// <summary>The compact unique identifier definition.</summary>
		internal const string CompactGuidDefinition = "CHAR(16) CHARACTER SET OCTETS";

        /// <summary>
        /// Initializes a new instance of the NServiceKit.OrmLite.Oracle.OracleOrmLiteDialectProvider
        /// class.
        /// </summary>
		public OracleDialectProvider()
            : this(false)
        {
        }

        /// <summary>
        /// Initializes a new instance of the NServiceKit.OrmLite.Oracle.OracleOrmLiteDialectProvider
        /// class.
        /// </summary>
        /// <param name="compactGuid">true to compact unique identifier.</param>
		public OracleDialectProvider(bool compactGuid)
        {
            CompactGuid = compactGuid;
            base.BoolColumnDefinition = base.IntColumnDefinition;
            base.GuidColumnDefinition = CompactGuid ? CompactGuidDefinition : DefaultGuidDefinition;
            base.AutoIncrementDefinition = string.Empty;
            base.DateTimeColumnDefinition = "TIMESTAMP";
            base.TimeColumnDefinition = "TIME";
            base.RealColumnDefinition = "FLOAT";
            base.DefaultStringLength = 128;
            base.InitColumnTypeMap();
            base.ParamPrefix = ":";
        }
        
        //public override string ToSelectStatement(Type tableType, string sqlFilter, params object[] filterParams)
        //{
        //    var sql = new StringBuilder();
        //    const string SelectStatement = "SELECT ";
        //    var modelDef = GetModel(tableType);
        //    var isFullSelectStatement =
        //        !string.IsNullOrEmpty(sqlFilter)
        //        && sqlFilter.Trim().Length > SelectStatement.Length
        //        && sqlFilter.Trim().Substring(0, SelectStatement.Length).ToUpper().Equals(SelectStatement);

        //    if (isFullSelectStatement) return sqlFilter.SqlFormat(filterParams);

        //    sql.AppendFormat("SELECT {0} \nFROM {1}",
        //                     GetColumnNames(modelDef),
        //                     GetQuotedTableName(modelDef));
        //    if (!string.IsNullOrEmpty(sqlFilter))
        //    {
        //        sqlFilter = sqlFilter.SqlFormat(filterParams);
        //        if (!sqlFilter.StartsWith("\nORDER ", StringComparison.InvariantCultureIgnoreCase)
        //            && !sqlFilter.StartsWith("\nROWS ", StringComparison.InvariantCultureIgnoreCase)) // ROWS <m> [TO <n>])
        //        {
        //            sql.Append("\nWHERE ");
        //        }
        //        sql.Append(sqlFilter);
        //    }
        //    return sql.ToString();
        //}

        /// <summary>Creates parameterized insert statement.</summary>
        /// <exception cref="Exception">Thrown when an exception error condition occurs.</exception>
        /// <param name="connection">       The connection.</param>
        /// <param name="objWithProperties">The object with properties.</param>
        /// <param name="insertFields">     The insert fields.</param>
        /// <returns>The new parameterized insert statement.</returns>
        //public override IDbCommand CreateParameterizedInsertStatement(IDbConnection connection, object objWithProperties, ICollection<string> insertFields = null)
        //{
        //    if (insertFields == null)
        //        insertFields = new List<string>();

        //    var sbColumnNames = new StringBuilder();
        //    var sbColumnValues = new StringBuilder();
        //    var modelDef = GetModel(objWithProperties.GetType());

        //    var dbCommand = connection.CreateCommand();
        //    dbCommand.CommandTimeout = OrmLiteConfig.CommandTimeout;
        //    foreach (var fieldDef in modelDef.FieldDefinitions)
        //    {
        //        if (fieldDef.IsComputed) continue;
        //        if (insertFields.Count > 0 && !insertFields.Contains(fieldDef.Name)) continue;

        //        if ((fieldDef.AutoIncrement || !string.IsNullOrEmpty(fieldDef.Sequence)
        //            || fieldDef.Name == OrmLiteConfig.IdField)
        //            && dbCommand != null)
        //        {

        //            if (fieldDef.AutoIncrement && string.IsNullOrEmpty(fieldDef.Sequence))
        //            {
        //                fieldDef.Sequence = Sequence(
        //                    (modelDef.IsInSchema
        //                        ? modelDef.Schema + "_" + NamingStrategy.GetTableName(modelDef.ModelName)
        //                        : NamingStrategy.GetTableName(modelDef.ModelName)),
        //                    fieldDef.FieldName, fieldDef.Sequence);
        //            }

        //            PropertyInfo pi = objWithProperties.GetType().GetProperty(fieldDef.Name,
        //                BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.FlattenHierarchy);

        //            var result = GetNextValue(dbCommand, fieldDef.Sequence, pi.GetValue(objWithProperties, new object[] { }));
        //            if (pi.PropertyType == typeof(String))
        //                ReflectionUtils.SetProperty(objWithProperties, pi, result.ToString());
        //            else if (pi.PropertyType == typeof(Int16))
        //                ReflectionUtils.SetProperty(objWithProperties, pi, Convert.ToInt16(result));
        //            else if (pi.PropertyType == typeof(Int32))
        //                ReflectionUtils.SetProperty(objWithProperties, pi, Convert.ToInt32(result));
        //            else if (pi.PropertyType == typeof(Guid))
        //                ReflectionUtils.SetProperty(objWithProperties, pi, result);
        //            else
        //                ReflectionUtils.SetProperty(objWithProperties, pi, Convert.ToInt64(result));
        //        }

        //        //insertFields contains Property "Name" of fields to insert ( that's how expressions work )
        //        if (insertFields.Count > 0 && !insertFields.Contains(fieldDef.Name)) continue;

        //        if (sbColumnNames.Length > 0) sbColumnNames.Append(",");
        //        if (sbColumnValues.Length > 0) sbColumnValues.Append(",");

        //        try
        //        {
        //            sbColumnNames.Append(GetQuotedColumnName(fieldDef.FieldName));
        //            sbColumnValues.Append(ParamString)
        //                          .Append(fieldDef.FieldName);

        //            AddParameterForFieldToCommand(dbCommand, fieldDef, objWithProperties);
        //        }
        //        catch (Exception ex)
        //        {
        //            Log.Error("ERROR in CreateParameterizedInsertStatement(): " + ex.Message, ex);
        //            throw;
        //        }
        //    }

        //    dbCommand.CommandText = string.Format("INSERT INTO {0} ({1}) VALUES ({2})",
        //                                        GetQuotedTableName(modelDef), sbColumnNames, sbColumnValues);
        //    return dbCommand;
        //}

        /// <summary>Converts this object to an insert row statement.</summary>
        /// <exception cref="Exception">Thrown when an exception error condition occurs.</exception>
        /// <param name="dbCommand">        The database command.</param>
        /// <param name="objWithProperties">The object with properties.</param>
        /// <param name="insertFields">     The insert fields.</param>
        /// <returns>The given data converted to a string.</returns>
		//public override string ToInsertRowStatement(IDbCommand dbCommand, object objWithProperties, ICollection<string> insertFields = null)
  //      {
  //          if (insertFields == null)
  //              insertFields = new List<string>();

  //          var sbColumnNames = new StringBuilder();
  //          var sbColumnValues = new StringBuilder();

  //          var tableType = objWithProperties.GetType();
  //          var modelDef = GetModel(tableType);

  //          foreach (var fieldDef in modelDef.FieldDefinitions)
  //          {

  //              if (fieldDef.IsComputed) continue;
  //              if (insertFields.Count > 0 && !insertFields.Contains(fieldDef.Name)) continue;

  //              if ((fieldDef.AutoIncrement || !string.IsNullOrEmpty(fieldDef.Sequence)
  //                  || fieldDef.Name == OrmLiteConfig.IdField)
  //                  && dbCommand != null)
  //              {

  //                  if (fieldDef.AutoIncrement && string.IsNullOrEmpty(fieldDef.Sequence))
  //                  {
  //                      fieldDef.Sequence = Sequence(
  //                          (modelDef.IsInSchema
  //                              ? modelDef.Schema + "_" + NamingStrategy.GetTableName(modelDef.ModelName)
  //                              : NamingStrategy.GetTableName(modelDef.ModelName)),
  //                          fieldDef.FieldName, fieldDef.Sequence);
  //                  }

  //                  PropertyInfo pi = tableType.GetProperty(fieldDef.Name,
  //                      BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.FlattenHierarchy);

  //                  var result = GetNextValue(dbCommand, fieldDef.Sequence, pi.GetValue(objWithProperties, new object[] { }));
  //                  if (pi.PropertyType == typeof(String))
  //                      ReflectionUtils.SetProperty(objWithProperties, pi, result.ToString());
  //                  else if (pi.PropertyType == typeof(Int16))
  //                      ReflectionUtils.SetProperty(objWithProperties, pi, Convert.ToInt16(result));
  //                  else if (pi.PropertyType == typeof(Int32))
  //                      ReflectionUtils.SetProperty(objWithProperties, pi, Convert.ToInt32(result));
  //                  else if (pi.PropertyType == typeof(Guid))
  //                      ReflectionUtils.SetProperty(objWithProperties, pi, result);
  //                  else
  //                      ReflectionUtils.SetProperty(objWithProperties, pi, Convert.ToInt64(result));
  //              }

  //              if (sbColumnNames.Length > 0) sbColumnNames.Append(",");
  //              if (sbColumnValues.Length > 0) sbColumnValues.Append(",");

  //              try
  //              {
  //                  sbColumnNames.Append(string.Format("{0}", GetQuotedColumnName(fieldDef.FieldName)));
  //                  if (!string.IsNullOrEmpty(fieldDef.Sequence) && dbCommand == null)
  //                      sbColumnValues.Append(string.Format("@{0}", fieldDef.Name));
  //                  else
  //                      sbColumnValues.Append(fieldDef.GetQuotedValue(objWithProperties));
  //              }
  //              catch (Exception)
  //              {
  //                  throw;
  //              }
  //          }

  //          var sql = string.Format("INSERT INTO {0} ({1}) VALUES ({2}) ",
  //                                  GetQuotedTableName(modelDef), sbColumnNames, sbColumnValues);

  //          return sql;
  //      }

  //      /// <summary>Converts this object to an update row statement.</summary>
  //      /// <exception cref="Exception">Thrown when an exception error condition occurs.</exception>
  //      /// <param name="objWithProperties">The object with properties.</param>
  //      /// <param name="updateFields">     The update fields.</param>
  //      /// <returns>The given data converted to a string.</returns>
		//public override string ToUpdateRowStatement(object objWithProperties, ICollection<string> updateFields = null)
  //      {
  //          if (updateFields == null)
  //              updateFields = new List<string>();

  //          var sqlFilter = new StringBuilder();
  //          var sql = new StringBuilder();
  //          var tableType = objWithProperties.GetType();
  //          var modelDef = GetModel(tableType);

  //          foreach (var fieldDef in modelDef.FieldDefinitions)
  //          {
  //              if (fieldDef.IsComputed) continue;

  //              try
  //              {
  //                  if ((fieldDef.IsPrimaryKey || fieldDef.Name == OrmLiteConfig.IdField)
  //                      && updateFields.Count == 0)
  //                  {
  //                      if (sqlFilter.Length > 0) sqlFilter.Append(" AND ");

  //                      sqlFilter.AppendFormat("{0} = {1}",
  //                          GetQuotedColumnName(fieldDef.FieldName),
  //                          fieldDef.GetQuotedValue(objWithProperties));

  //                      continue;
  //                  }
  //                  if (updateFields.Count > 0 && !updateFields.Contains(fieldDef.Name)) continue;
  //                  if (sql.Length > 0) sql.Append(",");
  //                  sql.AppendFormat("{0} = {1}",
  //                      GetQuotedColumnName(fieldDef.FieldName),
  //                      fieldDef.GetQuotedValue(objWithProperties));
  //              }
  //              catch (Exception)
  //              {
  //                  throw;
  //              }
  //          }

  //          var updateSql = string.Format("UPDATE {0} \nSET {1} {2}",
  //              GetQuotedTableName(modelDef), sql, (sqlFilter.Length > 0 ? "\nWHERE " + sqlFilter : ""));

  //          return updateSql;
  //      }

  //      /// <summary>Converts the objWithProperties to a delete row statement.</summary>
  //      /// <exception cref="Exception">Thrown when an exception error condition occurs.</exception>
  //      /// <param name="objWithProperties">The object with properties.</param>
  //      /// <returns>objWithProperties as a string.</returns>
		//public override string ToDeleteRowStatement(object objWithProperties)
  //      {
  //          var tableType = objWithProperties.GetType();
  //          var modelDef = GetModel(tableType);

  //          var sqlFilter = new StringBuilder();

  //          foreach (var fieldDef in modelDef.FieldDefinitions)
  //          {
  //              try
  //              {
  //                  if (fieldDef.IsPrimaryKey || fieldDef.Name == OrmLiteConfig.IdField)
  //                  {
  //                      if (sqlFilter.Length > 0) sqlFilter.Append(" AND ");
  //                      sqlFilter.AppendFormat("{0} = {1}",
  //                          GetQuotedColumnName(fieldDef.FieldName),
  //                          fieldDef.GetQuotedValue(objWithProperties));
  //                  }
  //              }
  //              catch (Exception)
  //              {
  //                  throw;
  //              }
  //          }

  //          var deleteSql = string.Format("DELETE FROM {0} WHERE {1}",
  //              GetQuotedTableName(modelDef), sqlFilter);

  //          return deleteSql;
  //      }

  //      /// <summary>Converts a tableType to a create table statement.</summary>
  //      /// <param name="tableType">Type of the table.</param>
  //      /// <returns>tableType as a string.</returns>
		//public override string ToCreateTableStatement(Type tableType)
  //      {
  //          var sbColumns = new StringBuilder();
  //          var sbConstraints = new StringBuilder();
  //          var sbPk = new StringBuilder();

  //          var modelDef = GetModel(tableType);
  //          foreach (var fieldDef in modelDef.FieldDefinitions)
  //          {
  //              if (fieldDef.IsPrimaryKey)
  //              {
  //                  sbPk.AppendFormat(sbPk.Length != 0 ? ",{0}" : "{0}", GetQuotedColumnName(fieldDef.FieldName));
  //              }

  //              if (sbColumns.Length != 0) sbColumns.Append(", \n  ");

  //              var columnDefinition = GetColumnDefinition(
  //                  fieldDef.FieldName,
  //                  fieldDef.FieldType,
  //                  fieldDef.IsPrimaryKey,
  //                  fieldDef.AutoIncrement,
  //                  fieldDef.IsNullable,
  //                  fieldDef.FieldLength,
  //                  fieldDef.Scale,
  //                  fieldDef.DefaultValue);

  //              sbColumns.Append(columnDefinition);

  //              if (fieldDef.ForeignKey == null) continue;

  //              var refModelDef = GetModel(fieldDef.ForeignKey.ReferenceType);

  //              sbConstraints.AppendFormat(", \n\n  CONSTRAINT {0} FOREIGN KEY ({1}) REFERENCES {2} ({3})",
  //                  GetQuotedName(fieldDef.ForeignKey.GetForeignKeyName(modelDef, refModelDef, NamingStrategy, fieldDef)),
  //                  GetQuotedColumnName(fieldDef.FieldName),
  //                  GetQuotedTableName(refModelDef),
  //                  GetQuotedColumnName(refModelDef.PrimaryKey.FieldName));
  //          }

  //          if (sbPk.Length != 0) sbColumns.AppendFormat(", \n  PRIMARY KEY({0})", sbPk);

  //          var sql = new StringBuilder(string.Format(
  //              "CREATE TABLE {0} \n(\n  {1}{2} \n)  \n",
  //              GetQuotedTableName(modelDef),
  //              sbColumns,
  //              sbConstraints));

  //          return sql.ToString();

  //      }

  //      /// <summary>Converts this object to a create sequence statement.</summary>
  //      /// <param name="tableType">   Type of the table.</param>
  //      /// <param name="sequenceName">Name of the sequence.</param>
  //      /// <returns>The given data converted to a string.</returns>
  //      public override string ToCreateSequenceStatement(Type tableType, string sequenceName)
  //      {
  //          var result = "";
  //          var modelDef = GetModel(tableType);

  //          foreach (var fieldDef in modelDef.FieldDefinitions)
  //          {
  //              if (fieldDef.AutoIncrement || !fieldDef.Sequence.IsNullOrEmpty())
  //              {
  //                  string seqName = Sequence((modelDef.IsInSchema
  //                          ? modelDef.Schema + "_" + NamingStrategy.GetTableName(modelDef.ModelName)
  //                          : NamingStrategy.GetTableName(modelDef.ModelName)), fieldDef.FieldName, fieldDef.Sequence);

  //                  if (seqName.EqualsIgnoreCase(sequenceName))
  //                  {
  //                      result = "CREATE SEQUENCE " + seqName;
  //                      break;
  //                  }
  //              }
  //          }
  //          return result;
  //      }

  //      /// <summary>Converts a tableType to a create sequence statements.</summary>
  //      /// <param name="tableType">Type of the table.</param>
  //      /// <returns>tableType as a List&lt;string&gt;</returns>
		//public override List<string> ToCreateSequenceStatements(Type tableType)
  //      {
  //          var gens = new List<string>();
  //          foreach (var seq in SequenceList(tableType))
  //          {
  //              gens.Add("CREATE SEQUENCE " + seq);
  //          }
  //          return gens;
  //      }

  //      /// <summary>Sequence list.</summary>
  //      /// <param name="tableType">Type of the table.</param>
  //      /// <returns>A List&lt;string&gt;</returns>
  //      public override List<string> SequenceList(Type tableType)
  //      {
  //          var gens = new List<string>();
  //          var modelDef = GetModel(tableType);

  //          foreach (var fieldDef in modelDef.FieldDefinitions)
  //          {
  //              if (fieldDef.AutoIncrement || !fieldDef.Sequence.IsNullOrEmpty())
  //              {
  //                  var seqName = Sequence((modelDef.IsInSchema
  //                          ? modelDef.Schema + "_" + NamingStrategy.GetTableName(modelDef.ModelName)
  //                          : NamingStrategy.GetTableName(modelDef.ModelName)), fieldDef.FieldName, fieldDef.Sequence);

  //                  if (gens.IndexOf(seqName) == -1)
  //                      gens.Add(seqName);
  //              }
  //          }
  //          return gens;
  //      }

        /// <summary>Gets column definition.</summary>
        /// <param name="fieldName">    Name of the field.</param>
        /// <param name="fieldType">    Type of the field.</param>
        /// <param name="isPrimaryKey"> true if this object is primary key.</param>
        /// <param name="autoIncrement">true to automatically increment.</param>
        /// <param name="isNullable">   true if this object is nullable.</param>
        /// <param name="fieldLength">  Length of the field.</param>
        /// <param name="scale">        The scale.</param>
        /// <param name="defaultValue"> The default value.</param>
        /// <returns>The column definition.</returns>
		public override string GetColumnDefinition(string fieldName, Type fieldType,
            bool isPrimaryKey, bool autoIncrement, bool isNullable,
            int? fieldLength, int? scale, string defaultValue)
        {
            string fieldDefinition;

            if (fieldType == typeof(string))
            {
                fieldDefinition = string.Format(StringLengthColumnDefinitionFormat,
                                                fieldLength.GetValueOrDefault(DefaultStringLength));
            }
            else if (fieldType == typeof(Decimal))
            {
                fieldDefinition = string.Format("{0} ({1},{2})", DecimalColumnDefinition,
                    fieldLength.GetValueOrDefault(DefaultDecimalPrecision),
                    scale.GetValueOrDefault(DefaultDecimalScale));
            }
            else
            {
                fieldDefinition = GetColumnTypeDefinition(fieldType,fieldName,fieldLength);
            }

            var sql = new StringBuilder();
            sql.AppendFormat("{0} {1}", GetQuotedColumnName(fieldName), fieldDefinition);

            if (!isNullable)
            {
                sql.Append(" NOT NULL");
            }

            if (!string.IsNullOrEmpty(defaultValue))
            {
                sql.AppendFormat(DefaultValueFormat, defaultValue);
            }

            return sql.ToString();
        }

        /// <summary>Converts a tableType to a create index statements.</summary>
        /// <param name="tableType">Type of the table.</param>
        /// <returns>tableType as a List&lt;string&gt;</returns>
		//public override List<string> ToCreateIndexStatements(Type tableType)
  //      {
  //          var sqlIndexes = new List<string>();

  //          var modelDef = GetModel(tableType);
  //          foreach (var fieldDef in modelDef.FieldDefinitions)
  //          {
  //              if (!fieldDef.IsIndexed) continue;

  //              var indexName = GetIndexName(
  //                  fieldDef.IsUnique,
  //                  (modelDef.IsInSchema
  //                      ? modelDef.Schema + "_" + modelDef.ModelName
  //                      : modelDef.ModelName).SafeVarName(),
  //                  fieldDef.FieldName);

  //              sqlIndexes.Add(
  //                  ToCreateIndexStatement(fieldDef.IsUnique, indexName, modelDef, fieldDef.FieldName, false));
  //          }

  //          foreach (var compositeIndex in modelDef.CompositeIndexes)
  //          {
  //              var indexName = GetCompositeIndexNameWithSchema(compositeIndex, modelDef);
  //              var indexNames = string.Join(",", compositeIndex.FieldNames.ToArray());

  //              sqlIndexes.Add(
  //                  ToCreateIndexStatement(compositeIndex.Unique, indexName, modelDef, indexNames, false));
  //          }

  //          return sqlIndexes;
  //      }

        /// <summary>Converts this object to a create index statement.</summary>
        /// <param name="isUnique">  true if this object is unique.</param>
        /// <param name="indexName"> Name of the index.</param>
        /// <param name="modelDef">  The model definition.</param>
        /// <param name="fieldName"> Name of the field.</param>
        /// <param name="isCombined">true if this object is combined.</param>
        /// <returns>The given data converted to a string.</returns>
		protected override string ToCreateIndexStatement(bool isUnique, string indexName, ModelDefinition modelDef, string fieldName, bool isCombined)
        {
            return string.Format("CREATE {0} INDEX {1} ON {2} ({3} ) \n",
                isUnique ? "UNIQUE" : "",
                indexName,
                GetQuotedTableName(modelDef),
                GetQuotedColumnName(fieldName));
        }

        /// <summary>Converts this object to an exist statement.</summary>
        /// <exception cref="Exception">Thrown when an exception error condition occurs.</exception>
        /// <param name="fromTableType">    Type of from table.</param>
        /// <param name="objWithProperties">The object with properties.</param>
        /// <param name="sqlFilter">        A filter specifying the SQL.</param>
        /// <param name="filterParams">     Options for controlling the filter.</param>
        /// <returns>The given data converted to a string.</returns>
		//public override string ToExistStatement(Type fromTableType,
  //          object objWithProperties,
  //          string sqlFilter,
  //          params object[] filterParams)
  //      {

  //          var fromModelDef = GetModel(fromTableType);
  //          var sql = new StringBuilder();
  //          sql.AppendFormat("SELECT 1 \nFROM {0}", GetQuotedTableName(fromModelDef));

  //          var filter = new StringBuilder();

  //          if (objWithProperties != null)
  //          {
  //              var tableType = objWithProperties.GetType();

  //              if (fromTableType != tableType)
  //              {
  //                  int i = 0;
  //                  var fpk = new List<FieldDefinition>();
  //                  var modelDef = GetModel(tableType);

  //                  foreach (var def in modelDef.FieldDefinitions)
  //                  {
  //                      if (def.IsPrimaryKey) fpk.Add(def);
  //                  }

  //                  foreach (var fieldDef in fromModelDef.FieldDefinitions)
  //                  {
  //                      if (fieldDef.IsComputed) continue;
  //                      try
  //                      {
  //                          if (fieldDef.ForeignKey != null
  //                              && GetModel(fieldDef.ForeignKey.ReferenceType).ModelName == modelDef.ModelName)
  //                          {
  //                              if (filter.Length > 0) filter.Append(" AND ");
  //                              filter.AppendFormat("{0} = {1}", GetQuotedColumnName(fieldDef.FieldName),
  //                                  fpk[i].GetQuotedValue(objWithProperties));
  //                              i++;
  //                          }
  //                      }
  //                      catch (Exception)
  //                      {
  //                          throw;
  //                      }
  //                  }

  //              }
  //              else
  //              {
  //                  var modelDef = GetModel(tableType);
  //                  foreach (var fieldDef in modelDef.FieldDefinitions)
  //                  {
  //                      if (fieldDef.IsComputed) continue;
  //                      try
  //                      {
  //                          if (fieldDef.IsPrimaryKey)
  //                          {
  //                              if (filter.Length > 0) filter.Append(" AND ");
  //                              filter.AppendFormat("{0} = {1}",
  //                                  GetQuotedColumnName(fieldDef.FieldName),
  //                                  fieldDef.GetQuotedValue(objWithProperties));
  //                          }
  //                      }
  //                      catch (Exception)
  //                      {
  //                          throw;
  //                      }
  //                  }
  //              }

  //              if (filter.Length > 0) sql.AppendFormat("\nWHERE {0} ", filter);
  //          }

  //          if (!string.IsNullOrEmpty(sqlFilter))
  //          {
  //              sqlFilter = sqlFilter.SqlFormat(filterParams);
  //              if (!sqlFilter.StartsWith("\nORDER ", StringComparison.InvariantCultureIgnoreCase)
  //                  && !sqlFilter.StartsWith("\nROWS ", StringComparison.InvariantCultureIgnoreCase)) // ROWS <m> [TO <n>])
  //              {
  //                  sql.Append(filter.Length > 0 ? " AND  " : "\nWHERE ");
  //              }
  //              sql.Append(sqlFilter);
  //          }

  //          var sb = new StringBuilder("select 1  from dual where");
  //          sb.AppendFormat(" exists ({0})", sql.ToString());
  //          return sb.ToString();
  //      }

        /// <summary>Converts this object to a select from procedure statement.</summary>
        /// <exception cref="Exception">Thrown when an exception error condition occurs.</exception>
        /// <param name="fromObjWithProperties">from object with properties.</param>
        /// <param name="outputModelType">      Type of the output model.</param>
        /// <param name="sqlFilter">            A filter specifying the SQL.</param>
        /// <param name="filterParams">         Options for controlling the filter.</param>
        /// <returns>The given data converted to a string.</returns>
		//public override string ToSelectFromProcedureStatement(
  //          object fromObjWithProperties,
  //          Type outputModelType,
  //          string sqlFilter,
  //          params object[] filterParams)
  //      {

  //          var sbColumnValues = new StringBuilder();

  //          Type fromTableType = fromObjWithProperties.GetType();

  //          var modelDef = GetModel(fromTableType);

  //          foreach (var fieldDef in modelDef.FieldDefinitions)
  //          {
  //              if (sbColumnValues.Length > 0) sbColumnValues.Append(",");

  //              try
  //              {
  //                  sbColumnValues.Append(fieldDef.GetQuotedValue(fromObjWithProperties));
  //              }
  //              catch (Exception)
  //              {
  //                  throw;
  //              }
  //          }

  //          var sql = new StringBuilder();
  //          sql.AppendFormat("SELECT {0} \nFROM  {1} {2}{3}{4}  \n",
  //              GetColumnNames(GetModel(outputModelType)),
  //              GetQuotedTableName(modelDef),
  //              sbColumnValues.Length > 0 ? "(" : "",
  //              sbColumnValues,
  //              sbColumnValues.Length > 0 ? ")" : "");

  //          if (!string.IsNullOrEmpty(sqlFilter))
  //          {
  //              sqlFilter = sqlFilter.SqlFormat(filterParams);
  //              if (!sqlFilter.StartsWith("\nORDER ", StringComparison.InvariantCultureIgnoreCase)
  //                  && !sqlFilter.StartsWith("\nROWS ", StringComparison.InvariantCultureIgnoreCase)) // ROWS <m> [TO <n>]
  //              {
  //                  sql.Append("\nWHERE ");
  //              }
  //              sql.Append(sqlFilter);
  //          }

  //          return sql.ToString();
  //      }

        /// <summary>Converts the objWithProperties to an execute procedure statement.</summary>
        /// <exception cref="Exception">Thrown when an exception error condition occurs.</exception>
        /// <param name="objWithProperties">The object with properties.</param>
        /// <returns>objWithProperties as a string.</returns>
		//public override string ToExecuteProcedureStatement(object objWithProperties)
  //      {
  //          var sbColumnValues = new StringBuilder();

  //          var tableType = objWithProperties.GetType();
  //          var modelDef = GetModel(tableType);

  //          foreach (var fieldDef in modelDef.FieldDefinitions)
  //          {
  //              if (sbColumnValues.Length > 0) sbColumnValues.Append(",");
  //              try
  //              {
  //                  sbColumnValues.Append(fieldDef.GetQuotedValue(objWithProperties));
  //              }
  //              catch (Exception)
  //              {
  //                  throw;
  //              }
  //          }

  //          var sql = string.Format("EXECUTE PROCEDURE {0} {1}{2}{3};",
  //              GetQuotedTableName(modelDef),
  //              sbColumnValues.Length > 0 ? "(" : "",
  //              sbColumnValues,
  //              sbColumnValues.Length > 0 ? ")" : "");

  //          return sql;
  //      }

  //      /// <summary>Gets the next value.</summary>
  //      /// <param name="dbCmd">   The database command.</param>
  //      /// <param name="sequence">The sequence.</param>
  //      /// <param name="value">   The value.</param>
  //      /// <returns>The next value.</returns>
		//private object GetNextValue(IDbCommand dbCmd, string sequence, object value)
  //      {
  //          Object retObj;

  //          if (value.ToString() != "0")
  //          {
  //              long nv;
  //              if (long.TryParse(value.ToString(), out nv))
  //              {
  //                  LastInsertId = nv;
  //                  retObj = LastInsertId;
  //              }
  //              else
  //              {
  //                  LastInsertId = 0;
  //                  retObj = value;
  //              }
  //              return retObj;

  //          }

  //          //dbCmd.CommandText = string.Format("SELECT {0}.NEXTVAL FROM dual", Quote(sequence));
  //          var sql = string.Format("SELECT {0}.NEXTVAL FROM dual", Quote(sequence));
  //          dbCmd.CommandText = sql;
  //          var result = dbCmd.GetLongScalar();

  //          LastInsertId = result;
  //          return result;
  //      }

        /// <summary>Gets or sets a value indicating whether the quote names.</summary>
        /// <value>true if quote names, false if not.</value>
		public bool QuoteNames { get; set; }

        /// <summary>Quotes.</summary>
        /// <param name="name">The name.</param>
        /// <returns>A string.</returns>
		private string Quote(string name)
        {
            return QuoteNames
                ? $"\"{name}\""
                : RESERVED.Contains(name.ToUpper())
                    ? $"\"{name}\""
                    : name;
        }

        /// <inheritdoc />
        /// <summary>Gets quoted name.</summary>
        /// <param name="name">Name of the field.</param>
        /// <returns>The quoted name.</returns>
		public override string GetQuotedName(string name)
        {
            return Quote(name);
        }

        public override DbConnection CreateIDbConnection(string connectionString)
        {
            return new OracleConnection(connectionString);
        }

        /// <summary>Gets quoted table name.</summary>
        /// <param name="modelDef">The model definition.</param>
        /// <returns>The quoted table name.</returns>
        public override string GetQuotedTableName(ModelDefinition modelDef)
        {
            if (!modelDef.IsInSchema)
                return Quote(NamingStrategy.GetTableName(modelDef.ModelName));

            return Quote(string.Format("{0}_{1}", modelDef.Schema,
                NamingStrategy.GetTableName(modelDef.ModelName)));
        }

        /// <summary>Gets quoted table name.</summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns>The quoted table name.</returns>
        public override string GetQuotedTableName(string tableName)
        {
            return Quote(NamingStrategy.GetTableName(tableName));
        }

        /// <summary>Gets quoted column name.</summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <returns>The quoted column name.</returns>
		public override string GetQuotedColumnName(string fieldName)
        {
            return Quote(NamingStrategy.GetColumnName(fieldName));
        }

        /// <summary>Sequences.</summary>
        /// <param name="modelName">Name of the model.</param>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="sequence"> The sequence.</param>
        /// <returns>A string.</returns>
		//private string Sequence(string modelName, string fieldName, string sequence)
  //      {
  //          return sequence.IsNullOrEmpty()
  //              ? Quote(modelName + "_" + fieldName + "_GEN")
  //              : Quote(sequence);
  //      }

        /// <summary>Expression visitor.</summary>
        /// <typeparam name="T">Generic type parameter.</typeparam>
        /// <returns>A SqlExpressionVisitor&lt;T&gt;</returns>
		public override SqlExpressionVisitor<T> ExpressionVisitor<T>()
        {
            return new OracleSqlExpressionVisitor<T>(this);
        }

        public override bool DoesTableExist(IDbConnection connection, string tableName)
        {
            if (!QuoteNames & !RESERVED.Contains(tableName.ToUpper()))
            {
                tableName = tableName.ToUpper();
            }


            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SELECT count(*) FROM all_tables where table_name=:tablename";

                var param = cmd.CreateParameter();
                param.ParameterName = "tablename";
                param.Value = tableName;
                cmd.Parameters.Add(param);

                return cmd.ExecuteNonQuery() > 0;
            }
        }

        public override bool DoesSequenceExist(IDbConnection dbCmd, string sequenceName)
        {
            if (!QuoteNames & !RESERVED.Contains(sequenceName.ToUpper()))
            {
                sequenceName = sequenceName.ToUpper();
            }

            using (var cmd = dbCmd.CreateCommand())
            {
                cmd.CommandText = "SELECT count(*) FROM ALL_SEQUENCES WHERE UPPER(Sequence_NAME) = :sequencename";

                var param = cmd.CreateParameter();
                param.ParameterName = "sequencename";
                param.Value = sequenceName;
                cmd.Parameters.Add(param);

                return cmd.ExecuteNonQuery() > 0;
            }
        }
    }
}

/*
DEBUG: Ignoring existing generator 'CREATE GENERATOR ModelWFDT_Id_GEN;': unsuccessful metadata update
DEFINE GENERATOR failed
attempt to store duplicate value (visible to active transactions) in unique index "RDB$INDEX_11" 
*/

