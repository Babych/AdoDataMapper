using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Practices.EnterpriseLibrary.Data;

namespace DAL
{
    public class theDataMapper : IDataMapper
    {
        #region Private Properties

        private static readonly Dictionary<Type, MappingSchema> SetTypes = new Dictionary<Type, MappingSchema>();
        private readonly Database theDatabase;
        private DbConnection connection;

        #endregion

        #region Private Methods

        private DbConnection GetConnection()
        {
            if (connection != null && string.IsNullOrEmpty(connection.ConnectionString) == false)
            {
                if (connection.State == ConnectionState.Closed || connection.State == ConnectionState.Broken)
                {
                    // reopen closed/broken connection
                    try
                    {
                        connection.Open();
                    }
                    catch (InvalidOperationException ex)
                    {
                        Logger.Error(ex);
                    }
                    catch (SqlException ex)
                    {
                        Logger.Error(ex);
                    }
                    catch (ConfigurationErrorsException ex)
                    {
                        Logger.Error(ex);
                    }
                }

                return connection;
            }

            try
            {
                // create new connection.
                connection = theDatabase.CreateConnection();
                connection.Open();
            }
            catch (InvalidOperationException ex)
            {
                Logger.Error(ex);
            }
            catch (SqlException ex)
            {
                Logger.Error(ex);
            }
            catch (ConfigurationErrorsException ex)
            {
                Logger.Error(ex);
            }

            return connection;
        }

        private static MappingSchema GetTypesModel(Type type)
        {
            if (SetTypes.ContainsKey(type))
            {
                return SetTypes[type];
            }

            MappingSchema res = new MappingSchema();
            SetTypes[type] = res;
            PropertyInfo[] props = type.GetProperties();
            foreach (PropertyInfo propertyInfo in props)
            {
                object[] attributes = propertyInfo.GetCustomAttributes(typeof(MappingFieldAttribute), true);
                if (attributes.Length == 0)
                {
                    continue;
                }
                MappingFieldAttribute mappingFieldAttribute = attributes[0] as MappingFieldAttribute;
                if (mappingFieldAttribute == null)
                {
                    continue;
                }
                res.Mapping[(string.IsNullOrEmpty(mappingFieldAttribute.Name) ?
                    propertyInfo.Name : mappingFieldAttribute.Name).ToUpperInvariant()] = propertyInfo;
            }
            return res;
        }

        private static object CreateElement(IDataRecord reader, Type type, Dictionary<int, PropertyInfo> columns)
        {
            object t = Activator.CreateInstance(type);
            foreach (KeyValuePair<int, PropertyInfo> pair in columns)
            {
                object r = reader.GetValue(pair.Key);
                try
                {
                    pair.Value.SetValue(t, GetValue(r, pair.Value.PropertyType), null);
                }
                catch (Exception ex)
                {
                    throw new DataException(type + "." + pair.Value.Name + " property value cannot be set", ex);
                }

            }
            if (t is IDynamicPropertiesDict)
            {
                try
                {
                    IDynamicPropertiesDict row = t as IDynamicPropertiesDict;
                    for (int i = 0; i < reader.FieldCount; i++)
                        row.RowData.Add(reader.GetName(i), reader.GetValue(i));
                }
                catch (Exception ex)
                {
                    throw new DataException(type + "." + t.GetType() + " value cannot be set", ex);
                }
            
            }
            return t;
        }

        private static object GetValue(object r, Type type)
        {
            object result;
            if (r == DBNull.Value) r = null;
            type = (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                       ? Nullable.GetUnderlyingType(type)
                       : type;

            if (r == null) result = null;
            else if (type == typeof(bool)) result = Convert.ToBoolean(r, CultureInfo.InvariantCulture);
            else if (type == typeof(decimal)) result = Convert.ToDecimal(r, CultureInfo.InvariantCulture);
            else if (type == typeof(long)) result = Convert.ToInt64(r, CultureInfo.InvariantCulture);
            else if (type == typeof(int)) result = Convert.ToInt32(r, CultureInfo.InvariantCulture);
            else if (type == typeof(double)) result = Convert.ToDouble(r, CultureInfo.InvariantCulture);
            else if (type == typeof(DateTime)) result = Convert.ToDateTime(r, CultureInfo.InvariantCulture);
            else if (type.IsEnum) result = Enum.Parse(type, r.ToString());
            else result = r;

            return result;
        }

        private void PopulateCommandParameters(DbCommand command, Dictionary<string, object> parameters)
        {
            if (command.CommandType == CommandType.StoredProcedure)
            {
                theDatabase.DiscoverParameters(command);
                
                var removingParameters = command.Parameters.Cast<DbParameter>().Where(param => param.ParameterName.Equals("@Count", StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
                if (removingParameters != null)
                {
                    command.Parameters.RemoveAt(removingParameters.ParameterName);
                }

                removingParameters = command.Parameters.Cast<DbParameter>().Where(param => param.ParameterName.Equals("@RETURN_VALUE", StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
                if (removingParameters != null)
                {
                    command.Parameters.RemoveAt(removingParameters.ParameterName);
                }

                if (command.Parameters.Count > 0 &&
                    parameters != null && 
                    parameters.Count != 0)
                {
                    foreach (DbParameter item in command.Parameters)
                    {
                        item.Value = parameters.Where(t => t.Key.Equals(item.ParameterName, StringComparison.OrdinalIgnoreCase)).FirstOrDefault().Value ?? DBNull.Value;
                    }
                }
            }
        }

        private static Dictionary<int, PropertyInfo> GetColumns(IDataReader reader, Type modelType)
        {
            var colNames = new Dictionary<int, PropertyInfo>();

            MappingSchema mapping = GetTypesModel(modelType);
            Dictionary<string, PropertyInfo> columns = mapping.Mapping;
            for (int i = 0; i < reader.FieldCount; i++)
            {
                string name = reader.GetName(i).ToUpperInvariant();
                if (columns.ContainsKey(name))
                {
                    colNames[i] = columns[name];
                }
            }
            return colNames;
        }

        private DbCommand GetSqlStringCommand(string query, Dictionary<string, object> parameters = null)
        {
            var command = theDatabase.GetSqlStringCommand(query);

            command.Connection = GetConnection();

            PopulateCommandParameters(command, parameters);

            return command;
        }

        private DbCommand GetStoredProcCommand(string storedProcedureName, Dictionary<string, object> parameters = null)
        {
            var command = theDatabase.GetStoredProcCommand(storedProcedureName);

            command.Connection = GetConnection();

            PopulateCommandParameters(command, parameters);

            return command;
        }

        private void ExecuteDataReader(DbCommand command, string sql, Dictionary<string, object> parameters, params IList[] res)
        {
            using (IDataReader reader = command.ExecuteReader())
            {
                int pos = 0;
                do
                {
                    if (res.Length <= pos) break;
                    IList list = res[pos];
                    if (list != null)
                    {
                        Type modelType = list.GetType().GetProperty("Item").PropertyType;
                        Dictionary<int, PropertyInfo> colNames = GetColumns(reader, modelType);

                        while (reader.Read())
                        {
                            object data = CreateElement(reader, modelType, colNames);
                            list.Add(data);
                        }
                    }
                    pos++;
                } while (reader.NextResult());
            }
        }

        private void ExecuteDataReader(DbCommand command, string sql, Dictionary<string, object> parameters, out int count, params IList[] res)
        {
            var countParameter = command.CreateParameter();
            countParameter.DbType = DbType.Int32;
            countParameter.Size = 4;
            countParameter.Direction = ParameterDirection.Output;
            countParameter.ParameterName = "@Count";
            command.Parameters.Add(countParameter);

            using (IDataReader reader = command.ExecuteReader())
            {
                int pos = 0;
                do
                {
                    if (res.Length <= pos) break;
                    IList list = res[pos];
                    if (list != null)
                    {
                        Type modelType = list.GetType().GetProperty("Item").PropertyType;
                        Dictionary<int, PropertyInfo> colNames = GetColumns(reader, modelType);

                        while (reader.Read())
                        {
                            object data = CreateElement(reader, modelType, colNames);
                            list.Add(data);
                        }
                    }
                    pos++;
                } while (reader.NextResult());
            }
            count = (countParameter.Value != null) ? (int)countParameter.Value : 0;
        }

        #endregion

        #region Public Methods

        public theDataMapper(Database theDatabase)
        {
            this.theDatabase = theDatabase;
        }

        public object ExecuteScalar(string sql, Dictionary<string, object> parameters = null)
        {
            using (DbCommand command = GetStoredProcCommand(sql, parameters))
            {
                try
                {
                    return command.ExecuteScalar();
                }
                finally
                {
                    if (command.Connection.State != ConnectionState.Closed)
                        command.Connection.Close();
                }

            }
        }

        public void ExecuteNotQuery(string sql, Dictionary<string, object> parameters = null)
        {
            using (DbCommand command = GetStoredProcCommand(sql, parameters))
            {
                try
                {
                    command.ExecuteNonQuery();
                }
                finally
                {
                    if (command.Connection.State != ConnectionState.Closed)
                        command.Connection.Close();

                }
            }
        }

        public void ExecuteSqlQuery(string sql, Dictionary<string, object> parameters, params IList[] res)
        {
            using (DbCommand command = GetSqlStringCommand(sql, parameters))
            {
                try
                {
                    ExecuteDataReader(command, sql, parameters, res);
                }
                finally
                {
                    if (command.Connection.State != ConnectionState.Closed)
                        command.Connection.Close();
                }
            }

        }

        public void ExecuteStoredProc(string sql, Dictionary<string, object> parameters, params IList[] res)
        {
            using (DbCommand command = GetStoredProcCommand(sql, parameters))
            {

                try
                {
                    ExecuteDataReader(command, sql, parameters, res);
                }
                finally
                {
                    if (command.Connection.State != ConnectionState.Closed)
                        command.Connection.Close();
                }
            }
        }

        public void ExecuteStoredProc(string sql, Dictionary<string, object> parameters, out int count, params IList[] res)
        {
            using (DbCommand command = GetStoredProcCommand(sql, parameters))
            {
                try
                {
                    ExecuteDataReader(command, sql, parameters, out count, res);
                }
                finally
                {
                    if (command.Connection.State != ConnectionState.Closed)
                        command.Connection.Close();
                }
            }
        }

        #endregion
    }
}
