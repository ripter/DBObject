using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using MySql.Data.MySqlClient;

namespace org.ZenSoftware
{

    /// <summary>
    /// Base object for ORM Objects
    /// By Chris Richards 2008
    /// </summary>
    public class DBObject : org.zensoftware.KVCObject
    {
        public string TableName
        {
            get { return (string)valueForKey("_table_name_"); }
            set { setValueForKey("_table_name_", value); }
        }
        public string IndexColumn
        {
            get { return (string)valueForKey("_index_column_"); }
            set { setValueForKey("_index_column_", value); }
        }
        public string ConnectionString
        {
            get { return (string)valueForKey("_connection_string_"); }
            set { setValueForKey("_connection_string_", value); }
        }

        /// <summary>
        /// Create a DBObject
        /// </summary>
        /// <param name="table_name">table used for this object</param>
        /// <param name="index_column">index column used in the table. Expects something like "id"</param>
        /// <param name="connection_string">connection string to use for this object</param>
        public DBObject(string table_name, string index_column, string connection_string)
        {
            this.TableName = table_name;
            this.IndexColumn = index_column;
            this.ConnectionString = connection_string;
        }

        /// <summary>
        /// Fill out our properties from the command.
        /// </summary>
        /// <param name="command"></param>
        /// <param name="connection"></param>
        public void Fill(MySqlCommand command)
        {
            if (!canCallTable()) { throw new Exception("TableName, IndexColumn, or ConnectionString are not defined."); }

            using (MySqlConnection conn = new MySqlConnection(this.ConnectionString))
            {
                //Connect the command to the connectiong
                command.Connection = conn;

                try
                {
                    //Open the connection and get the reader
                    conn.Open();
                    MySqlDataReader reader = command.ExecuteReader();

                    //I only want to get the frist record because I'm returning just one object.
                    while (reader.Read())
                    {
                        //Get all the columns
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            try {
                            setValueForKey("column_" + reader.GetName(i), reader[i]);
                            }
                            catch (MySql.Data.Types.MySqlConversionException myEx)
                            {
                                this.LogError("Attempting to set column '" + reader.GetName(i) + "'. Skipped this column and continued execution. Command was: " + command.CommandText, myEx);
                            }
                        }
                    }

                }
                catch (Exception ex)
                {
                    LogError("DBObject.Fill(" + command.CommandText + ")", ex);
                }

            }
        }
        /// <summary>
        /// This will fill all the columns from the index.
        /// Same as 'SELECT * FROM table_name WHERE table_index=?index'
        /// Similar to ByIndex() except this fills/replaces the current object instead of returning a new object.
        /// </summary>
        public void FillFromIndex(object value)
        {
            //Create the Command and fill in the values
            MySqlCommand cmd = new MySqlCommand("SELECT * FROM " + this.TableName + " WHERE " + this.IndexColumn + "=?index");
            cmd.Parameters.AddWithValue("?index", value);

            this.Fill(cmd);
        }

        /// <summary>
        /// index_column is the column to use in the WHERE clause. The object expects something like "id" 
        /// All columns to updated *must* start with "column_" in the Properties (this is done automatically if you used the object.)
        /// </summary>
        /// <param name="connection"></param>
        public void Update()
        {
            if (!canCallTable()) { throw new Exception("TableName, IndexColumn, or ConnectionString are not defined."); }
            string table = this.TableName;
            string index_column = this.IndexColumn;
            string connection = this.ConnectionString;

            if (!this.hasKeys() && null == valueForKey("column_" + index_column))
            {
                throw new Exception("No index value defined.");
                return;
            }

            //Build our Command
            MySqlCommand cmd = new MySqlCommand();
            //Build the query
            StringBuilder sb_query = new StringBuilder();
            sb_query.Append("UPDATE ");
            sb_query.Append(table);
            sb_query.Append(" SET ");

            //Add all of the properties
            foreach (string key in this.keys())
            {
                //If it's a column and it's not the id
                if (key.StartsWith("column_") && key != "column_" + index_column)
                {
                    //Cool, lets add the key
                    sb_query.Append(key.Substring(key.IndexOf("_") + 1));
                    sb_query.Append("=");
                    sb_query.Append("?" + key);
                    sb_query.Append(", ");
                    //Now add the value
                    cmd.Parameters.AddWithValue("?" + key, valueForKey(key));
                }
            }
            //Now remove the  trailing comma
            sb_query.Remove(sb_query.Length - 2, 2);

            //Add the WHERE
            sb_query.Append(" WHERE ");
            sb_query.Append(index_column);
            sb_query.Append("=?column_");
            sb_query.Append(index_column);
            cmd.Parameters.AddWithValue("?column_" + index_column, valueForKey("column_" + index_column));

            //Start the Database stuff
            using (MySqlConnection conn = new MySqlConnection(connection))
            {
                //Put the Command together
                cmd.CommandText = sb_query.ToString();
                cmd.Connection = conn;

                //Now run it!
                try
                {
                    conn.Open();
                    cmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    LogError("DBObject.Update()", ex);
                }
            }

        }
        /// <summary>
        /// Inserts all properties who's name starts with "column_" into the database.
        /// It will then set it's own index_column property with the new index value.
        /// </summary>
        /// <param name="insertIndex">If true, this will insert the index, if false it will not.</param>
        public void Insert(bool insertIndex)
        {
            if (!canCallTable()) { throw new Exception("TableName, IndexColumn, or ConnectionString are not defined."); }
            string table = this.TableName;
            string index_column = this.IndexColumn;
            string connection = this.ConnectionString;

            if (!this.hasKeys())
            {
                throw new Exception("No keys defined.");
                return;
            }

            //Build the command
            MySqlCommand cmd = new MySqlCommand();

            //Build the Query
            StringBuilder sb_query = new StringBuilder();   //First part of the query
            sb_query.Append("INSERT INTO ");
            sb_query.Append(table);
            StringBuilder sb_values = new StringBuilder(); //Second part of the query
            sb_values.Append("VALUES ");

            //Add the Columns to insert and their values
            sb_query.Append("(");
            sb_values.Append("(");
            foreach (string key in keys())
            {
                //If it's a column and it's not the id
                if (key.StartsWith("column_") )
                {
                    //If it's not the index column always insert it, if it is check to see if we should insert it.
                    if (key != "column_" + index_column || (key == "column_" + index_column && true == insertIndex))
                    {
                        //Add the Column name
                        sb_query.Append(key.Substring(key.IndexOf("_") + 1));
                        sb_query.Append(",");

                        //Add the Value
                        sb_values.Append("?");
                        sb_values.Append(key);
                        sb_values.Append(",");
                        //Add the value to the command
                        cmd.Parameters.AddWithValue("?" + key, valueForKey(key));
                    }
                }
            }
            //Now remove the  trailing comma
            sb_query.Remove(sb_query.Length - 1, 1);
            sb_query.Append(")");
            sb_values.Remove(sb_values.Length - 1, 1);
            sb_values.Append(")");

            //Start the Database stuff
            using (MySqlConnection conn = new MySqlConnection(connection))
            {
                //Put the Command together
                cmd.CommandText = sb_query.ToString() + " " + sb_values.ToString();
                cmd.Connection = conn;

                //Now run it!
                try
                {
                    conn.Open();
                    cmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    LogError("DBObject.Insert()", ex);
                }
            }

            //Now lets get the ID back
            // TODO: This could return the wrong id. We need to verify that the columns are the ones we just inserted.
            MySqlCommand cmdID = new MySqlCommand("SELECT " + index_column + " FROM " + table + " ORDER BY "+ index_column +" DESC LIMIT 1");
            this.Fill(cmdID);

        }
        /// <summary>
        /// Inserts all properties who's name starts with "column_" into the database.
        /// It will then set it's own index_column property with the new index value.
        /// This will not insert the index column.
        /// </summary>
        public void Insert()
        {
            this.Insert(false);
        }

        /// <summary>
        /// Deletes the record from the database. (Matches on the index column.)
        /// </summary>
        public void Delete()
        {
            //Call the delete with the index
            this.Delete(this.IndexColumn);
        }
        /// <summary>
        /// Deletes this Record based on the key passed.
        /// </summary>
        /// <param name="key">Needs to be a key that exists in the database. Like "id" that matches "column_id"</param>
        public void Delete(string key)
        {
            if (!canCallTable()) { throw new Exception("TableName, IndexColumn, or ConnectionString are not defined."); }

            MySqlCommand cmd = new MySqlCommand();
            StringBuilder query = new StringBuilder();

            query.Append("DELETE FROM ");
            query.Append(this.TableName);
            query.Append(" WHERE ");
            query.Append(key);
            query.Append("=?column_");
            query.Append(key);
            cmd.Parameters.AddWithValue("?column_" + key, valueForKey("column_" + key));

            //Start the Database stuff
            using (MySqlConnection conn = new MySqlConnection(this.ConnectionString))
            {
                cmd.CommandText = query.ToString();
                cmd.Connection = conn;

                try
                {
                    conn.Open();
                    cmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    LogError("DBObject.Delete()", ex);
                }
            }
        }

        /// <summary>
        /// Do all the defined columns exist as a single record in the database.
        /// You can use this to know if you should update or insert.
        /// </summary>
        /// <returns>True if the record already exits, false if it doesn't.</returns>
        public bool DoesExist()
        {
            if (!canCallTable()) { throw new Exception("TableName, IndexColumn, or ConnectionString are not defined."); }
            string table = this.TableName;
            string index_column = this.IndexColumn;
            string connection = this.ConnectionString;

            //If there are no properties then it can't be exist
            if (!hasKeys())
            {
                throw new Exception("No keys defined.");
                return false;
            }

            //Our Command
            MySqlCommand cmd = new MySqlCommand();
            //Build the Query
            StringBuilder sb_query = new StringBuilder();
            sb_query.Append("SELECT COUNT(*) FROM ");
            sb_query.Append(table);
            sb_query.Append(" WHERE ");

            //Check that all the columns match (ignore the index_column if it exists)
            foreach (string key in keys())
            {
                //If it's a column and it's not the index
                if (key.StartsWith("column_") && key != "column_" + index_column)
                {
                    //Add the Key
                    sb_query.Append(key.Substring(key.IndexOf("_") + 1));

                    //Check for null entries
                    if (DBNull.Value != valueForKey(key))
                    {
                        //Cool, normal check
                        sb_query.Append("=");
                        sb_query.Append("?" + key);
                        cmd.Parameters.AddWithValue("?" + key, valueForKey(key));
                    }
                    else
                    {
                        //Null is special
                        sb_query.Append(" IS NULL ");
                    }
                    sb_query.Append(" AND ");
                    //Now add the value

                }
            }
            //Now remove the  trailing comma
            sb_query.Remove(sb_query.Length - 5, 5);

            //Start the Database stuff
            using (MySqlConnection conn = new MySqlConnection(connection))
            {
                //Put the Command together
                cmd.CommandText = sb_query.ToString();
                cmd.Connection = conn;

                //Now run it!
                try
                {
                    conn.Open();
                    int count = Convert.ToInt32(cmd.ExecuteScalar());
                    if (count > 0)
                    {
                        //it exists
                        return true;
                    }
                }
                catch (Exception ex)
                {
                    LogError("DBObject.DoesExist()", ex);
                }
            }

            //Cool, it passes
            return false;
        }
        /// <summary>
        /// If the record exists {doesExist()} then this will set the object's index column to the index of the existing record.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="index_column"></param>
        /// <param name="connection"></param>
        public void GetExistingIndex()
        {
            if (!canCallTable()) { throw new Exception("TableName, IndexColumn, or ConnectionString are not defined."); }
            string table = this.TableName;
            string index_column = this.IndexColumn;
            string connection = this.ConnectionString;

            //If there are no properties then there is nothing to do
            if (!hasKeys())
            {
                throw new Exception("No keys defined.");
                return;
            }
            //Our Command
            MySqlCommand cmd = new MySqlCommand();
            //Build the Query
            StringBuilder sb_query = new StringBuilder();
            sb_query.Append("SELECT ");
            sb_query.Append(index_column);
            sb_query.Append(" FROM ");
            sb_query.Append(table);
            sb_query.Append(" WHERE ");

            //Check that all the columns match (ignore the index_column if it exists)
            foreach (string key in keys())
            {
                //If it's a column and it's not the index
                if (key.StartsWith("column_") && key != "column_" + index_column)
                {
                    //Add the Key
                    sb_query.Append(key.Substring(key.IndexOf("_") + 1));

                    //Check for null entries
                    if (DBNull.Value != valueForKey(key))
                    {
                        //Cool, normal check
                        sb_query.Append("=");
                        sb_query.Append("?" + key);
                        cmd.Parameters.AddWithValue("?" + key, valueForKey(key));
                    }
                    else
                    {
                        //Null is special
                        sb_query.Append(" IS NULL ");
                    }
                    sb_query.Append(" AND ");
                }
            }
            //Now remove the  trailing AND
            sb_query.Remove(sb_query.Length - 5, 5);
            //Make sure we only get one
            sb_query.Append(" LIMIT 1");

            //Start the Database stuff
            using (MySqlConnection conn = new MySqlConnection(connection))
            {
                //Put the Command together
                cmd.CommandText = sb_query.ToString();
                cmd.Connection = conn;

                //Now run it!
                try
                {
                    conn.Open();
                    setValueForKey("column_" + index_column, cmd.ExecuteScalar());
                }
                catch (Exception ex)
                {
                    LogError("DBObject.GetExistingIndex()", ex);
                }
            }
        }

        /// <summary>
        /// Get just a single object by a select command.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="command"></param>
        /// <param name="connection"></param>
        /// <returns></returns>
        static protected T SingleBySelect<T>(MySqlCommand command) where T : DBObject
        {
            //Use the list, just return a single item though
            List<T> list = DBObject.BySelect<T>(command);
            if (list.Count > 0)
            {
                return list[0];
            }
            else
            {
                return default(T);
            }
        }
        /// <summary>
        /// Get a list of T Objects from a query.
        /// The objects will have properties for every column specified by the query. 
        /// The Property names will be "column_" + column_name.
        /// </summary>
        /// <typeparam name="T">Must Inherit from DBObject</typeparam>
        /// <param name="command"></param>
        /// <returns></returns>
        static public List<T> BySelect<T>(MySqlCommand command) where T : DBObject
        {
            //Create a dummy T so we can use it's values
            DBObject dummy = (DBObject)System.Activator.CreateInstance<T>();

            //Create our list of T
            List<T> list = new List<T>();
            using (MySqlConnection conn = new MySqlConnection(dummy.ConnectionString))
            {
                //Connect the command to the connectiong
                command.Connection = conn;

                try
                {
                    //Open the connection and get the reader
                    conn.Open();
                    MySqlDataReader reader = command.ExecuteReader();

                    //I only want to get the frist record because I'm returning just one object.
                    while (reader.Read())
                    {
                        T obj = System.Activator.CreateInstance<T>();
                        //Get all the columns
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            try
                            {
                                //Get the name and the value
                                object[] parms = { "column_" + reader.GetName(i), reader[i] };
                                //Get the SetProperty Method and Call it
                                MethodInfo method = obj.GetType().GetMethod("setValueForKey");
                                method.Invoke(obj, parms);
                            }
                            catch (MySql.Data.Types.MySqlConversionException myEx)
                            {
                                dummy.LogError("Attempting to set column '" + reader.GetName(i) + "'. Skipped this column and continued execution. Command was: " + command.CommandText, myEx);
                            }
                        }
                        //Add it to the list
                        list.Add(obj);
                    }

                }
                catch (Exception ex)
                {
                    StringBuilder error = new StringBuilder();
                    error.Append("DBObject.BySelect<");
                    error.Append(typeof(T));
                    error.Append(">('");
                    error.Append(command.CommandText);
                    error.Append("')");
                    error.Append(" - Paramerters: ");
                    foreach (MySqlParameter parm in command.Parameters)
                    {
                        error.Append(" [ ");
                        error.Append(parm.ParameterName);
                        error.Append(" = ");
                        error.Append(parm.Value);
                        error.Append(" ] ");
                    }
                    dummy.LogError(error.ToString(), ex);
                }

            }
            return list;
        }

        /// <summary>
        /// Retreave the object just on the where clause. 
        /// It will assume 'SELECT * FROM table_name WHERE'
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="where_statement">The where statement without the word 'WHERE'. Example: 'id=?id AND active=true'</param>
        /// <param name="parameters">The values for the where statement</param>
        /// <returns></returns>
        static protected List<T> Where<T>(string where_statement, Dictionary<string, object> parameters) where T : DBObject
        {
            //Create a dummy T so we can use it's values
            DBObject dummy = (DBObject)System.Activator.CreateInstance<T>();
            //Create the Command and fill in the values
            MySqlCommand cmd = new MySqlCommand("SELECT * FROM " + dummy.TableName + " WHERE " + where_statement);
            foreach (KeyValuePair<string, object> kvp in parameters)
            {
                cmd.Parameters.AddWithValue(kvp.Key, kvp.Value);
            }
            //Return it
            return DBObject.BySelect<T>(cmd);
        }
        /// <summary>
        /// Retreave the object from a single where item.
        /// This assumes 'SELECT * FROM table_name'
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="column">Column must be something like 'id' or 'name' that matches the name of a real column in the database.</param>
        /// <param name="value">Value that the column must match.</param>
        /// <returns></returns>
        static protected List<T> Where<T>(string column, object value) where T : DBObject
        {
            //Create a dummy T so we can use it's values
            DBObject dummy = (DBObject)System.Activator.CreateInstance<T>();
            //Create the Command and fill in the values
            MySqlCommand cmd = new MySqlCommand("SELECT * FROM " + dummy.TableName + " WHERE " + column + "=?item");
            cmd.Parameters.AddWithValue("?item", value);

            //Return it
            return DBObject.BySelect<T>(cmd);
        }    
        /// <summary>
        /// Retuns an object by the Indexer.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value">Indexer Value</param>
        /// <returns></returns>
        static protected T ByIndex<T>(object value) where T : DBObject
        {
            //Create a dummy T so we can use it's values
            DBObject dummy = (DBObject)System.Activator.CreateInstance<T>();
            //Create the Command and fill in the values
            MySqlCommand cmd = new MySqlCommand("SELECT * FROM " + dummy.TableName + " WHERE " + dummy.IndexColumn + "=?index");
            cmd.Parameters.AddWithValue("?index", value);
            //return it
            return DBObject.SingleBySelect<T>(cmd);
        }

        /// <summary>
        /// Returns true if table, index, and connection string are defined.
        /// </summary>
        /// <returns></returns>
        protected bool canCallTable()
        {
            if (string.IsNullOrEmpty(this.TableName) || string.IsNullOrEmpty(this.IndexColumn) || string.IsNullOrEmpty(this.ConnectionString))
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// Log an Error. Override this for your own Error Logging
        /// </summary>
        /// <param name="location"></param>
        /// <param name="ex"></param>
        protected void LogError(string location, Exception ex)
        {
            //Just write it to the Console
            Console.WriteLine("\n\nError:" + location + "\n" + ex.Message);
        }
    }
}
