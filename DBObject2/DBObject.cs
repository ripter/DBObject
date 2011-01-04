using System;
using System.Collections.Generic;
using MySql.Data.MySqlClient;

namespace DBObject2
{
    /// <summary>
    /// DBObject acts an object layer to make working with the database easer and less error prone.
    /// </summary>
    public class DBObject
    {
        private string _connection_string;
        private int _total_rows;
        private List<string> _columns;
        private List<string> _unique_columns;
        private string _table_name;
        private string _primary_key;
        private List<DBRow> _rows;

        /// <summary>
        /// Connection String for Querys
        /// </summary>
        public string ConnectionString
        {
            get
            {
                if (string.IsNullOrEmpty(_connection_string))
                {
                    throw new NoConnectionStringException();
                }
                return _connection_string;
            }
            set { _connection_string = value; }
        }
        /// <summary>
        /// The Table use for Querys
        /// </summary>
        public string TableName
        {
            get
            {
                if (string.IsNullOrEmpty(_table_name))
                {
                    throw new NoTableException();
                }
                return _table_name;
            }
            set { _table_name = value; }
        }
        /// <summary>
        /// The PrimaryKey used for Updates and Queries
        /// </summary>
        public string PrimaryKey
        {
            get
            {
                if (string.IsNullOrEmpty(_primary_key))
                {
                    throw new NoPrimaryKeyException();
                }
                return _primary_key;
            }
            set { _primary_key = value; }
        }
        /// <summary>
        /// Columns in the Table
        /// </summary>
        public List<string> Columns
        {
            get
            {
                return _columns;
            }
            set
            {
                _columns = value;
            }
        }
        /// <summary>
        /// The Rows returned by the last request.
        /// </summary>
        public List<DBRow> Rows
        {
            get { return _rows; }
            set { _rows = value; }
        }
        /// <summary>
        /// Returns the Total number of Rows in the Database.
        /// </summary>
        /// <returns></returns>
        public int TotalRowCount
        {
            get
            {
                if (-1 == _total_rows)
                {

                    _total_rows = Convert.ToInt32(DBObject.Scalar(this._connection_string, "SELECT COUNT(*) AS count FROM " + this.TableName));

                }
                return _total_rows;
            }
        }

        /// <summary>
        /// Default Constructor
        /// </summary>
        public DBObject()
        {
            //Initalize the varables
            _columns = new List<string>();
            _rows = new List<DBRow>();
            _total_rows = -1;
        }
        /// <summary>
        /// Creates an Empty DBObject with a connection string.
        /// </summary>
        /// <param name="connection_string"></param>
        public DBObject(string connection_string)
            : this()
        {
            _connection_string = connection_string;
        }
        /// <summary>
        /// Creates an Empty DBObject with a connection string and table
        /// </summary>
        /// <param name="connection_string"></param>
        /// <param name="table_name"></param>
        public DBObject(string connection_string, string table_name, string primary_key)
            : this()
        {
            _connection_string = connection_string;
            _table_name = table_name;
            _primary_key = primary_key;

            //Lets get the columns from the table
            MySqlConnection conn = new MySqlConnection(connection_string);
            try
            {
                //Create the Command
                MySqlCommand cmd = new MySqlCommand("SHOW COLUMNS FROM " + table_name, conn);
                //Open the Connection
                conn.Open();
                //Get the reader
                MySqlDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    //Add the Column Name
                    _columns.Add((string)reader["Field"]);
                }
            }
            catch (Exception ex)
            {
                DBObject.OnError("Getting Columns for " + table_name, ex);
            }
            finally
            {
                //Always close the Connection
                conn.Close();
            }
        }


        /// <summary>
        /// Fills the object from the Select statement.
        /// Fills in the columns and rows.
        /// This Adds the rows to any existing rows.
        /// </summary>
        /// <param name="select_query"></param>
        /// <param name="query_parameters"></param>
        public void FillFromSelect(string select_query, params object[] query_parameters)
        {
            //This method requires the connection string be defined.
            if (string.IsNullOrEmpty(_connection_string)) { throw new NoConnectionStringException(); }

            //Create the MySqlCommand
            MySqlCommand cmd = new MySqlCommand(select_query);
            for (int i = 0; i < query_parameters.Length; i++)
            {
                cmd.Parameters.AddWithValue("@" + i, query_parameters[i]);
            }

            //Open the connection and run it
            MySqlConnection conn = new MySqlConnection(this._connection_string);
            try
            {
                //Tell the query to use this connecton
                cmd.Connection = conn;

                //Open the Connection
                conn.Open();

                MySqlDataReader reader = cmd.ExecuteReader();

                //Get the Column info
                if (0 == this.Columns.Count)
                {
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        this.Columns.Add(reader.GetName(i));
                    }
                }

                //Get the Rows
                while (reader.Read())
                {
                    Dictionary<string, object> row = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        //We want to convert DBNulls to just normal null
                        if (reader[i].GetType() == typeof(DBNull))
                        {
                            row.Add(reader.GetName(i), null);
                        }
                        else
                        {
                            row.Add(reader.GetName(i), reader[i]);
                        }
                    }

                    this.Rows.Add(new DBRow(row));
                }
                reader.Close();


            }
            catch (Exception ex)
            {
                DBObject.OnError("FillFromSelect( " + select_query + ")", ex);
            }
            finally
            {
                //Always close the Connection
                conn.Close();
            }
        }

        /// <summary>
        /// Gets just the rows that meet the MySQL where caluse.
        /// After this method, the Rows property will only contain Rows that match the where caluse.
        /// </summary>
        /// <param name="where_caluse">Examle: "first_name=@0 AND last_name=@1"</param>
        /// <param name="query_parameter"></param>
        public void Where(string where_caluse, params object[] query_parameters)
        {
            string query = "SELECT * FROM " + this.TableName + " WHERE " + where_caluse;
            //Clear out any Rows that might already exist
            this.Rows.Clear();
            //Now get all of the new rows
            this.FillFromSelect(query, query_parameters);
        }

        /// <summary>
        /// Updates any Rows that have changed.
        /// </summary>
        public void Update()
        {
            //This method requires the connection string be defined.
            if (string.IsNullOrEmpty(_connection_string)) { throw new NoConnectionStringException(); }
            //Requires a table
            if (string.IsNullOrEmpty(_table_name)) { throw new NoTableException(); }
            //Requires a primary key
            if (string.IsNullOrEmpty(_primary_key)) { throw new NoPrimaryKeyException(); }

            //TODO: Add threading
            //Update all of our rows.
            for (int i = 0; i < this.Rows.Count; i++)
            {
                this.Rows[i].Update(this);
            }

        }

        /// <summary>
        /// Override this to implent error handling
        /// </summary>
        /// <param name="location"></param>
        /// <param name="ex"></param>
        static public void OnError(string location, Exception ex)
        {
            //Format it
            string errorMsg = "Error occurred at: '" + location + "'" +
                "\nSource: '" + ex.Source + "'" +
                "\nMessage: '" + ex.Message + "'" +
                "\n\nStack Trace: " + ex.StackTrace;
            if (null != ex.InnerException)
            {
                errorMsg += "\n\nInner Exception: " + ex.InnerException.Message;
            }

            throw new Exception(errorMsg, ex);
        }

        /// <summary>
        /// Filles the DBObject from the select query provided.
        /// </summary>
        /// <param name="select_query">Example: "SELECT * FROM user WHERE id=@0" </param>
        /// <param name="query_parameters">Numbered parameters for the select query. Example: "1"</param>
        /// <param name="connection_string">Connection string to use</param>
        /// <returns></returns>
        static public DBObject BySelect(string connection_string, string select_query, params object[] query_parameters)
        {
            DBObject obj = new DBObject(connection_string);
            obj.FillFromSelect(select_query, query_parameters);
            return obj;
        }

        /// <summary>
        /// Returns a single value from the query.
        /// </summary>
        /// <param name="connection_string">Connection string to use</param>
        /// <param name="select_query">Example: "SELECT email FROM user WHERE id=@0"</param>
        /// <param name="query_parameters">Numbered parameters for the select query. Example: "1"</param>
        /// <returns></returns>
        static public object Scalar(string connection_string, string select_query, params object[] query_parameters)
        {
            //This method requires the connection string be defined.
            if (string.IsNullOrEmpty(connection_string)) { throw new NoConnectionStringException(); }

            //Create the MySqlCommand
            MySqlCommand cmd = new MySqlCommand(select_query);
            for (int i = 0; i < query_parameters.Length; i++)
            {
                cmd.Parameters.AddWithValue("@" + i, query_parameters[i]);
            }

            object scalar = null;

            //Open the connection and run it
            MySqlConnection conn = new MySqlConnection(connection_string);
            try
            {
                //Tell the query to use this connecton
                cmd.Connection = conn;

                //Open the Connection
                conn.Open();

                scalar = cmd.ExecuteScalar();
            }
            catch (Exception ex)
            {
                DBObject.OnError("Scalar( " + select_query + ")", ex);
            }
            finally
            {
                //Always close the Connection
                conn.Close();
            }

            return scalar;
        }
        /// <summary>
        /// Runs the Query but doens't return anything.
        /// </summary>
        /// <param name="connection_string">Connection string to use</param>
        /// <param name="query">Example: "UPDATE email SET name=@0 WHERE id=@1"</param>
        /// <param name="query_parameters">Numbered parameters for the select query. Example: "Chris", "1"</param>
        static public void NonQuery(string connection_string, string query, params object[] query_parameters)
        {
            NonQuery(connection_string, query, 3, query_parameters);
        }
        /// <summary>
        /// Runs the Query but doens't return anything.
        /// </summary>
        /// <param name="connection_string">Connection string to use</param>
        /// <param name="query">Example: "UPDATE email SET name=@0 WHERE id=@1"</param>
        /// <param name="attempts">Number of times to try this query again on a MySql Error</param>
        /// <param name="query_parameters">Numbered parameters for the select query. Example: "Chris", "1"<</param>
        static protected void NonQuery(string connection_string, string query, int attempts, params object[] query_parameters)
        {
            //This method requires the connection string be defined.
            if (string.IsNullOrEmpty(connection_string)) { throw new NoConnectionStringException(); }

            //Create the MySqlCommand
            MySqlCommand cmd = new MySqlCommand(query);
            for (int i = 0; i < query_parameters.Length; i++)
            {
                cmd.Parameters.AddWithValue("@" + i, query_parameters[i]);
            }

            //Open the connection and run it
            MySqlConnection conn = new MySqlConnection(connection_string);
            try
            {
                //Tell the query to use this connecton
                cmd.Connection = conn;

                //Open the Connection
                conn.Open();

                cmd.ExecuteNonQuery();
            }
            catch (MySqlException mEx)
            {
                if (attempts > -1)
                {
                    //Try it again
                    NonQuery(connection_string, query, attempts - 1, query_parameters);
                }
                else
                {
                    //It's not happening
                    DBObject.OnError("MySql Exception - " + conn, mEx);
                }
            }
            catch (Exception ex)
            {
                DBObject.OnError("NonQuery( " + query + ")", ex);
            }
            finally
            {
                //Always close the Connection
                conn.Close();
            }
        }
    }
}