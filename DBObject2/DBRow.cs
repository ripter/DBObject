using System;
using System.Collections.Generic;
using MySql.Data.MySqlClient;

namespace DBObject2
{
    /// <summary>
    /// Represents a Row in the Database.
    /// </summary>
    public class DBRow
    {
        //Holds all of the data in the row.
        protected Dictionary<string, object> _data;
        //True if the Row is dirty.
        protected bool _dirty;
        //Number of times we will retry a command.
        protected int retry = 3;

        /// <summary>
        /// Indexer, returns the Column Key
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public object this[string key]
        {
            get
            {
                if (_data.ContainsKey(key))
                {
                    return _data[key];
                }
                return null;
            }
            set
            {
                if (!_data.ContainsKey(key))
                {
                    _data.Add(key, value);
                    _dirty = true;  //Adding new Value always Dirty.
                }
                else
                {
                    //Only if we are actually changing the value
                    if (_data[key] != value)
                    {
                        _data[key] = value;
                        _dirty = true;
                    }
                }
            }
        }

        /// <summary>
        /// Returns a list of the current keys.
        /// </summary>
        public List<string> Keys
        {
            get
            {
                List<string> list = new List<string>();
                foreach (string key in _data.Keys)
                {
                    list.Add(key);
                }
                return list;
            }
        }

        /// <summary>
        /// Default Constructor.
        /// </summary>
        public DBRow()
        {
            _data = new Dictionary<string, object>();
            _dirty = false;
        }
        /// <summary>
        /// Create the DBRow with values
        /// </summary>
        /// <param name="data"></param>
        public DBRow(Dictionary<string, object> data)
        {
            _data = data;
            _dirty = false;
        }

        /// <summary>
        /// Updates the Database to match the Row if it's changed.
        /// </summary>
        /// <param name="db">The DBObject this row belongs to.</param>
        public void Update(DBObject db)
        {
            Update(db, retry);
        }
        /// <summary>
        /// Updates the Database to match the Row if it's changed.
        /// </summary>
        /// <param name="db">The DBObject this row belongs to.</param>
        /// <param name="attempts">The number of times to keep trying this method incase of error.</param>
        protected void Update(DBObject db, int attempts)
        {
            if (_dirty)
            {
                //Create the MySqlCommand
                MySqlCommand cmd = new MySqlCommand("UPDATE " + db.TableName + " SET ");
                for (int i = 0; i < db.Columns.Count; i++)
                {
                    //Update everything but the primary key, make sure it's a column we know about.
                    if (db.Columns[i] != db.PrimaryKey && _data.ContainsKey(db.Columns[i]))
                    {
                        //Set the command text
                        cmd.CommandText += " " + db.Columns[i] + " = @" + i;
                        //Set the Value
                        cmd.Parameters.AddWithValue("@" + i, _data[db.Columns[i]]);
                        //Add a comma
                        cmd.CommandText += ",";
                    }
                }
                //Remove the last commna
                cmd.CommandText = cmd.CommandText.Remove(cmd.CommandText.Length - 1, 1);

                //Set the VERY importent Where
                cmd.CommandText += " WHERE " + db.PrimaryKey + " = @" + db.Columns.Count;
                cmd.Parameters.AddWithValue("@" + db.Columns.Count, _data[db.PrimaryKey]);

                //Open the connection and run it
                MySqlConnection conn = new MySqlConnection(db.ConnectionString);
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
                        Update(db, attempts - 1);
                    }
                    else
                    {
                        //It's not happening
                        DBObject.OnError("MySql Exception - " + conn, mEx);
                    }
                }
                catch (Exception ex)
                {
                    DBObject.OnError("Update( " + cmd.CommandText + ")", ex);
                }
                finally
                {
                    //Always close the Connection
                    conn.Close();
                }

            }
        }
        /// <summary>
        /// Inserts the Row into the Database.
        /// </summary>
        /// <param name="db">The DBObject to insert the row into</param>
        public void Insert(DBObject db)
        {
            this.Insert(db, retry);
        }
        /// <summary>
        /// Inserts the Row into the Database.
        /// </summary>
        /// <param name="db">The DBObject to insert the row into</param>
        /// <param name="attempts">The number of times to keep trying this method incase of error.</param>
        protected void Insert(DBObject db, int attempts)
        {
            MySqlCommand cmd = new MySqlCommand("INSERT INTO " + db.TableName + "(");
            string values = " VALUES(";
            for (int i = 0; i < db.Columns.Count; i++)
            {
                //Insert everything but the primary key.
                if (_data.ContainsKey(db.Columns[i]) && db.Columns[i] != db.PrimaryKey)
                {
                    //Set the command text
                    cmd.CommandText += db.Columns[i];
                    //Set the Value Query
                    values += "@" + i;
                    //Set the Value
                    cmd.Parameters.AddWithValue("@" + i, _data[db.Columns[i]]);

                    //Add a comma
                    cmd.CommandText += ",";
                    values += ",";
                }
            }
            //Remove the trailing commas
            cmd.CommandText = cmd.CommandText.Remove(cmd.CommandText.Length - 1, 1);
            values = values.Remove(values.Length - 1, 1);
            //Put it all together
            cmd.CommandText += ") " + values + ")";


            //Open the connection and run it
            MySqlConnection conn = new MySqlConnection(db.ConnectionString);
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
                    Insert(db, attempts - 1);
                }
                else
                {
                    //It's not happening
                    DBObject.OnError("MySql Exception - " + conn, mEx);
                }
            }
            catch (Exception ex)
            {
                DBObject.OnError("INSERT( " + cmd.CommandText + ")", ex);
            }
            finally
            {
                //Always close the Connection
                conn.Close();
            }

            //Now get the ID
            this.FindIndex(db);

            //Add ourselves to the DBObject.Rows
            db.Rows.Add(this);
        }
        /// <summary>
        /// Deletes the Row from the Database
        /// </summary>
        /// <param name="db"></param>
        public void Delete(DBObject db)
        {
            MySqlCommand cmd = new MySqlCommand("DELETE FROM " + db.TableName + " WHERE " + db.PrimaryKey + " =@0");
            cmd.Parameters.AddWithValue("@0", _data[db.PrimaryKey]);

            //Open the connection and run it
            MySqlConnection conn = new MySqlConnection(db.ConnectionString);
            try
            {
                //Tell the query to use this connecton
                cmd.Connection = conn;

                //Open the Connection
                conn.Open();

                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                DBObject.OnError("DELETE( " + cmd.CommandText + ")", ex);
            }
            finally
            {
                //Always close the Connection
                conn.Close();
            }
        }

        /// <summary>
        /// Finds the index for this record based on the columns that are not null.
        /// </summary>
        /// <param name="db">The DBObject for this row</param>
        public bool FindIndex(DBObject db)
        {
            return this.FindIndex(db, retry);
        }
        /// <summary>
        /// Finds the index from only the columns specified.
        /// </summary>
        /// <param name="db">The DBObject for this row</param>
        /// <param name="columns">The columns that must match for it to be considered.</param>
        public bool FindIndex(DBObject db, string[] columns)
        {
            return this.FindIndex(db, columns, retry);
        }
        /// <summary>
        /// Finds the index for this record based on the columns that are not null.
        /// </summary>
        /// <param name="db">The DBObject for this row</param>
        /// <param name="attempts">The number of times to keep trying this method incase of error.</param>
        protected bool FindIndex(DBObject db, int attempts)
        {
            //Make sure we don't already know the Primary Key
            if (null != this[db.PrimaryKey]) { return false; }
            bool worked = false;

            //Create the MySqlCommand
            MySqlCommand cmd = new MySqlCommand("SELECT " + db.PrimaryKey + " FROM " + db.TableName + " WHERE ");
            for (int i = 0; i < db.Columns.Count; i++)
            {
                if (null != this[db.Columns[i]] && db.PrimaryKey != db.Columns[i])
                {
                    //Add the Column
                    cmd.CommandText += " " + db.Columns[i] + "=@" + i;
                    cmd.Parameters.AddWithValue("@" + i, this[db.Columns[i]]);
                    //Don't know how many we have, so always add an AND.
                    cmd.CommandText += " AND ";
                }
            }
            //Rmove the last AND
            cmd.CommandText = cmd.CommandText.Remove(cmd.CommandText.Length - 5, 5);
            cmd.CommandText += " ORDER BY " + db.PrimaryKey + " DESC";
            cmd.CommandText += " LIMIT 1;";

            //Open the connection and run it
            MySqlConnection conn = new MySqlConnection(db.ConnectionString);
            try
            {
                //Tell the query to use this connecton
                cmd.Connection = conn;

                //Open the Connection
                conn.Open();

                //Get the ID
                object id = cmd.ExecuteScalar();
                //Check if it worked.
                if (null != id)
                {
                    this[db.PrimaryKey] = cmd.ExecuteScalar();
                    worked = true;
                }
            }
            catch (MySqlException mEx)
            {
                if (attempts > -1)
                {
                    //Try it again
                    FindIndex(db, attempts - 1);
                }
                else
                {
                    //It's not happening
                    DBObject.OnError("MySql Exception - " + conn, mEx);
                }
            }
            catch (Exception ex)
            {
                DBObject.OnError("FindIndex( " + cmd.CommandText + ")", ex);
            }
            finally
            {
                //Always close the Connection
                conn.Close();
            }
            //Return if it worked or not.
            return worked;
        }
        /// <summary>
        /// Finds the index from only the columns specified.
        /// </summary>
        /// <param name="db">The DBObject for this row</param>
        /// <param name="columns">The columns that must match for it to be considered.</param>
        /// <param name="attemps">The number of times to keep trying this method incase of error.</param>
        protected bool FindIndex(DBObject db, string[] columns, int attempts)
        {
            //Make sure we don't already know the Primary Key
            if (null != this[db.PrimaryKey]) { return false; }
            bool worked = false;

            //Create the MySqlCommand
            MySqlCommand cmd = new MySqlCommand("SELECT " + db.PrimaryKey + " FROM " + db.TableName + " WHERE ");
            for (int i = 0; i < columns.Length; i++)
            {
                if (null != columns[i])
                {
                    //Add the Column
                    cmd.CommandText += " " + columns[i] + "=@" + i;
                    cmd.Parameters.AddWithValue("@" + i, this[columns[i]]);
                    //Don't know how many we have, so always add an AND.
                    cmd.CommandText += " AND ";
                }
                else
                {
                    //Add the Column
                    cmd.CommandText += " " + columns[i] + " IS NULL";
                    //Don't know how many we have, so always add an AND.
                    cmd.CommandText += " AND ";
                }
            }
            //Rmove the last AND
            cmd.CommandText = cmd.CommandText.Remove(cmd.CommandText.Length - 5, 5);
            cmd.CommandText += " ORDER BY " + db.PrimaryKey + " DESC";
            cmd.CommandText += " LIMIT 1;";

            //Open the connection and run it
            MySqlConnection conn = new MySqlConnection(db.ConnectionString);
            try
            {
                //Tell the query to use this connecton
                cmd.Connection = conn;

                //Open the Connection
                conn.Open();

                //Get the ID
                object id = cmd.ExecuteScalar();
                //Check if it worked
                if (null != id)
                {
                    this[db.PrimaryKey] = cmd.ExecuteScalar();
                    worked = true;
                }
            }
            catch (MySqlException mEx)
            {
                if (attempts > -1)
                {
                    //Try it again
                    FindIndex(db, attempts - 1);
                }
                else
                {
                    //It's not happening
                    DBObject.OnError("MySql Exception - " + conn, mEx);
                }
            }
            catch (Exception ex)
            {
                DBObject.OnError("FindIndex( " + cmd.CommandText + ")", ex);
            }
            finally
            {
                //Always close the Connection
                conn.Close();
            }
            //Return if it worked
            return worked;
        }

        /// <summary>
        /// Fills in the Row by its ID
        /// </summary>
        /// <param name="db">The DBObject for this row</param>
        public void FillFromIndex(DBObject db)
        {
            //Make sure we know the Primary Key
            if (null == this[db.PrimaryKey]) { throw new NoPrimaryKeyException(); }

            //Create the MySqlCommand
            MySqlCommand cmd = new MySqlCommand("SELECT * FROM " + db.TableName + " WHERE " + db.PrimaryKey + "=@0");
            cmd.Parameters.AddWithValue("@0", this[db.PrimaryKey]);

            //Open the connection and run it
            MySqlConnection conn = new MySqlConnection(db.ConnectionString);
            try
            {
                //Tell the query to use this connecton
                cmd.Connection = conn;

                //Open the Connection
                conn.Open();

                //Get the Row
                MySqlDataReader reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    for (int i = 0; i < db.Columns.Count; i++)
                    {
                        string column = db.Columns[i];
                        if (db.PrimaryKey != column)
                        {
                            //We want to convert DBNulls to just normal null
                            if (reader[i].GetType() == typeof(DBNull))
                            {
                                this[column] = null;
                            }
                            else
                            {
                                this[column] = reader[column];
                            }
                        }
                    }
                }
                reader.Close();
            }
            catch (Exception ex)
            {
                DBObject.OnError("FindIndex( " + cmd.CommandText + ")", ex);
            }
            finally
            {
                //Always close the Connection
                conn.Close();
            }
        }

        
    }
}