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
            MySqlCommand cmd = new MySqlCommand("INSERT INTO " + db.TableName + "(");
            string values = " VALUES(";
            for (int i = 0; i < db.Columns.Count; i++)
            {
                //Update everythign but the primary key.
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
        public void FindIndex(DBObject db)
        {
            //Make sure we don't already know the Primary Key
            if (null != this[db.PrimaryKey]) { return; }

            //Create the MySqlCommand
            MySqlCommand cmd = new MySqlCommand("SELECT " + db.PrimaryKey + " FROM " + db.TableName + " WHERE " );
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
                this[db.PrimaryKey] = cmd.ExecuteScalar();
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
