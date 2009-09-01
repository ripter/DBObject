using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;
using org.ZenSoftware;
using org.zensoftware;

namespace DBObject_2_A
{
    class Program
    {
        static public string connMy = "Server=192.168.1.19;User Id=web;Password=0192dog;Persist Security Info=True;Database=ifn_wellness;pooling=true";
        
        static void Main(string[] args)
        {
            //Get a user
            //User user = User.ByID(1313);
            List<User> user_list = User.ByFirstName("chris");

            foreach (User user in user_list)
            {
                //Display some info
                Console.WriteLine("Full Name: " + user.FullName);
                Console.WriteLine("\tBirthDate: " + user.BirthDate.ToShortDateString());
                Console.WriteLine("\tDeathDate: " + user.DeathDate.ToShortDateString());
                Console.WriteLine("");
            }


            Console.WriteLine("\n\nPress any key to exit.");
            Console.ReadKey();
        }
    }

    public class User : DBObject
    {
        public int ID
        {
            get { return (int)this.valueForKey("column_id"); }
            set { this.setValueForKey("column_id", value); }
        }
        public string FirstName
        {
            get
            {
                return (string)valueForKey("column_first_name");
            }
            set
            {
                setValueForKey("column_first_name", value);
            }
        }
        public string LastName
        {
            get
            {
                return (string)valueForKey("column_last_name");
            }
            set { setValueForKey("column_last_name", value); }
        }
        public string FullName
        {
            get
            {
                return this.FirstName + " " + this.LastName;
            }
        }
        public string Email
        {
            get
            {
                return (string)valueForKey("column_email");
            }
            set
            {
                setValueForKey("column_email", value);
            }
        }

        public DateTime BirthDate
        {
            get
            {
                if (null == valueForKey("user_info"))
                {
                    //Store the user_info table inside of this object
                    DBObject user_info = new DBObject("users_info", "user_id", this.ConnectionString);
                    //Create the query
                    MySqlCommand cmd = new MySqlCommand("SELECT birth_date, death_date FROM users_info WHERE user_id=?id");
                    cmd.Parameters.AddWithValue("?id", this.ID);
                    //Fill the object
                    user_info.Fill(cmd);
                    //Save the user_info
                    setValueForKey("user_info", user_info);
                }
                object obj = valueForKeyPath("user_info.column_birth_date");
                if (null != obj && DBNull.Value != obj)
                {
                    return (DateTime)valueForKeyPath("user_info.column_birth_date");
                }
                return DateTime.Now;
            }
            set
            {
                setValueForKeyPath("user_info.column_birth_date", value);
            }
        }
        public DateTime DeathDate
        {
            get
            {
                if (null == valueForKey("user_info"))
                {
                    //Store the user_info table inside of this object
                    DBObject user_info = new DBObject("users_info", "user_id", this.ConnectionString);
                    //Create the query
                    MySqlCommand cmd = new MySqlCommand("SELECT birth_date, death_date FROM users_info WHERE user_id=?id");
                    cmd.Parameters.AddWithValue("?id", this.ID);
                    //Fill the object
                    user_info.Fill(cmd);
                    //Save the user_info
                    setValueForKey("user_info", user_info);
                }
                object obj = valueForKeyPath("user_info.column_death_date");
                if (null != obj && DBNull.Value != obj)
                {
                    return (DateTime)valueForKeyPath("user_info.column_death_date");
                }
                return DateTime.Now;
            }
            set
            {
                setValueForKeyPath("user_info.column_death_date", value);
                //((DBObject)valueForKey("user_info")).setValueForKey("column_death_date", value);
            }
        }

        public User()
            :base("users", "id", Program.connMy)
        {
        }

        public User(string first_name, string last_name)
            :base("users", "id", Program.connMy)
        {
            //Setup the connection
            this.TableName = "users";
            this.IndexColumn = "id";
            this.ConnectionString = Program.connMy;

            //Add the columnds
            this.FirstName = first_name;
            this.LastName = last_name;
            this.Email = "DELETEME@junk.com";

            this.Insert();
        }

        static public User ByID(int id)
        {
            //Id is the index, so just return by index
            return User.ByIndex<User>(id);
        }

        static public List<User> ByFirstName(string name)
        {
            return User.Where<User>("first_name", name);
        }

        public void Update()
        {
            this.Update();
        }

        public void ISee(string forKey, KVCObject forObject, object oldValue, object newValue)
        {
            Console.WriteLine("\nI see a change!");
            Console.WriteLine("\told value: " + oldValue);
            Console.WriteLine("\tnew value: " + newValue);
            Console.Write("\n");
        }
    }
}
