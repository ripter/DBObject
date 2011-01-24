using System;
using System.Collections.Generic;
using System.Text;
using DBObject2;

namespace ConsoleTest
{
    public class Utility
    {
        static public string connMy = "Server=173.165.104.214;User Id=web;Password=0192dog;Persist Security Info=True;Database=nunit";
    }

    class Program
    {
        static void Main(string[] args)
        {
            Setup();


            DBObject db = new DBObject(Utility.connMy, "users", "id");

            //db.FillFromSelect("SELECT * FROM users WHERE middle_initial=@0 ", "A");
            
            //Create a row with the propertys we want to find
            DBRow where = new DBRow();
            where["middle_initial"] = "A";
            //Now find everyone with an A for a middle name.
            db.Where(where);
            

            //Verify they are the three we expect
            foreach (DBRow row in db.Rows)
            {
                Console.WriteLine("\tid:\t" + row["id"]);
                Console.WriteLine("\tfirst_name:\t" + row["first_name"]);
                Console.WriteLine("\tmiddle_initial:\t" + row["middle_initial"]);
                Console.WriteLine("\tlast_name:\t" + row["last_name"]);
                Console.WriteLine("\temail:\t" + row["email"]);
            }
            
           

            Teardown();


            Console.WriteLine("\n\nPress any key to exit.");
            Console.ReadKey();
        }

        
        static public void Setup()
        {
            //Create the test data.
            DBObject db = new DBObject(Utility.connMy, "users", "external_id"); //Lie about the primary key so we can insert it.

            DBRow dan = new DBRow();
            dan["id"] = 2;
            dan["first_name"] = "Dan";
            dan["middle_initial"] = "A";
            dan["last_name"] = "Rese";
            dan["email"] = "dan@ifntech.com";
            dan.Insert(db);

            DBRow chris = new DBRow();
            chris["id"] = 1313;
            chris["first_name"] = "Chris";
            chris["middle_initial"] = "A";
            chris["last_name"] = "Richards";
            chris["email"] = "chris@ifntech.com";
            chris.Insert(db);

            DBRow ross = new DBRow();
            ross["id"] = 1315;
            ross["first_name"] = "Ross";
            ross["middle_initial"] = "A";
            ross["last_name"] = "Weaver";
            ross["email"] = "rweaver@ifntech.com";
            ross.Insert(db);

            DBRow rick = new DBRow();
            rick["id"] = 1327;
            rick["first_name"] = "Rick";
            rick["middle_initial"] = "R";
            rick["last_name"] = "Frazer";
            rick["email"] = "rfrazer@ifntech.com";
            rick.Insert(db);

        }

        static public void Teardown()
        {
            //Delete all of the records
            DBObject db = new DBObject(Utility.connMy, "users", "id");
            db.Where("true=true");

            foreach (DBRow row in db.Rows)
            {
                row.Delete(db);
            }
        }
    }
}
