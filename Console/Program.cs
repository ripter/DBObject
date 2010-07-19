using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DBObject2;

namespace Console
{
    class Program
    {
        static public string connMy = "Server=192.168.1.1;User Id=user;Password=pasword;Persist Security Info=True;Database=nunit";

        static void Main(string[] args)
        {
            /*
            DBObject obj = new DBObject(connMy, "users", "id");
            obj.Where("id IN (1313,1315,1327)");

            var results = from row in obj.Rows
                          where (int)row["id"] == 1315
                          select new { id = row["id"], firstName = row["first_name"], lastName = row["last_name"], middleInital = row["middle_initial"] };

            foreach(var result in results){
                System.Console.Write(result.id);
                System.Console.Write("\t");
                System.Console.Write(result.firstName);
                System.Console.Write("\t");
                System.Console.Write(result.lastName);
                System.Console.Write("\t");
                System.Console.Write(result.middleInital);
                System.Console.Write("\n");
            }
            */

            DBObject obj = new DBObject(connMy, "users", "id");
            obj.Where("id=@0", 1313);
            obj.Columns.Add("badcolumn");   //Add a column that won't exist in the row
            //Verify we got the value we expected
            System.Console.WriteLine(obj.Rows[0]["middle_initial"]);

            //Now Change the value
            obj.Rows[0]["middle_initial"] = "Q";
            obj.Update();

            //Get it fresh from the DB.
            obj.Where("id=@0", 1313);
            //Verify we got the value we expected
            System.Console.WriteLine(obj.Rows[0]["middle_initial"] + " Verify Value Change");

            //Now Change it back
            obj.Rows[0]["middle_initial"] = "A";
            obj.Update();

            //Verify it reset
            obj.Where("id=@0", 1313);
            System.Console.WriteLine(obj.Rows[0]["middle_initial"]);


            System.Console.ReadKey();
        }
    }
}
