using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DBObject2;

namespace Console
{
    class Program
    {
        static public string connMy = "Server=66.249.242.34;User Id=web;Password=mzksie74;Persist Security Info=True;Database=nunit";

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

            //Make sure the row doesn't exist already.
            DBObject obj = new DBObject(connMy, "users", "id");
            obj.Where("email=@0", "ladygaga@pophit.com");
            System.Console.WriteLine(obj.Rows.Count);

            //Create a new Row
            DBRow ladygaga = new DBRow();
            ladygaga["first_name"] = "Lady";
            ladygaga["last_name"] = "Gaga";
            ladygaga["email"] = "ladygaga@pophit.com";

            //Insert the Row
            ladygaga.Insert(obj);


            //Try to get it now
            obj.Where("email=@0", "ladygaga@pophit.com");
            System.Console.WriteLine(obj.Rows.Count);

            System.Console.ReadKey();
        }
    }
}
