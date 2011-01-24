using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DBObject2;

namespace DBObject2
{
    class LINQ
    {
        static public string connMy = "Server=192.168.1.1;User Id=web;Password=web;Persist Security Info=True;Database=nunit";

        public void TestLinq()
        {
            DBObject obj = new DBObject(connMy, "users", "id");
            obj.Where("id IN (1313,1315,1327)");

            var results = from row in obj.Rows
                          where (int)row["id"] == 1313
                          select row["middle_initial"];
        }
    }
}