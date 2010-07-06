﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using DBObject2;

namespace TestDBObject2
{
    [TestFixture]
    public class TestBasicQuery
    {
        static public string connMy = "Server=66.249.242.34;User Id=web;Password=mzksie74;Persist Security Info=True;Database=nunit";

        [Test]
        public void NumberOfColumns()
        {
            DBObject obj = DBObject.BySelect(connMy, "SELECT id, first_name, last_name, email FROM users WHERE id = @0", 1313);
            Assert.AreEqual(4, obj.Columns.Count);
        }

        [Test]
        public void NumberOfColumnsFromNew()
        {
            DBObject obj = new DBObject(connMy, "users", "id");
            Assert.AreEqual(11, obj.Columns.Count);
        }

        [Test]
        public void NumberOfColumnsFromNewWithWhere()
        {
            DBObject obj = new DBObject(connMy, "users", "id");
            obj.Where("email=@0", "ladygaga@pophit.com");
            Assert.AreEqual(11, obj.Columns.Count);
        }

        [Test]
        public void ColumnNames()
        {
            DBObject obj = DBObject.BySelect(connMy, "SELECT id, first_name, last_name, email FROM users WHERE id = @0", 1313);
            Assert.AreEqual("id", obj.Columns[0]);
            Assert.AreEqual("first_name", obj.Columns[1]);
            Assert.AreEqual("last_name", obj.Columns[2]);
            Assert.AreEqual("email", obj.Columns[3]);
        }

        [Test]
        public void ColumnNameFromNew()
        {
            DBObject obj = new DBObject(connMy, "users", "id");
            Assert.AreEqual("id", obj.Columns[0]);
            Assert.AreEqual("first_name", obj.Columns[1]);
            Assert.AreEqual("last_name", obj.Columns[2]);
            Assert.AreEqual("email", obj.Columns[3]);
        }

        

        [Test]
        public void NumberOfRows()
        {
            DBObject obj = DBObject.BySelect(connMy, "SELECT id, first_name, last_name, email FROM users WHERE id = @0", 1313);
            Assert.AreEqual(1, obj.Rows.Count);
        }

        [Test]
        public void NumberOfRowsWithLimit()
        {
            DBObject obj = DBObject.BySelect(connMy, "SELECT id, first_name, last_name, email FROM users ORDER BY id DESC LIMIT 10" );
            Assert.AreEqual(10, obj.Rows.Count);
        }

        [Test]
        public void Column1FromRow1()
        {
            DBObject obj = DBObject.BySelect(connMy, "SELECT id, first_name, last_name, email FROM users WHERE id = @0", 1313);
            Assert.AreEqual("Chris", obj.Rows[0][obj.Columns[1]]);
        }

        [Test]
        public void Column1FromRow2()
        {
            DBObject obj = DBObject.BySelect(connMy, "SELECT id, first_name, last_name, email FROM users WHERE id = @0 OR id = @1", 1313, 1315);
            Assert.AreEqual("Ross", obj.Rows[1][obj.Columns[1]]);
        }

        [Test]
        public void FirstNameFromRow1()
        {
            DBObject obj = DBObject.BySelect(connMy, "SELECT id, first_name, last_name, email FROM users WHERE id = @0", 1313);
            Assert.AreEqual("Chris", obj.Rows[0]["first_name"]);
        }

        [Test]
        public void CreateWithConnectionThenQuery()
        {
            DBObject obj = new DBObject(connMy);
            obj.FillFromSelect("SELECT id, first_name, last_name, email FROM users WHERE id = @0", 1313);
            Assert.AreEqual("Chris", obj.Rows[0]["first_name"]);
        }

        [Test]
        public void ThrowNoConnectionStringError()
        {
            DBObject obj = new DBObject();
            Assert.Throws<NoConnectionStringException>(delegate { obj.FillFromSelect("SELECT id, first_name, last_name, email FROM users WHERE id = @0", 1313); });
        }

        [Test]
        public void TableName()
        {
            DBObject obj = new DBObject(connMy, "users", "id");
            Assert.AreEqual("users", obj.TableName);
        }

        [Test]
        public void TableNameNotSet()
        {
            DBObject obj = new DBObject(connMy);
            Assert.Throws<NoTableException>(delegate { string foo = obj.TableName; });
        }
    }

    [TestFixture]
    public class TestAutoMethods
    {
        static public string connMy = "Server=66.249.242.34;User Id=web;Password=mzksie74;Persist Security Info=True;Database=nunit";

        [Test]
        public void GetColumnNames()
        {
            DBObject obj = new DBObject(connMy, "users", "id");
            obj.Where("id=@0", 1313);
            Assert.AreEqual("Chris", obj.Rows[0]["first_name"]);
        }

        [Test]
        public void RowCount()
        {
            DBObject obj = new DBObject(connMy, "users", "id");
            obj.Where("id=@0 OR id=@1 OR id=@2", 1313, 1315, 2);
            Assert.AreEqual(3, obj.Rows.Count);
        }

        [Test]
        public void TotalRowCount()
        {
            DBObject obj = new DBObject(connMy, "activity_reoccur", "id");
            Assert.AreEqual(13, obj.TotalRowCount);
        }

        [Test]
        public void PrimaryKey()
        {
            DBObject obj = new DBObject(connMy, "users", "id");
            Assert.AreEqual("id", obj.PrimaryKey);
        }
    }

    [TestFixture]
    public class TestAutoQueries
    {
        static public string connMy = "Server=66.249.242.34;User Id=web;Password=mzksie74;Persist Security Info=True;Database=nunit";

        [Test]
        public void UpdateOneRowOneColumn()
        {
            DBObject obj = new DBObject(connMy, "users", "id");
            obj.Where("id=@0", 1313);
            //Verify we got the value we expected
            Assert.AreEqual("A", obj.Rows[0]["middle_initial"]);
            
            //Now Change the value
            obj.Rows[0]["middle_initial"] = "Z";
            obj.Update();
            
            //Get it fresh from the DB.
            obj.Where("id=@0", 1313);
            //Verify we got the value we expected
            Assert.AreEqual("Z", obj.Rows[0]["middle_initial"]);
            
            //Now Change it back
            obj.Rows[0]["middle_inital"] = "A";
            obj.Update();
        }

        [Test]
        public void UpdateThreeRowsOneColumn()
        {
            DBObject obj = new DBObject(connMy, "users", "id");
            obj.Where("id IN (1313,1315,1327)");

            //
            //NOTE: This is not the best use of LINQ
            //       This would be alot more effectent/faster in a single loop with an if statement on ID

            //Change the columns
            DBRow dbrow = obj.Rows.Single(delegate(DBRow row) { return (int)row["id"] == 1313; });
            dbrow["middle_initial"] = "X";

            dbrow = obj.Rows.Single(delegate(DBRow row) { return (int)row["id"] == 1315; });
            dbrow["middle_initial"] = "Y";

            dbrow = obj.Rows.Single(delegate(DBRow row) { return (int)row["id"] == 1327; });
            dbrow["middle_initial"] = "Z";
            
            //Update
            obj.Update();


            //Now Verify
            obj.Where("id IN (1313,1315,1327)");
            dbrow = obj.Rows.Single(delegate(DBRow row) { return (int)row["id"] == 1313; });
            dbrow["middle_initial"] = "A";

            dbrow = obj.Rows.Single(delegate(DBRow row) { return (int)row["id"] == 1315; });
            dbrow["middle_initial"] = "B";

            dbrow = obj.Rows.Single(delegate(DBRow row) { return (int)row["id"] == 1327; });
            dbrow["middle_initial"] = "C";

            //Update
            obj.Update();
        }

        [Test]
        public void InsertRecord()
        {
            //Make sure the row doesn't exist already.
            DBObject obj = new DBObject(connMy, "users", "id");
            obj.Where("email=@0", "ladygaga@pophit.com");
            Assert.AreEqual(0, obj.Rows.Count);
            
            //Create a new Row
            DBRow ladygaga = new DBRow();
            ladygaga["first_name"] = "Lady";
            ladygaga["last_name"] = "Gaga";
            ladygaga["email"] = "ladygaga@pophit.com";
            
            //Insert the Row
            ladygaga.Insert(obj);

            //Try to get it now
            obj.Where("email=@0", "ladygaga@pophit.com");
            Assert.AreEqual(1, obj.Rows.Count, "Checking that the record exists.");

            //Now get rid of it.
            obj.Rows[0].Delete(obj);
            //Verify that it's gone
            obj.Where("email=@0", "ladygaga@pophit.com");
            Assert.AreEqual(0, obj.Rows.Count);
        }

        [Test]
        public void FindIndex()
        {
            DBObject db = new DBObject(connMy, "users", "id");
            //Add just the data we want
            DBRow row = new DBRow();
            row["email"] = "chris@ifntech.com";

            //Now find the id
            row.FindIndex(db);

            Assert.AreEqual(1313, row[db.PrimaryKey]);
        }

        [Test]
        public void FindIndexTwoColumns()
        {
            DBObject db = new DBObject(connMy, "users", "id");
            //Add just the data we want
            DBRow row = new DBRow();
            row["email"] = "chris@ifntech.com";
            row["first_name"] = "Chris";

            //Now find the id
            row.FindIndex(db);

            Assert.AreEqual(1313, row[db.PrimaryKey]);
        }

        [Test]
        public void FillFromIndex()
        {
            DBObject db = new DBObject(connMy, "users", "id");
            //Add just the data we want
            DBRow row = new DBRow();
            row["id"] = 1313;

            //Fill from the ID
            row.FillFromIndex(db);

            Assert.AreEqual("chris@ifntech.com", row["email"]);
        }
    }

}
