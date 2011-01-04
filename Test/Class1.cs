using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using DBObject2;

namespace TestDBObject2
{
    public class Utility
    {
        static public string connMy = "Server=192.168.1.1;User Id=web;Password=web;Persist Security Info=True;Database=nunit";
    }

    [TestFixture]
    public class TestBasicQuery
    {
        [SetUp]
        public void Setup()
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
        [TearDown]
        public void Teardown()
        {
            //Delete all of the records
            DBObject db = new DBObject(Utility.connMy, "users", "id");
            db.Where("true=true");

            foreach (DBRow row in db.Rows)
            {
                row.Delete(db);
            }
        }

        [Test]
        public void NumberOfColumns()
        {
            DBObject obj = DBObject.BySelect(Utility.connMy, "SELECT id, first_name, last_name, email FROM users WHERE id = @0", 1313);
            Assert.AreEqual(4, obj.Columns.Count);
        }

        [Test]
        public void NumberOfColumnsFromNew()
        {
            DBObject obj = new DBObject(Utility.connMy, "users", "id");
            Assert.AreEqual(12, obj.Columns.Count);
        }

        [Test]
        public void NumberOfColumnsFromNewWithWhere()
        {
            DBObject obj = new DBObject(Utility.connMy, "users", "id");
            obj.Where("email=@0", "ladygaga@pophit.com");
            Assert.AreEqual(12, obj.Columns.Count);
        }

        [Test]
        public void ColumnNames()
        {
            DBObject obj = DBObject.BySelect(Utility.connMy, "SELECT id, first_name, last_name, email FROM users WHERE id = @0", 1313);
            Assert.AreEqual("id", obj.Columns[0]);
            Assert.AreEqual("first_name", obj.Columns[1]);
            Assert.AreEqual("last_name", obj.Columns[2]);
            Assert.AreEqual("email", obj.Columns[3]);
        }

        [Test]
        public void ColumnNameFromNew()
        {
            DBObject obj = new DBObject(Utility.connMy, "users", "id");
            Assert.AreEqual("id", obj.Columns[0]);
            Assert.AreEqual("first_name", obj.Columns[1]);
            Assert.AreEqual("last_name", obj.Columns[2]);
            Assert.AreEqual("email", obj.Columns[3]);
        }



        [Test]
        public void NumberOfRows()
        {
            DBObject obj = DBObject.BySelect(Utility.connMy, "SELECT id, first_name, last_name, email FROM users WHERE id = @0", 1313);
            Assert.AreEqual(1, obj.Rows.Count);
        }

        [Test]
        public void NumberOfRowsWithLimit()
        {
            DBObject obj = DBObject.BySelect(Utility.connMy, "SELECT id, first_name, last_name, email FROM users ORDER BY id DESC LIMIT 2");
            Assert.AreEqual(2, obj.Rows.Count);
        }

        [Test]
        public void Column1FromRow1()
        {
            DBObject obj = DBObject.BySelect(Utility.connMy, "SELECT id, first_name, last_name, email FROM users WHERE id = @0", 1313);
            Assert.AreEqual("Chris", obj.Rows[0][obj.Columns[1]]);
        }

        [Test]
        public void Column1FromRow2()
        {
            DBObject obj = DBObject.BySelect(Utility.connMy, "SELECT id, first_name, last_name, email FROM users WHERE id = @0 OR id = @1", 1313, 1315);
            Assert.AreEqual("Ross", obj.Rows[1][obj.Columns[1]]);
        }

        [Test]
        public void FirstNameFromRow1()
        {
            DBObject obj = DBObject.BySelect(Utility.connMy, "SELECT id, first_name, last_name, email FROM users WHERE id = @0", 1313);
            Assert.AreEqual("Chris", obj.Rows[0]["first_name"]);
        }

        [Test]
        public void CreateWithConnectionThenQuery()
        {
            DBObject obj = new DBObject(Utility.connMy);
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
            DBObject obj = new DBObject(Utility.connMy, "users", "id");
            Assert.AreEqual("users", obj.TableName);
        }

        [Test]
        public void TableNameNotSet()
        {
            DBObject obj = new DBObject(Utility.connMy);
            Assert.Throws<NoTableException>(delegate { string foo = obj.TableName; });
        }
    }

    [TestFixture]
    public class TestAutoMethods
    {
        [SetUp]
        public void Setup()
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
        [TearDown]
        public void Teardown()
        {
            //Delete all of the records
            DBObject db = new DBObject(Utility.connMy, "users", "id");
            db.Where("true=true");

            foreach (DBRow row in db.Rows)
            {
                row.Delete(db);
            }
        }

        [Test]
        public void GetColumnNames()
        {
            DBObject obj = new DBObject(Utility.connMy, "users", "id");
            obj.Where("id=@0", 1313);
            Assert.AreEqual("Chris", obj.Rows[0]["first_name"]);
        }

        [Test]
        public void RowCount()
        {
            DBObject obj = new DBObject(Utility.connMy, "users", "id");
            obj.Where("id=@0 OR id=@1 OR id=@2", 1313, 1315, 2);
            Assert.AreEqual(3, obj.Rows.Count);
        }

        [Test]
        public void TotalRowCount()
        {
            DBObject obj = new DBObject(Utility.connMy, "users", "id");
            Assert.AreEqual(4, obj.TotalRowCount);
        }

        [Test]
        public void PrimaryKey()
        {
            DBObject obj = new DBObject(Utility.connMy, "users", "id");
            Assert.AreEqual("id", obj.PrimaryKey);
        }
    }

    [TestFixture]
    public class TestAutoQueries
    {
        [SetUp]
        public void Setup()
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
        [TearDown]
        public void Teardown()
        {
            //Delete all of the records
            DBObject db = new DBObject(Utility.connMy, "users", "id");
            db.Where("true=true");

            foreach (DBRow row in db.Rows)
            {
                row.Delete(db);
            }
        }

        [Test]
        public void UpdateOneRowOneColumn()
        {
            DBObject obj = new DBObject(Utility.connMy, "users", "id");
            obj.Where("id=@0", 1313);
            //Verify we got the value we expected
            Assert.AreEqual("A", obj.Rows[0]["middle_initial"], "Verifying Default Test Value");

            //Now Change the value
            obj.Rows[0]["middle_initial"] = "Z";
            obj.Update();

            //Get it fresh from the DB.
            obj.Where("id=@0", 1313);
            //Verify we got the value we expected
            Assert.AreEqual("Z", obj.Rows[0]["middle_initial"], "Testing Change");

            //Now Change it back
            obj.Rows[0]["middle_initial"] = "A";
            obj.Update();
            obj.Where("id=@0", 1313);
            //Verify we got the value we expected
            Assert.AreEqual("A", obj.Rows[0]["middle_initial"], "Verifying Value is Reset to Test Value");
        }

        [Test]
        public void UpdateThreeRowsOneColumn()
        {
            DBObject obj = new DBObject(Utility.connMy, "users", "id");
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
            Assert.AreEqual("X", dbrow["middle_initial"], "Test Change");

            dbrow = obj.Rows.Single(delegate(DBRow row) { return (int)row["id"] == 1315; });
            Assert.AreEqual("Y", dbrow["middle_initial"], "Test Change");

            dbrow = obj.Rows.Single(delegate(DBRow row) { return (int)row["id"] == 1327; });
            Assert.AreEqual("Z", dbrow["middle_initial"], "Test Change");

            //Reset the values
            obj.Where("id IN (1313,1315,1327)");
            dbrow = obj.Rows.Single(delegate(DBRow row) { return (int)row["id"] == 1313; });
            dbrow["middle_initial"] = "A";

            dbrow = obj.Rows.Single(delegate(DBRow row) { return (int)row["id"] == 1315; });
            dbrow["middle_initial"] = "B";

            dbrow = obj.Rows.Single(delegate(DBRow row) { return (int)row["id"] == 1327; });
            dbrow["middle_initial"] = "C";
            //Update
            obj.Update();


            //Verify reset
            obj.Where("id IN (1313,1315,1327)");
            dbrow = obj.Rows.Single(delegate(DBRow row) { return (int)row["id"] == 1313; });
            Assert.AreEqual("A", dbrow["middle_initial"], "Verify Reset to Test Value");

            dbrow = obj.Rows.Single(delegate(DBRow row) { return (int)row["id"] == 1315; });
            Assert.AreEqual("B", dbrow["middle_initial"], "Verify Reset to Test Value");

            dbrow = obj.Rows.Single(delegate(DBRow row) { return (int)row["id"] == 1327; });
            Assert.AreEqual("C", dbrow["middle_initial"], "Verify Reset to Test Value");


        }

        [Test]
        public void UpdateWithBadColumnName()
        {
            DBObject obj = new DBObject(Utility.connMy, "users", "id");
            obj.Where("id=@0", 1313);
            obj.Columns.Add("badcolumn");   //Add a column that won't exist in the row
            //Verify we got the value we expected
            Assert.AreEqual("A", obj.Rows[0]["middle_initial"]);

            //Now Change the value
            obj.Rows[0]["middle_initial"] = "Q";
            obj.Update();

            //Get it fresh from the DB.
            obj.Where("id=@0", 1313);
            //Verify we got the value we expected
            Assert.AreEqual("Q", obj.Rows[0]["middle_initial"], "Verify Value Change");

            //Now Change it back
            obj.Rows[0]["middle_initial"] = "A";
            obj.Update();

            //Verify it reset
            obj.Where("id=@0", 1313);
            Assert.AreEqual("A", obj.Rows[0]["middle_initial"], "Verify Test Value Reset");
        }

        [Test]
        public void InsertRecord()
        {
            //Make sure the row doesn't exist already.
            DBObject obj = new DBObject(Utility.connMy, "users", "id");
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
        public void InsertRecordShowInRow()
        {
            //Inserting a Record, the record should be added to the objects Rows property

            //--
            // These parrts are tested by InsertRecord()
            //Verify our record doesn't already exist.
            DBObject obj = new DBObject(Utility.connMy, "users", "id");
            obj.Where("email=@0", "ladygaga@pophit.com");
            Assert.AreEqual(0, obj.Rows.Count);

            //Create a new Row
            DBRow ladygaga = new DBRow();
            ladygaga["first_name"] = "Lady";
            ladygaga["last_name"] = "Gaga";
            ladygaga["email"] = "ladygaga@pophit.com";
            //--


            Assert.AreEqual(0, obj.Rows.Count);
            //Insert the Row
            ladygaga.Insert(obj);
            //Test that it's in the DBObject
            Assert.AreEqual(1, obj.Rows.Count);

            //--
            // These parrts are tested by InsertRecord()
            //Try to get it now
            obj.Where("email=@0", "ladygaga@pophit.com");
            Assert.AreEqual(1, obj.Rows.Count, "Checking that the record exists.");

            //Now get rid of it.
            obj.Rows[0].Delete(obj);
            //Verify that it's gone
            obj.Where("email=@0", "ladygaga@pophit.com");
            Assert.AreEqual(0, obj.Rows.Count);
            //--
        }

        [Test]
        public void FindIndex()
        {
            DBObject db = new DBObject(Utility.connMy, "users", "id");
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
            DBObject db = new DBObject(Utility.connMy, "users", "id");
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
            DBObject db = new DBObject(Utility.connMy, "users", "id");
            //Add just the data we want
            DBRow row = new DBRow();
            row["id"] = 1313;

            //Fill from the ID
            row.FillFromIndex(db);

            Assert.AreEqual("chris@ifntech.com", row["email"]);
        }
    }

    [TestFixture]
    public class TestFindingIndex
    {
        [SetUp]
        public void Setup()
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
        [TearDown]
        public void Teardown()
        {
            //Delete all of the records
            DBObject db = new DBObject(Utility.connMy, "users", "id");
            db.Where("true=true");

            foreach (DBRow row in db.Rows)
            {
                row.Delete(db);
            }
        }

        /// <summary>
        /// Test that finding by index returns true
        /// </summary>
        [Test]
        public void FindIndexReturnsTrue()
        {
            DBObject db = new DBObject(Utility.connMy, "users", "id");
            DBRow row = new DBRow();
            //Set the columns we want to find
            row["first_name"] = "Chris";
            row["last_name"] = "Richards";

            //Check that it reports success in finding the index
            Assert.IsTrue(row.FindIndex(db));
            //Check that it really did get the index.
            Assert.AreEqual(1313, row["id"]);
        }

        /// <summary>
        /// Test that finding by index returns true when we specify the colummns to use
        /// </summary>
        [Test]
        public void FindIndexWithColumnsReturnsTrue()
        {
            DBObject db = new DBObject(Utility.connMy, "users", "id");
            DBRow row = new DBRow();
            //Set the columns we want to find
            row["first_name"] = "Bob";  //It will still work because we are not checking this column
            row["last_name"] = "Richards";

            //Check that it reports success in finding the index
            Assert.IsTrue(row.FindIndex(db, new string[] {"last_name"}));
            //Check that it really did get the index.
            Assert.AreEqual(1313, row["id"]);
        }

        /// <summary>
        /// A failed find should return false.
        /// </summary>
        [Test]
        public void FailedFindIndexReturnsFalse()
        {
            DBObject db = new DBObject(Utility.connMy, "users", "id");
            DBRow row = new DBRow();
            //Set the columns we want to find
            row["first_name"] = "Bob";
            row["last_name"] = "JoeJoe";

            //Check that it reports failure in finding the index
            Assert.IsFalse(row.FindIndex(db));
            //Check that it really did not get the index.
            Assert.IsNull(row["id"]);
        }

        /// <summary>
        /// Test that finding by index returns true when we specify the colummns to use
        /// </summary>
        [Test]
        public void FailedFindIndexWithColumnsReturnsFalse()
        {
            DBObject db = new DBObject(Utility.connMy, "users", "id");
            DBRow row = new DBRow();
            //Set the columns we want to find
            row["first_name"] = "Bob";  //It will still fail because we are checking this column
            row["last_name"] = "Richards";

            //Check that it reports failure in finding the index
            Assert.IsFalse(row.FindIndex(db, new string[] { "first_name" }));
            //Check that it really did not get the index.
            Assert.IsNull(row["id"]);
        }
    }
}
