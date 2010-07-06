using System;
using System.Collections.Generic;
using System.Text;

namespace DBObject2

{
    /// <summary>
    /// Thrown when a Connection String is needed but can not be found.
    /// </summary>
    public class NoConnectionStringException : Exception
    {
        public NoConnectionStringException()
            : base("A Connection String has not been set for the DBObject")
        {
        }
    }
    /// <summary>
    /// Thrown when a method requires the table defined and it is not defined.
    /// </summary>
    public class NoTableException : Exception
    {
        public NoTableException()
            : base("A Table has not been set for the DBObject")
        {
        }
    }
    /// <summary>
    /// Thrown when a Primary Key is required but not defined.
    /// </summary>
    public class NoPrimaryKeyException : Exception
    {
        public NoPrimaryKeyException()
            : base("A Primary key has not been set for the DBObject")
        {
        }
    }
}
