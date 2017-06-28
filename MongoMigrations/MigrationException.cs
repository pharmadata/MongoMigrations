namespace MongoMigrations
{
    using System;

    public class MigrationException : Exception
    {
        public MigrationException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}