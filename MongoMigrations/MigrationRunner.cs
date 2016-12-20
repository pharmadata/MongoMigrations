namespace MongoMigrations
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using MongoDB.Bson.Serialization;
    using MongoDB.Driver;

    public class MigrationRunner
    {
        static MigrationRunner()
        {
            Init();
        }

        public static void Init()
        {
            BsonSerializer.RegisterSerializer(typeof(MigrationVersion), new MigrationVersionSerializer());
        }

        public MigrationRunner(IMongoDatabase database)
        {
            Database = database;
            DatabaseStatus = new DatabaseMigrationStatus(this);
            MigrationLocator = new MigrationLocator();
        }

        public string DatabaseName => Database.DatabaseNamespace.DatabaseName;
        public IMongoDatabase Database { get; set; }
        public MigrationLocator MigrationLocator { get; set; }
        public DatabaseMigrationStatus DatabaseStatus { get; set; }

        public virtual void UpdateToLatest()
        {
            Console.WriteLine(WhatWeAreUpdating() + " to latest...");
            UpdateTo(MigrationLocator.LatestVersion());
        }

        private string WhatWeAreUpdating()
        {
            return $"Updating server(s) \"{ServerAddresses()}\" for database \"{DatabaseName}\"";
        }

        private string ServerAddresses()
        {
            return string.Join(",", Database.Client.Settings.Servers.Select(x => x.Host));
        }

        protected virtual void ApplyMigrations(IEnumerable<Migration> migrations)
        {
            migrations.ToList()
                      .ForEach(ApplyMigration);
        }

        protected virtual void ApplyMigration(Migration migration)
        {
            Console.WriteLine(new { Message = "Applying migration", migration.Version, migration.Description, DatabaseName });

            var appliedMigration = DatabaseStatus.StartMigration(migration);
            migration.Database = Database;
            try
            {
                migration.Update();
            }
            catch (Exception exception)
            {
                OnMigrationException(migration, exception);
            }
            DatabaseStatus.CompleteMigration(appliedMigration);
        }

        protected virtual void OnMigrationException(Migration migration, Exception exception)
        {
            var message = new
            {
                Message = "Migration failed to be applied: " + exception.Message,
                migration.Version,
                Name = migration.GetType(),
                migration.Description,
                DatabaseName = DatabaseName
            };
            Console.WriteLine(message);
            throw new MigrationException(message.ToString(), exception);
        }

        public virtual void UpdateTo(MigrationVersion updateToVersion)
        {
            var currentVersion = DatabaseStatus.GetLastAppliedMigration();
            Console.WriteLine(new { Message = WhatWeAreUpdating(), currentVersion, updateToVersion, DatabaseName = DatabaseName });

            var migrations = MigrationLocator.GetMigrationsAfter(currentVersion)
                                             .Where(m => m.Version <= updateToVersion);

            ApplyMigrations(migrations);
        }
    }
}