using System.Linq;
using MongoDB.Driver.Linq;

namespace MongoMigrations
{
    using System;
    using System.Collections.Generic;
    using MongoDB.Bson;
    using MongoDB.Driver;

    public abstract class CollectionMigration : Migration
    {
        protected string CollectionName;

        public CollectionMigration(MigrationVersion version, string collectionName) : base(version)
        {
            CollectionName = collectionName;
        }

        public virtual IMongoQueryable<BsonDocument> Filter()
        {
            return null;
        }

        public override void Update()
        {
            var collection = GetCollection();
            var documents = GetDocuments(collection);
            UpdateDocuments(collection, documents);
        }

        public virtual void UpdateDocuments(IMongoCollection<BsonDocument> collection, IEnumerable<BsonDocument> documents)
        {
            ValidateMigrationDocuments();
            BeforeMigration(collection);

            foreach (var document in documents)
            {
                try
                {
                    UpdateDocument(collection, document);
                }
                catch (Exception exception)
                {
                    OnErrorUpdatingDocument(document, exception);
                }
            }
            AfterMigration(collection);
        }

        public virtual void UpdateDocument(IMongoCollection<BsonDocument> collection, BsonDocument document) { }

        /// <summary>
        /// Callled before UpdateDocuments in collection.
        /// </summary>
        /// <param name="collection"></param>
        public virtual void BeforeMigration(IMongoCollection<BsonDocument> collection) { }

        /// <summary>
        /// Called after all migration hooks.
        /// </summary>
        /// <param name="collection"></param>
        public virtual void AfterMigration(IMongoCollection<BsonDocument> collection) { }

        /// <summary>
        /// Called before all migrations hooks are called. Throw excpetion to interrupt other migrations hooks.
        /// </summary>
        public virtual void ValidateMigrationDocuments() { }

        protected virtual void OnErrorUpdatingDocument(BsonDocument document, Exception exception)
        {
            var message =
                new
                {
                    Message = "Failed to update document",
                    CollectionName,
                    Id = document.TryGetDocumentId(),
                    MigrationVersion = Version,
                    MigrationDescription = Description
                };
            throw new MigrationException(message.ToString(), exception);
        }

        protected virtual IMongoCollection<BsonDocument> GetCollection()
        {
            return Database.GetCollection<BsonDocument>(CollectionName);
        }

        protected virtual IEnumerable<BsonDocument> GetDocuments(IMongoCollection<BsonDocument> collection)
        {
            var query = Filter();
            if (query != null)
            {
                return query.ToList();
            }

            return collection.Find(Builders<BsonDocument>.Filter.Empty).ToList();
        }
    }
}