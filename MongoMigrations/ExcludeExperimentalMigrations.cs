using System.Reflection;

namespace MongoMigrations
{
    using System.Linq;

    public class ExcludeExperimentalMigrations : MigrationFilter
    {
        public override bool Exclude(Migration migration)
        {
            if (migration == null)
            {
                return false;
            }
            return migration.GetType()
                .GetTypeInfo()
                .GetCustomAttributes(true)
                .OfType<ExperimentalAttribute>()
                .Any();
        }
    }
}