namespace KafkaConnectSqlServer_POC.Helper
{
    public class DbResult<T>
    {
        public DbResult(T result, bool error)
        {
            Result = result;
            Error = error;
        }

        public T Result { get; }
        public bool Error { get; }
    }
}
