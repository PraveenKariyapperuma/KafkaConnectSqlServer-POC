namespace KafkaConnectSqlServer_POC.Helper
{
    class DbConnection
    {
        static public string Hostname { get; private set; }
        static public string ServerName { get; private set; }
        static public string Port { get; private set; }
        static public string UserName { get; private set; }
        static public string Password { get; private set; }

        public static void SetValue(string name, string value)
        {
            typeof(DbConnection).GetProperty(name).SetValue(null, value);
        }
    }
}
