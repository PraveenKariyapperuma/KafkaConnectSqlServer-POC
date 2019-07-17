namespace KafkaConnectSqlServer_POC.Helper
{
    public class Constants
    {
        public class ZookeeperConfig
        {
            public const string DataDirectory = "{DataDirectory}";
            public const string Name = "\\zookeeper.properties";
        }

        public class KafkaServerConfig
        {
            public const string LogDirectory = "{LogDirectory}";
            public const string Name = "\\server.properties";
        }

        public class StandaloneConfig
        {
            public const string OffsetFileName = "{OffsetFileName}";
            public const string Name = "\\connect-standalone.properties";
            public const string FileName = "/tmp/connect.offsets";
        }

        public class SqlServerConfig
        {
            public const string Name = "\\sql-deb.properties";
            public const string Hostname = "{Hostname}";
            public const string Port = "{Port}";
            public const string UserName = "{UserName}";
            public const string Password = "{Password}";
            public const string ServerName = "{ServerName}";
        }

        public class Query
        {
            public const string ServerAgentStatus = "EXEC xp_servicecontrol N'QUERYSTATE',N'SQLServerAGENT'";

            public const string ServerAgentStart = "EXEC xp_servicecontrol N'START',N'SQLServerAGENT'";

            public const string DbExists = @"IF DB_ID('{0}') IS NOT NULL 
                                                select 1 as result
                                             ELSE
                                                select 0 as result";

            public const string CreateDb = "Create database {0}";

            public const string DropDb = @"ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; Drop database {0}";

            public const string CreateTable = @"USE [{0}]
                                                CREATE TABLE [dbo].[Person](
                                                	[id] [int] IDENTITY(1,1) NOT NULL,
                                                	[name] [nvarchar](50) NULL,
                                                 CONSTRAINT [PK_Person] PRIMARY KEY CLUSTERED 
                                                ([id] ASC)
                                                WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) 
                                                ON [PRIMARY]) 
                                                ON [PRIMARY]";

            public const string EnableCDC = @"USE {0}
                                                        EXEC sys.sp_cdc_enable_db  

                                                        EXEC sys.sp_cdc_enable_table  
                                                        @source_schema = N'dbo',  
                                                        @source_name   = N'Person',  
                                                        @role_name     = NULL,  
                                                        @filegroup_name = N'PRIMARY',  
                                                        @supports_net_changes = 1";

            public const string CreatePerson = @"USE [{0}]
                                                    INSERT INTO Person
                                                    VALUES('{1}')";

            public const string ReadPerson = @"USE[{0}]
                                                    SELECT 'Id : ' + CONVERT(VARCHAR(10), [id]) +' '+ ', Name : ' + [name] as result
                                                    FROM Person
                                                    WHERE id = {1}";

            public const string UpdatePerson = @"USE [{0}]
                                                    UPDATE Person SET name = '{1}'
                                                    WHERE id = {2}";

            public const string DeletePerson = @"USE [{0}]
                                                    DELETE FROM Person
                                                    WHERE id = {1}";

            public const string GetLastCreated = @"USE[{0}]
                                                    SELECT TOP 1 'Id : ' + CONVERT(VARCHAR(10), [id]) +' '+ ', Name : ' + [name] as result
                                                    FROM Person ORDER BY [id] DESC";
        }
    }
}
