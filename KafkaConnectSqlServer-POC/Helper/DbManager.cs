using System;
using System.Data.SqlClient;

namespace KafkaConnectSqlServer_POC.Helper
{
    public class DbManager
    {
        static string connectionString;

        static DbManager()
        {
            string hostName = DbConnection.Hostname;
            string serverName = DbConnection.ServerName;
            string source = hostName == serverName ? serverName : $@"{hostName}\{serverName}";
            connectionString = $"Data Source={source}; User Id={DbConnection.UserName}; Password={DbConnection.Password}";
        }

        public static void Initialize()
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                object result = null;

                connection.Open();
                SqlCommand command = new SqlCommand(Constants.Query.ServerAgentStatus.Replace("{0}", "master"), connection);
                result = command.ExecuteScalar();

                if (result.ToString() == "Stopped.")
                {
                    command.CommandText = Constants.Query.ServerAgentStart;
                    command.ExecuteNonQuery();

                    command.CommandText = Constants.Query.ServerAgentStatus;
                    result = command.ExecuteScalar();

                    if (result.ToString() == "Stopped.")
                    {
                        throw new Exception("SqlServer Agent service could not be started");
                    }
                }

                command.CommandText  =Constants.Query.DbExists.Replace("{0}", "KafkaPOC");
                result = command.ExecuteScalar();
                if (((int)result) == 1)
                {
                    command.CommandText = Constants.Query.DropDb.Replace("{0}", "KafkaPOC");
                    command.ExecuteNonQuery();
                }
                command.CommandText = Constants.Query.CreateDb.Replace("{0}", "KafkaPOC");
                command.ExecuteNonQuery();
                command.CommandText = Constants.Query.CreateTable.Replace("{0}", "KafkaPOC");
                command.ExecuteNonQuery();
                command.CommandText = Constants.Query.EnableCDC.Replace("{0}", "KafkaPOC");
                command.ExecuteNonQuery();
                connection.Close();
            }
        }

        public static DbResult<string> CreatePerson(string name)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    SqlCommand command = new SqlCommand(string.Format(Constants.Query.CreatePerson, "KafkaPOC", name), connection);
                    command.ExecuteNonQuery();
                    connection.Close();
                    return new DbResult<string>(string.Empty, false);
                }
                catch (Exception ex)
                {
                    return new DbResult<string>(ex.Message, true);
                }
            }
        }

        public static DbResult<string> ReadPerson(string id)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    SqlCommand command = new SqlCommand(string.Format(Constants.Query.ReadPerson, "KafkaPOC", id), connection);
                    var result = command.ExecuteScalar();
                    connection.Close();
                    return new DbResult<string>(result.ToString(), false);
                }
                catch (Exception)
                {
                    return new DbResult<string>("Invalid Id.", true);
                }
            }
        }

        public static DbResult<string> UpdatePerson(string id, string name)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    SqlCommand command = new SqlCommand(string.Format(Constants.Query.UpdatePerson, "KafkaPOC", name, id), connection);
                    var result = command.ExecuteNonQuery();
                    connection.Close();
                    return new DbResult<string>((result > 0 ? "Updated!" : "Invalid Id."), false);
                }
                catch (Exception ex)
                {
                    return new DbResult<string>(ex.Message, true);
                }
            }
        }

        public static DbResult<string> DeletePerson(string id)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    SqlCommand command = new SqlCommand(string.Format(Constants.Query.DeletePerson, "KafkaPOC", id), connection);
                    var result = command.ExecuteNonQuery();
                    connection.Close();
                    return new DbResult<string>((result > 0 ? "Deleted!" : "Invalid Id."), false);
                }
                catch (Exception ex)
                {
                    return new DbResult<string>(ex.Message, true);
                }
            }
        }

        public static DbResult<string> ReadLastCreatedPerson()
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    SqlCommand command = new SqlCommand(string.Format(Constants.Query.GetLastCreated, "KafkaPOC"), connection);
                    var result = command.ExecuteScalar();
                    connection.Close();
                    return new DbResult<string>(result.ToString(), false);
                }
                catch (Exception ex)
                {
                    return new DbResult<string>(ex.Message, true);
                }
            }
        }
    }
}
