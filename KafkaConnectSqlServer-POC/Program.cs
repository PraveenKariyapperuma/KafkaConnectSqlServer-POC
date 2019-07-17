using KafkaConnectSqlServer_POC.Helper;
using Microsoft.Win32;
using System;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConnectSqlServer_POC
{
    class Program
    {
        #region Window handles

        private const int MF_BYCOMMAND = 0x00000000;
        public const int SC_CLOSE = 0xF060;
        const uint ENABLE_QUICK_EDIT = 0x0040;
        const int STD_INPUT_HANDLE = -10;

        [DllImport("user32.dll")]
        public static extern bool SetForegroundWindow(IntPtr WindowHandle);

        [DllImport("user32.dll")]
        public static extern int DeleteMenu(IntPtr hMenu, int nPosition, int wFlags);

        [DllImport("user32.dll")]
        private static extern IntPtr GetSystemMenu(IntPtr hWnd, bool bRevert);

        [DllImport("kernel32.dll", ExactSpelling = true)]
        private static extern IntPtr GetConsoleWindow();

        [DllImport("kernel32.dll", SetLastError = true)]
        static extern IntPtr GetStdHandle(int nStdHandle);

        [DllImport("kernel32.dll")]
        static extern bool GetConsoleMode(IntPtr hConsoleHandle, out uint lpMode);

        [DllImport("kernel32.dll")]
        static extern bool SetConsoleMode(IntPtr hConsoleHandle, uint dwMode);

        #endregion

        #region Fields

        static readonly Process[] _consoles = new Process[4];
        static bool _appStatus = true;
        static bool _inProgress = false;
        static readonly string _resourcePath = Path.Combine(Path.GetPathRoot(Environment.SystemDirectory), "POC-Resources\\");
        static readonly string _jreResource = "jre-8u211-windows-x64.exe";
        static string _kafkaResource = "kafka_2.12-2.0.0.zip";
        static readonly string _kafkaBinPath = _resourcePath + _kafkaResource.Substring(0, _kafkaResource.Length - 4) + "\\bin\\windows";
        static readonly string _kafaConfigPath = _resourcePath + _kafkaResource.Substring(0, _kafkaResource.Length - 4) + "\\config";
        static string _kafkaDataPath = _resourcePath + _kafkaResource.Substring(0, _kafkaResource.Length - 4) + "\\data";
        static string _consolePath = Path.Combine(_resourcePath, _kafkaResource.Substring(0, _kafkaResource.Length - 4));
        static readonly string _jrePath = @"C:\Program Files (x86)\Common Files\Oracle\Java\javapath";
        static bool _interactiveMode = true;

        #endregion

        static void Main(string[] args)
        {
            DeleteMenu(GetSystemMenu(GetConsoleWindow(), false), SC_CLOSE, MF_BYCOMMAND);
            Console.CancelKeyPress += Console_CancelKeyPress;
            DisableQuickEdit();

            try
            {
                ValidateAppConfiguration();
                InstallJavaRuntime();
                GenerateKafkaResources();
                ConfigureKafka();
                ConfigureEnvironments();
                GenerateDatabase();
                OpenConsoles();
                InteractiveMode();
            }
            catch (Exception ex)
            {
                _appStatus = false;
                _inProgress = false;
                Thread.Sleep(1000);
                Console.WriteLine($"An error occurred : {ex.Message}");
                Console.WriteLine("\nPress any key to exit.");
                Console.ReadKey();
                Console_CancelKeyPress(null, null);
                Environment.Exit(0);
            }
        }

        static void DisableQuickEdit()
        {
            uint consoleMode;
            IntPtr consoleHandle = GetStdHandle(STD_INPUT_HANDLE);

            GetConsoleMode(consoleHandle, out consoleMode);
            consoleMode &= ~ENABLE_QUICK_EDIT;
            SetConsoleMode(consoleHandle, consoleMode);
        }

        private static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            if (_consoles.Where(c => c != null).Any())
            {
                for (int i = _consoles.Length - 1; i >= 0; i--)
                {
                    if (_consoles[i] != null)
                    {
                        int IDstring = System.Convert.ToInt32(_consoles[i].Id);
                        Process tempProc = Process.GetProcessById(IDstring);
                        tempProc.CloseMainWindow();
                        tempProc.WaitForExit();
                    }
                }
            }
        }

        private static void GenerateResourcePath()
        {
            if (!Directory.Exists(_resourcePath))
            {
                Directory.CreateDirectory(_resourcePath);
            }
        }

        private static bool VerifyJre()
        {
            RegistryKey rk = RegistryKey.OpenBaseKey(RegistryHive.LocalMachine, RegistryView.Registry64);
            RegistryKey subKey = rk.OpenSubKey("SOFTWARE\\JavaSoft\\Java Runtime Environment");
            return subKey != null ? true : false;
        }

        private static void InstallJavaRuntime()
        {
            Console.WriteLine("Checking if Java runtime is installed.");
            GenerateResourcePath();
            Sleep(2000);

            if (VerifyJre())
            {
                Console.WriteLine($"Java runtime is already installed.");
                Sleep(2000);
            }
            else
            {
                Console.WriteLine("Installing java runtime.");

                if (!File.Exists(_resourcePath + _jreResource))
                {
                    using (Stream stream = Assembly.GetExecutingAssembly().GetManifestResourceStream("KafkaConnectSqlServer_POC.Resources.jre-8u211-windows-x64.exe"))
                    {
                        byte[] bytes = new byte[stream.Length];
                        stream.Read(bytes, 0, bytes.Length);
                        File.WriteAllBytes(_resourcePath + _jreResource, bytes);
                    }
                }

                Process process = new Process() { StartInfo = new ProcessStartInfo(_resourcePath + _jreResource, "/s") };
                process.Start();
                ShowProgress();
                process.WaitForExit();

                _inProgress = false;
                Sleep(2000);
            }
        }

        private static void GenerateKafkaResources()
        {
            Console.WriteLine("Generating kafka resources.");
            ShowProgress();
            string kafkaPath = _kafkaResource.Substring(0, _kafkaResource.Length - 4);

            if (Directory.Exists(_resourcePath + kafkaPath))
            {
                Directory.Delete(_resourcePath + kafkaPath, true);
            }

            Directory.CreateDirectory(_resourcePath + kafkaPath);

            using (Stream stream = Assembly.GetExecutingAssembly().GetManifestResourceStream("KafkaConnectSqlServer_POC.Resources.kafka_2.12-2.0.0.zip"))
            {
                byte[] bytes = new byte[stream.Length];
                stream.Read(bytes, 0, bytes.Length);
                File.WriteAllBytes(_resourcePath + _kafkaResource, bytes);
            }

            System.IO.Compression.ZipFile.ExtractToDirectory(_resourcePath + _kafkaResource, _resourcePath + kafkaPath);
            File.Delete(_resourcePath + _kafkaResource);
            _inProgress = false;
            Sleep(2000);
        }

        private static void ConfigureKafka()
        {
            Console.WriteLine("Configuring kafka.");
            ShowProgress();

            var zookeeperConfig = File.ReadAllText(_kafaConfigPath + Constants.ZookeeperConfig.Name);
            zookeeperConfig = zookeeperConfig.Replace(Constants.ZookeeperConfig.DataDirectory, _kafkaDataPath.Replace("\\", "/") + "/zookeeper");
            File.WriteAllText(_kafaConfigPath + Constants.ZookeeperConfig.Name, zookeeperConfig);

            var kafkaServerConfig = File.ReadAllText(_kafaConfigPath + Constants.KafkaServerConfig.Name);
            kafkaServerConfig = kafkaServerConfig.Replace(Constants.KafkaServerConfig.LogDirectory, _kafkaDataPath.Replace("\\", "/") + "/kafka");
            File.WriteAllText(_kafaConfigPath + Constants.KafkaServerConfig.Name, kafkaServerConfig);

            var connectStandaloneConfig = File.ReadAllText(_kafaConfigPath + Constants.StandaloneConfig.Name);
            connectStandaloneConfig = connectStandaloneConfig.Replace(Constants.StandaloneConfig.OffsetFileName, _kafkaDataPath.Replace("\\", "/") + Constants.StandaloneConfig.FileName);
            File.WriteAllText(_kafaConfigPath + Constants.StandaloneConfig.Name, connectStandaloneConfig);

            var connectSqlServerConfig = File.ReadAllText(_kafaConfigPath + Constants.SqlServerConfig.Name);
            connectSqlServerConfig = connectSqlServerConfig.Replace(Constants.SqlServerConfig.Hostname, DbConnection.Hostname);
            connectSqlServerConfig = connectSqlServerConfig.Replace(Constants.SqlServerConfig.ServerName, DbConnection.ServerName);
            connectSqlServerConfig = connectSqlServerConfig.Replace(Constants.SqlServerConfig.Port, DbConnection.Port);
            connectSqlServerConfig = connectSqlServerConfig.Replace(Constants.SqlServerConfig.UserName, DbConnection.UserName);
            connectSqlServerConfig = connectSqlServerConfig.Replace(Constants.SqlServerConfig.Password, DbConnection.Password);
            File.WriteAllText(_kafaConfigPath + Constants.SqlServerConfig.Name, connectSqlServerConfig);

            _inProgress = false;
            Sleep(2000);
        }

        private static void ConfigureEnvironments()
        {
            var envVaribles = Environment.GetEnvironmentVariable("Path", EnvironmentVariableTarget.Process);
            if (envVaribles == null || !envVaribles.Split(';').Any(c => c == _jrePath))
            {
                envVaribles = envVaribles + ';' + _jrePath + ';';
                Environment.SetEnvironmentVariable("Path", envVaribles, EnvironmentVariableTarget.Process);
            }

            envVaribles = null;
            envVaribles = Environment.GetEnvironmentVariable("PATH", EnvironmentVariableTarget.Process);
            if (envVaribles == null || !envVaribles.Split(';').Any(c => c == _kafkaBinPath))
            {
                envVaribles = envVaribles + ';' + _kafkaBinPath + ';';
                Environment.SetEnvironmentVariable("PATH", envVaribles, EnvironmentVariableTarget.Process);
            }
        }

        private static void GenerateDatabase()
        {
            Console.WriteLine("Configuring database and related services.");
            ShowProgress();
            DbManager.Initialize();
            _inProgress = false;
            Sleep(2000);
        }

        private static void OpenConsoles()
        {
            ProcessStartInfo psi = new ProcessStartInfo
            {
                WorkingDirectory = _consolePath,
                FileName = "cmd",
                WindowStyle = ProcessWindowStyle.Minimized
            };

            Process zookeeperProcess = new Process();
            zookeeperProcess.StartInfo = psi;
            zookeeperProcess.StartInfo.Arguments = @"/K zookeeper-server-start.bat config\zookeeper.properties";
            Console.WriteLine("Starting zookeeper server.");
            ShowProgress();
            zookeeperProcess.Start();
            _consoles[0] = zookeeperProcess;
            Thread.Sleep(8000);
            _inProgress = false;
            Thread.Sleep(2000);

            Process kafkaProcess = new Process();
            kafkaProcess.StartInfo = psi;
            kafkaProcess.StartInfo.Arguments = @"/K kafka-server-start.bat config\server.properties";
            Console.WriteLine("Starting kafka server.");
            ShowProgress();
            kafkaProcess.Start();
            _consoles[1] = kafkaProcess;
            Thread.Sleep(8000);
            _inProgress = false;
            Thread.Sleep(2000);

            Process standaloneConnectorProcess = new Process();
            standaloneConnectorProcess.StartInfo = psi;
            standaloneConnectorProcess.StartInfo.Arguments = @"/K connect-standalone.bat config\connect-standalone.properties config\sql-deb.properties";
            Console.WriteLine("Starting sqlserver connector.");
            ShowProgress();
            standaloneConnectorProcess.Start();
            _consoles[2] = standaloneConnectorProcess;
            Thread.Sleep(8000);
            _inProgress = false;
            Thread.Sleep(2000);

            Process ConsumerProcess = new Process();
            ConsumerProcess.StartInfo = psi;
            ConsumerProcess.StartInfo.WindowStyle = ProcessWindowStyle.Normal;
            ConsumerProcess.StartInfo.Arguments = $@"/K kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic {DbConnection.ServerName}.dbo.Person --from-beginning";
            Console.WriteLine("Starting sqlserver connector consumer.");
            ShowProgress();
            ConsumerProcess.Start();
            _consoles[3] = ConsumerProcess;
            Thread.Sleep(4000);
            _inProgress = false;
            Thread.Sleep(2000);
            SetForegroundWindow(GetConsoleWindow());
        }

        private static void InteractiveMode()
        {
            Console.Clear();
            Console.Write("Please choose an operation : \n1 to Create.\n2 to Read.\n3 to Update.\n4 to Delete Or any other key to exit.\n=> ");
            while (_interactiveMode)
            {
                var result = Console.ReadKey();
                switch (result.Key)
                {
                    case ConsoleKey.NumPad1:
                    case ConsoleKey.D1:
                        {
                            CreateMode();
                            break;
                        }
                    case ConsoleKey.NumPad2:
                    case ConsoleKey.D2:
                        {
                            ReadMode();
                            break;
                        }
                    case ConsoleKey.NumPad3:
                    case ConsoleKey.D3:
                        {
                            UpdateMode();
                            break;
                        }
                    case ConsoleKey.NumPad4:
                    case ConsoleKey.D4:
                        {
                            DeleteMode();
                            break;
                        }
                    default:
                        {
                            _interactiveMode = false;
                            break;
                        }

                }

                if (_interactiveMode)
                    Console.Write("\nPlease choose an operation : \n1 to Create.\n2 to Read.\n3 to Update.\n4 to Delete Or any other key to exit.\n=> ");
            }

            Console_CancelKeyPress(null, null);
        }

        private static void Sleep(int ms)
        {
            Thread.Sleep(ms);
        }

        private static void ShowProgress()
        {
            Task.Run(() =>
            {
                _inProgress = true;
                int i = 0;

                while (_inProgress)
                {
                    if (i > 10)
                    {
                        Console.Write("\r           \r");
                        i = 0;
                    }
                    Console.Write(".");
                    i++;
                    Sleep(100);
                }
                Console.Write("\r           \r");
                Console.Write($"........... {(_appStatus == true ? "Done" : "Failed")}\n");
            });
        }

        private static void ValidateAppConfiguration()
        {
            foreach (var item in typeof(Constants.SqlServerConfig).GetFields().Where(c => c.Name != "Name"))
            {
                try
                {
                    var temp = ConfigurationManager.AppSettings[item.Name];
                    if (string.IsNullOrWhiteSpace(temp))
                        throw new Exception();
                    DbConnection.SetValue(item.Name, temp);
                }
                catch (Exception)
                {
                    throw new Exception($"Configuration value of {item.Name} is either invalid or malformed.");
                }
            }
        }

        private static void CreateMode()
        {
            Console.Write("\n\nPlease enter a person name or press '0' to choose another operation : \n=> ");

            while (true)
            {
                var result = Console.ReadLine();
                if (string.IsNullOrEmpty(result))
                {
                    Console.WriteLine("Name cannot be empty.");
                }
                else if (result == "0")
                {
                    break;
                }
                else
                {
                    DbResult<string> dbResult = DbManager.CreatePerson(result);

                    if (dbResult.Error)
                        Console.WriteLine($"Failed! \n{dbResult.Result}");
                    else
                    {
                        Console.WriteLine("Created!");
                        dbResult = DbManager.ReadLastCreatedPerson();
                        if (dbResult.Error)
                            Console.WriteLine($"Failed! \n{dbResult.Result}");
                        else
                            Console.WriteLine(dbResult.Result);
                    }
                }

                Console.Write("\nPlease enter a person name or press '0' to choose another operation : \n=> ");
            }

            Console.Clear();
        }

        private static void ReadMode()
        {
            Console.Write("\n\nPlease enter a person id or press '0' to choose another operation : \n=> ");

            while (true)
            {
                var result = Console.ReadLine();
                if (string.IsNullOrEmpty(result))
                {
                    Console.WriteLine("id cannot be empty.");
                }
                else if (!int.TryParse(result, out int r))
                {
                    Console.WriteLine("id should be numeric.");
                }
                else if (result == "0")
                {
                    break;
                }
                else
                {
                    DbResult<string> dbResult = DbManager.ReadPerson(result);

                    if (dbResult.Error)
                        Console.WriteLine($"Failed! \n{dbResult.Result}");
                    else
                        Console.WriteLine(dbResult.Result);
                }

                Console.Write("\nPlease enter a person id or press '0' to choose another operation : \n=> ");
            }

            Console.Clear();
        }

        private static void UpdateMode()
        {
            Console.Write("\n\nPlease enter a person id to be updated or press '0' to choose another operation : \n=> ");

            while (true)
            {
                var result0 = Console.ReadLine();
                if (string.IsNullOrEmpty(result0))
                {
                    Console.WriteLine("id cannot be empty.");
                }
                else if (!int.TryParse(result0, out int r))
                {
                    Console.WriteLine("id should be numeric.");
                }
                else if (result0 == "0")
                {
                    break;
                }
                else
                {
                    while (true)
                    {
                        Console.Write("\nPlease enter the person name to be updated or press '0' to change the id : \n=> ");

                        var result1 = Console.ReadLine();
                        if (string.IsNullOrEmpty(result1))
                        {
                            Console.WriteLine("name cannot be empty.");
                        }
                        else if (result1 == "0")
                        {
                            break;
                        }
                        else
                        {
                            DbResult<string> dbResult = DbManager.UpdatePerson(result0, result1);

                            if (dbResult.Error)
                                Console.WriteLine($"Failed! \n{dbResult.Result}");
                            else
                                Console.WriteLine(dbResult.Result);
                            break;
                        }
                    }
                }

                Console.Write("\nPlease enter a person id to be updated or press '0' to choose another operation : \n=> ");
            }

            Console.Clear();
        }

        private static void DeleteMode()
        {
            Console.Write("\n\nPlease enter a person id to delete or press '0' to choose another operation : \n=> ");

            while (true)
            {
                var result = Console.ReadLine();
                if (string.IsNullOrEmpty(result))
                {
                    Console.WriteLine("id cannot be empty.");
                }
                else if (!int.TryParse(result, out int r))
                {
                    Console.WriteLine("id should be numeric.");
                }
                else if (result == "0")
                {
                    break;
                }
                else
                {
                    DbResult<string> dbResult = DbManager.DeletePerson(result);

                    if (dbResult.Error)
                        Console.WriteLine($"Failed! \n{dbResult.Result}");
                    else
                        Console.WriteLine(dbResult.Result);
                }

                Console.Write("\nPlease enter a person id to delete or press '0' to choose another operation : \n=> ");
            }

            Console.Clear();
        }
    }
}
