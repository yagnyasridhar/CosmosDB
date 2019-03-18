using Cassandra;
using Cassandra.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace CosmosCassandra
{
    class Program
    {
        private const string UserName = "cosmoscassandrasridhar";
        private const string Password = "wVO4JkSiMooBxxxyajRaVyFfWli8BnE5wpZD6m3aGcrfdxAV5CMNI8i1Trum1dornSvanlsLXpdFMSj3xuEMcQ==";
        private const string CassandraContactPoint = "cosmoscassandrasridhar.cassandra.cosmos.azure.com";
        private static readonly int CassandraPort = 10350;
        private ISession Session;

        static void Main(string[] args)
        {
            var options = new Cassandra.SSLOptions(SslProtocols.Tls12, true, Program.ValidateServerCertificate);
            options.SetHostNameResolver((ipAddress) => CassandraContactPoint);
            Cluster cluster = Cluster.Builder().WithCredentials(UserName, Password).WithPort(CassandraPort).AddContactPoint(CassandraContactPoint).WithSSL(options).Build();
            ISession session = cluster.Connect();

            session.Execute("DROP KEYSPACE IF EXISTS studentProfile");
            session.Execute("CREATE KEYSPACE studentProfile WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 };");
            Console.WriteLine("Created keyspace studentProfile");

            session.Execute("drop table IF EXISTS studentProfile.student");
            session.Execute("CREATE TABLE studentProfile.student (ID text, Name text, rank int, Standard text, PRIMARY KEY(ID))");
            Console.WriteLine("Created table student");


            string insertCQLQuery = "insert into studentProfile.student(ID, Name, rank, Standard) values (:a, :b, :c, :d)";
            var statement = session.Prepare(insertCQLQuery);
            Student std = new Student { ID = "11", Name = "Sridhar", Rank = 2, Standard = "II" };
            session.Execute(statement.Bind(new { a = std.ID, b = std.Name, c = std.Rank, d = std.Standard}));            
            string insert1 = "insert into studentProfile.student(ID, Name, rank, Standard) values (:a, :b, :c, :d)";
            var statement1 = session.Prepare(insert1);
            session.Execute(statement1.Bind(new { a = "1", b = "Guhan", c = 1, d = "XII" }));

            session.Execute("Update studentProfile.student set Standard='XII' where ID='11'");

            session.Execute("Delete from studentProfile.student where ID='11'");
        }

        public static bool ValidateServerCertificate(
            object sender,
            X509Certificate certificate,
            X509Chain chain,
            SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None)
                return true;
            Console.WriteLine("Certificate error: {0}", sslPolicyErrors);
            return false;
        }
    }

    public class Student
    {
        public string ID { get; set; }
        public string Name { get; set; }
        public string Standard { get; set; }
        public int Rank { get; set; }
    }
}
