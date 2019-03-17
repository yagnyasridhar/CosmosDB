using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;

namespace SqlCosmos
{
    class Program
    {
        private const string EndpointUrl = "https://cosmossqlsridhar.documents.azure.com:443/";
        private const string PrimaryKey = "NwhY4Bxj07oC5fj7fXSO1ltfSvMqHO0vtcaacTOYfUwqYDg0otsTnmK9NKcaoFseSa9O0EthvjAiUAXxtB6B5Q==";
        public static DocumentClient client = new DocumentClient(new Uri(EndpointUrl), PrimaryKey);

        static void Main(string[] args)
        {
            Program prg = new Program();
            try
            {
                Database db = Program.GetOrCreateDatabaseAsync("EmployeeDB").GetAwaiter().GetResult();
                if (prg.CreateDB_Collection(db))
                {
                    Employee emp1 = new Employee { ID = 43953, Name = "Sridhar", Department = "IT", Address = new Address { State = "Tamil Nadu", Country = "India" } };
                    Employee emp2 = new Employee { ID = 007, Name = "Sridhar", Department = "Finance", Address = new Address { State = "Tamil Nadu", Country = "India" } };

                    Document d1 = prg.CreateItemAsync(emp1).GetAwaiter().GetResult();
                    Document d2 = prg.CreateItemAsync(emp2).GetAwaiter().GetResult();
                    prg.GetDocumentAsync(d2.Id, "Finance").GetAwaiter().GetResult();
                    prg.DeleteItemAsync(d2.Id, "Finance").GetAwaiter().GetResult();
                }
                else
                {
                    Console.WriteLine("Exception caught while creating DB");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception =>" + ex.Message);
            }
        }

        private static async Task<Database> GetOrCreateDatabaseAsync(string id)
        {
            Database db = await client.CreateDatabaseIfNotExistsAsync(new Database { Id = "EmployeeDB" });
            return db;
        }

        public bool CreateDB_Collection(Database dbRequest)
        {
            bool status = false;
            try
            {
                var collectionReq = client.CreateDocumentCollectionIfNotExistsAsync(
                dbRequest.SelfLink,
                new DocumentCollection
                {
                    Id = "Employee",
                    PartitionKey = new PartitionKeyDefinition
                    {
                        Paths = new Collection<string> { "/Department" }
                    }
                },
                new RequestOptions { OfferThroughput = 50000 }).GetAwaiter().GetResult();
                status = (collectionReq.StatusCode == System.Net.HttpStatusCode.Created) || (collectionReq.StatusCode == System.Net.HttpStatusCode.OK);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return status;
        }

        public async Task<Document> CreateItemAsync(Employee item)
        {
            return await client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri("EmployeeDB", "Employee"), item);
        }

        public async Task DeleteItemAsync(string id, string partitionKey)
        {
            await client.DeleteDocumentAsync(UriFactory.CreateDocumentUri("EmployeeDB", "Employee", id), new RequestOptions { PartitionKey = new PartitionKey(partitionKey) });
        }

        public async Task<Document> GetDocumentAsync(string id, string partitionKey)
        {
            try
            {
                Document document = await client.ReadDocumentAsync(UriFactory.CreateDocumentUri("EmployeeDB", "Employee", id), new RequestOptions { PartitionKey = new PartitionKey(partitionKey) });
                return document;
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    return null;
                }
                else
                {
                    throw e;
                }
            }
        }
        public async Task<Document> UpdateItemAsync(string id, Employee item)
        {
            return await client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri("EmployeeDB", "Employee", id), item);
        }
    }

    public class Employee
    {
        public int ID { get; set; }
        public string Name { get; set; }
        public Address Address { get; set; }
        public string Department { get; set; }
    }

    public class Address
    {
        public string State { get; set; }
        public string Country { get; set; }
    }
}
