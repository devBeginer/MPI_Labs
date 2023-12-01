
using Dapper;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Linq;
using MPI;

namespace ConsoleApp4
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Repository repository = new Repository();
            
            if (args[0] == "union")
            {
                parallelUnion(repository, args);
            }else if(args[0] == "insert")
            {
                parallelInsert(repository, args);
            }
            

            
        }
            

        static void parallelInsert(Repository userRepository, string[] args) { 
            MPI.Environment.Run(ref args, comm =>
                {
                    var myRank = MPI.Communicator.world.Rank;
                    var size = MPI.Communicator.world.Size;

                   

                    int arraySize = 4000;
                    if (!Int32.TryParse(args[1], out arraySize))
                    {
                        arraySize = 1000;
                    }

                    //var array = new List<Contact>(arraySize);
                    var array = new List<string>(arraySize);

                    DateTime start = DateTime.Now;
                    
                  

                    if (myRank == 0)
                    {

                        for (var i = 0; i < arraySize; i++)
                        {
                            //array.Add(new Contact("COMANY_TEST", "(000) 000-0000"));
                            array.Add(" SELECT 'COMANY_TEST', '(000) 000-0000' UNION ALL");
                        }
                        if (size > 1)
                        {
                            for (int i = 1; i < size; i++)
                            {
                                Communicator.world.Send(array, i, myRank);
                            }
                        }
                       
                    }

                    if (myRank > 0)
                    {
                        
                        //array = Communicator.world.Receive<List<Contact>>(0, 0);
                        array = Communicator.world.Receive<List<string>>(0, 0);

                    }

                    var query = "INSERT INTO Sales.Shippers(companyname, phone)";

                    for (var i = myRank * (arraySize / size); i < (myRank + 1) * (arraySize / size); i++)
                    {
                        query = query + array[i];
                        //userRepository.Create(array[i]);
                    }
                    userRepository.Create(query.Substring(0, query.Length - 9));

                    Communicator.world.Barrier();

                    if (myRank == 0)
                    {
                       
                        DateTime end = DateTime.Now;
                        TimeSpan ts = (end - start);
                        Console.WriteLine("Elapsed Time is {0} ms", ts.TotalMilliseconds);
                    }
                });
            }



        static void parallelUnion(Repository userRepository, string[] args) { 
           MPI.Environment.Run(ref args, comm =>
                {
                    var myRank = MPI.Communicator.world.Rank;
                    var size = MPI.Communicator.world.Size;

                  

                    var array = new List<Contact>();

                    DateTime start = DateTime.Now;
                    var queries = new List<string>();

                    int arraySize = 80;
                    if(!Int32.TryParse(args[1], out arraySize))
                    {
                        arraySize = 8;
                    }
                    

                    if (myRank == 0)
                    {
                        for (int i=0; i<arraySize; i++)
                        {
                            queries.Add("SELECT companyname, phone FROM Sales.Customers UNION ALL SELECT companyname, phone FROM Sales.Shippers ");
                            //queries.Add("SELECT companyname, phone FROM Sales.Customers  ");
                        }
                        if (size > 1)
                        {
                            for (int i=1; i<size; i++)
                            {
                                Communicator.world.Send(queries, i, myRank);
                            }
                        }
                        
                    }

                    if (myRank > 0)
                    {
                        queries = Communicator.world.Receive<List<string>>(0, 0);

                    }
                    //var arraySize = queries.Count;
                    var query = "";

                    for (var i = myRank * (arraySize / size); i < (myRank + 1) * (arraySize / size); i++)
                    {
                        query = query + queries[i] + "UNION ALL ";
                    }
                    
                    array = userRepository.GetContact(query.Substring(0, query.Length-10));
                    
                    if (myRank > 0)
                    {
                        
                        Communicator.world.Send(array, 0, myRank);
                        
                    }
                    

                    if (myRank == 0)
                    {
                        for (int i=1; i<size; i++)
                        {
                            var list = Communicator.world.Receive<List<Contact>>(i, i);
                            array.AddRange(list);
                        }
                        DateTime end = DateTime.Now;
                        TimeSpan ts = (end - start);
                        Console.WriteLine("Elapsed Time is {0} ms", ts.TotalMilliseconds);
                    }
                });
            }
        
    }

    

    [Serializable]
    public class Contact
    {
        public string companyname { get; set; }
        public string phone { get; set; }

        public Contact(string companyname, string phone)
        {
            this.companyname = companyname;
            this.phone = phone;
        }
    }

   


    
    public class Repository 
    {
        string connectionString = "Server=.\\SQLEXPRESS;Initial Catalog=TSQL2012; Encrypt=false; TrustServerCertificate=true; Integrated Security=True";
        public Repository()
        {
            
        }
        


        public List<Contact> GetContact(string query)
        {
            using (IDbConnection db = new SqlConnection(connectionString))
            {
                //return db.Query<Contact>("SELECT companyname, phone FROM Sales.Customers UNION SELECT companyname, phone FROM Sales.Customers").ToList();
                return db.Query<Contact>(query).ToList();
            }
        }

       

        public void Create(string query)
        {
            using (IDbConnection db = new SqlConnection(connectionString))
            {
                //var sqlQuery = "INSERT INTO Sales.Shippers(companyname, phone) VALUES(@companyname, @phone)";
                db.Execute(query);
            }
        }

        

        

    }
}