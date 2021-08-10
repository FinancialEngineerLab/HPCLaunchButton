using Microsoft.Hpc.Scheduler;
using Microsoft.Hpc.Scheduler.Properties;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace JobSample
{
    class Program
    {
        public static string InputConnectionString = @"Data Source=SQL\DB;Initial Catalog=Input;Integrated Security=True;timeout=300000;";
        public static List<string> LoadRecordList(string type, string InputConnString)
        {
            List<string> RecordList = new List<string>();

            const string readRecordSql = @"select * from SampleInput.dbo.TB _TargetVol where code = ‘991’";
            //and FUND_CODE_01='V9101'
            // Making record's keys case insensitive.
           Dictionary<string, object> dbRecord = new Dictionary<string, object>();

            // Expecting to read exactly 1 record for this record ID.
            var recordsRead = 1;

            using (var conn = new SqlConnection(InputConnString))
            {
                conn.Open();
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = readRecordSql;
                    cmd.Parameters.AddWithValue("@type", type);
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            RecordList.Add(reader["poly_no"].ToString());
                        }
                    }
                }
            }

            return RecordList;
        }
        static ManualResetEvent running = new ManualResetEvent(false);
        static void Main(string[] args)
        {
            List<DateTime> dt = new List<DateTime>();
            List<string> greeks = new List<string>();


            dt.Add(new DateTime(2021, 7, 24));
            dt.Add(new DateTime(2021, 7, 27));
            dt.Add(new DateTime(2021, 7, 28));
            dt.Add(new DateTime(2021, 7, 29));
            dt.Add(new DateTime(2021, 7, 30));
            dt.Add(new DateTime(2021, 7, 31));
            dt.Add(new DateTime(2021, 8, 3));
            dt.Add(new DateTime(2021, 8, 4));
            dt.Add(new DateTime(2021, 8, 5));
            dt.Add(new DateTime(2021, 8, 6));
            dt.Add(new DateTime(2021, 8, 7));
            //dt.Add(new DateTime(2020, 8, 18));


            greeks.Add("false");
            greeks.Add("false");
            greeks.Add("false");
            greeks.Add("false");
            greeks.Add("false");
            greeks.Add("false");
            greeks.Add("false");
            greeks.Add("false");
            greeks.Add("false");
            greeks.Add("false");
            greeks.Add("false");
            greeks.Add("false");
            greeks.Add("false");
            greeks.Add("false");
            greeks.Add("false");
            greeks.Add("false");
            //greeks.Add("false");
            //greeks.Add("true");
            //greeks.Add("true");


            for (int j = 0; j < dt.Count-1; j++)
            {
                List<string> RecordList = LoadRecordList("1", InputConnectionString);
                List<string> RecordSet = new List<string>();

                for (int i = 0; i < RecordList.Count; i++)
                {
                    //if (i < 3256)
                        RecordSet.Add(RecordList[i]);
                    //else
                    //    RecordSet[i % 3256] = RecordSet[i % 3256] + "," + RecordList[i];
                }

                string clusterName = Environment.GetEnvironmentVariable("SCHEDULERsample1");
                string FilePath = @"C:\Run\run.exe";

                using (IScheduler scheduler = new Scheduler())
                {
                   Console.WriteLine("Connecting to {0}", clusterName);
                    scheduler.Connect(clusterName);

                    ISchedulerJob job = scheduler.CreateJob();
                    job.Name = "JOB TEST" + dt[j].ToShortDateString();
                    job.NodeGroups.Add("TARGETgroup");
                    job.UnitType = JobUnitType.Core;
                    
                    for (int i = 0; i < RecordSet.Count; i++)
                    {
                        ISchedulerTask task = job.CreateTask();
                        task.CommandLine = "cmd.exe /c " + FilePath + " /ValuationDate:" + dt[j].ToShortDateString()
                            + " /thetadate:" + dt[j+1].ToShortDateString()  //+1
                            + " /ir_marketdate:" + dt[j+1].ToShortDateString()  // +1
                            + " /eq_marketdate:" + dt[j+1].ToShortDateString() // +1
                            + " /greeks:" + greeks[j]
                            + " /RecordId:" + RecordSet[i];
                        task.Type = TaskType.Basic;
                        job.AddTask(task);
                    }
                    scheduler.SubmitJob(job, null, null);
                }

            }
        }
    }
}


