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
        public static string InputConnectionString = @"Data Source=LHSQL\LHDB;Initial Catalog=Input;Integrated Security=True;timeout=0;";
 
        public static void SetInforce(string desc)
        {
 
            using (var conn = new SqlConnection(InputConnectionString))
            {
                conn.Open();
                
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = conn;
                cmd.CommandText = sql;
                cmd.CommandTimeout = 0;
                try
                {
                    cmd.ExecuteNonQuery();
                }
                catch (SqlException e)
                {
                    Console.WriteLine(e.Message);
                }
               
            }
        }
 
        public static List<string> LoadRecordList(string type, string InputConnString)
        {
            List<string> RecordList = new List<string>();
            const string readRecordSql = @"SELECT * FROM Input.dbo.TB1 WHERE blok = '1' aND FUND_CODE_01='VolTarget";
            Dictionary<string, object> dbRecord = new Dictionary<string, object>();
            
 
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
                            RecordList.Add(reader["id"].ToString());
                        }
                    }
                }
            }
            return RecordList;
        }
        static ManualResetEvent running = new ManualResetEvent(false);
 
        static void WaitForJob(IScheduler scheduler, ISchedulerJob job)
        {
            const JobState exitStates = JobState.Finished | JobState.Failed | JobState.Canceled;
 
            ManualResetEvent checkJobState = new ManualResetEvent(false);
 
            // Event handler for when the job state changes
            EventHandler<JobStateEventArg> jobStatusCheck = (sender, e) =>
            {
                Console.WriteLine(String.Format("  Job {0} state is now {1}.", job.Id, e.NewState));
                if ((e.NewState & exitStates) != 0)
                    checkJobState.Set();
            };
 
            // Event handler for when the eventing channel gets reconnected after a failure
            EventHandler<ConnectionEventArg> schedulerConnectionEvent = (sender, e) =>
            {
                if (e.Code == ConnectionEventCode.EventReconnect)
                {
                    Console.WriteLine("  Reconnect event detected");
                    //signal the thread to recheck the job state since the job state event may have been missed
                    // while we were disconnected.
                    checkJobState.Set();
                }
                else
               {
                    Console.WriteLine(String.Format("  schedulerConnectionEvent {0}.", e.Code));
                }
            };
 
            Console.WriteLine(String.Format("Waiting for job {0}...", job.Id));
 
            // Register event handlers before checkJobState is Reset
            job.OnJobState += jobStatusCheck;
            scheduler.OnSchedulerReconnect += schedulerConnectionEvent;
 
            try
            {
                do
                {
                   checkJobState.Reset();  // Always Reset before job.Refresh to avoid losing state transitions
                    job.Refresh();
                    if ((job.State & exitStates) != 0)
                    {
                        Console.WriteLine(String.Format("Job {0} completed with state {1}.", job.Id, job.State));
                        return;
                    }
 
                    checkJobState.WaitOne();
                } while (true);
            }
            finally
            {
                // must unregester handlers using the same job and scheduler objects that were used to register them above
                // see comment "Register event handlers"
                job.OnJobState -= jobStatusCheck;
                scheduler.OnSchedulerReconnect -= schedulerConnectionEvent;
            }
 
        }
        static void Main(string[] args)
        {
         
            string clusterName = Environment..GetEnvironmentVariable("SCHEDULER");
 
            List<string> valuationdates = new List<string>();
            List<string> VFUNDPARAMS = new List<string>();
            List<string> inputmaster = new List<string>();
 
      
            valuationdates.Add("2020-02-24");
            valuationdates.Add("2020-02-25");
            valuationdates.Add("2020-02-26");
            valuationdates.Add("2020-02-27");
            valuationdates.Add("2020-02-28");
            valuationdates.Add("2020-03-02");
            valuationdates.Add("2020-03-03");
           valuationdates.Add("2020-03-04");
            valuationdates.Add("2020-03-05");
            valuationdates.Add("2020-03-06");
            valuationdates.Add("2020-03-09");
            valuationdates.Add("2020-03-10");
            valuationdates.Add("2020-03-11");
            valuationdates.Add("2020-03-12");
            valuationdates.Add("2020-03-13");
            valuationdates.Add("2020-03-16");
            valuationdates.Add("2020-03-17");
            valuationdates.Add("2020-03-18");
            valuationdates.Add("2020-03-19");
            valuationdates.Add("2020-03-20");
            valuationdates.Add("2020-03-23");
            valuationdates.Add("2020-03-24");
            valuationdates.Add("2020-03-25");
            valuationdates.Add("2020-03-26");
            valuationdates.Add("2020-03-27");
            valuationdates.Add("2020-03-30");
            valuationdates.Add("2020-03-31");
            valuationdates.Add("2020-04-01");
            valuationdates.Add("2020-04-02");
            valuationdates.Add("2020-04-03");
            valuationdates.Add("2020-04-06");
            valuationdates.Add("2020-04-07");
            valuationdates.Add("2020-04-08");
            valuationdates.Add("2020-04-09");
            valuationdates.Add("2020-04-10");
            valuationdates.Add("2020-04-13");
            valuationdates.Add("2020-04-14");
            valuationdates.Add("2020-04-16");
            valuationdates.Add("2020-04-17");
            valuationdates.Add("2020-04-20");
            valuationdates.Add("2020-04-21");
            valuationdates.Add("2020-04-22");
            valuationdates.Add("2020-04-23");
            valuationdates.Add("2020-04-24");
            valuationdates.Add("2020-04-25");
            valuationdates.Add("2020-04-27");
            valuationdates.Add("2020-04-28");
            valuationdates.Add("2020-04-29");
            valuationdates.Add("2020-05-04");
            valuationdates.Add("2020-05-06");
            valuationdates.Add("2020-05-07");
            valuationdates.Add("2020-05-08");
            valuationdates.Add("2020-05-11");
            valuationdates.Add("2020-05-12");
            valuationdates.Add("2020-05-13");
            valuationdates.Add("2020-05-14");
            valuationdates.Add("2020-05-15");
            valuationdates.Add("2020-05-18");
            valuationdates.Add("2020-05-19");
            valuationdates.Add("2020-05-20");
            valuationdates.Add("2020-05-21");
            valuationdates.Add("2020-05-22");
            valuationdates.Add("2020-05-25");
            valuationdates.Add("2020-05-26");
            valuationdates.Add("2020-05-27");
            valuationdates.Add("2020-05-28");
            valuationdates.Add("2020-05-29");
            valuationdates.Add("2020-06-01");
            valuationdates.Add("2020-06-02");
            valuationdates.Add("2020-06-03");
            valuationdates.Add("2020-06-04");
            valuationdates.Add("2020-06-05");
            valuationdates.Add("2020-06-08");
            valuationdates.Add("2020-06-09");
            valuationdates.Add("2020-06-10");
            valuationdates.Add("2020-06-11");
            valuationdates.Add("2020-06-12");
            valuationdates.Add("2020-06-15");
            valuationdates.Add("2020-06-16");
            valuationdates.Add("2020-06-17");
            valuationdates.Add("2020-06-18");
            valuationdates.Add("2020-06-19");
            valuationdates.Add("2020-06-22");
            valuationdates.Add("2020-06-23");
            valuationdates.Add("2020-06-24");
 
 
 
            for (int j = 18; j < valuationdates.Count; j++)
            {
                SetInforce(inputmaster[j]);
 
                List<string> RecordList = LoadRecordList("1", InputConnectionString);
                List<string> RecordSet = new List<string>();
 
                int num = 44 * 74 * 7;
 
                if (RecordList.Count < num)
                    num = RecordList.Count;
 
                for (int i = 0; i < RecordList.Count; i++)
                {
                    if (i < num)
                        RecordSet.Add("'" + RecordList[i] + "'");
                    else
                        RecordSet[i % num] = RecordSet[i % num] + ",'" + RecordList[i] + "'";
                }
 
 
                using (IScheduler scheduler = new Scheduler())
                {
                    scheduler.Connect(clusterName);
 
                    ISchedulerJob job = scheduler.CreateJob();
                    job.Name = "TargetVol_" + valuationdates[j] + "_" + VFUNDPARAMS[j];
                    job.UnitType = JobUnitType.Core;
                    job.NodeGroups.Add("LHVH");
                    job.Priority = JobPriority.Lowest;
 
                    for (int i = 0; i < num; i++)
                    {
                        ISchedulerTask task = job.CreateTask();
                        task.Name = "TargetVol";
                        task.CommandLine = "Run.exe " + "/RecordIds:" + RecordSet[i] + " /ValuationDate:" + valuationdates[j] + " /VFUNDPARAMS:" + VFUNDPARAMS[j];
                        task.WorkDirectory = @"C:\Run\";
                        job.AddTask(task);
                    }
                    scheduler.SubmitJob(job, null, null);
                    WaitForJob(scheduler, job);
                }
            }
        }
    }
}
