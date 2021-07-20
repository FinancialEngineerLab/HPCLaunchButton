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
        public static string InputConnectionString = @"Data Source=SQL\DB;Initial Catalog=Input;Integrated Security=True;timeout=0;";
        public static List<string> LoadRecordList(string type, string InputConnString)
        {
            List<string> RecordList = new List<string>();

            const string readRecordSql = @"select * from Input.dbo.TB where BLOK = @type AND CODE_01='111'";
            // Making record's keys case insensitive.
            Dictionary<string, object> dbRecord = new Dictionary<string, object>();
            // Expecting to read exactly 1 record for this record ID.

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
                            RecordList.Add(reader["ID"].ToString());
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
            List<string> RecordList = LoadRecordList("1", InputConnectionString);
            List<string> RecordSet = new List<string>();

            int num = 44 * 74;

            for (int i = 0; i < RecordList.Count; i++)
            {
                if (i < num)
                    RecordSet.Add("'" + RecordList[i] + "'");
                else
                    RecordSet[i % num] = RecordSet[i % num] + ",'" + RecordList[i] + "'";
            }

            string clusterName = Environment.GetEnvironmentVariable("CCP_SCHEDULER");


            List<string> valuationdates = new List<string>();

            valuationdates.Add("2021-06-28");
            valuationdates.Add("2021-06-29");
            valuationdates.Add("2021-06-30");


            for (int j = 0; j < valuationdates.Count; j++)
            {
                using (IScheduler scheduler = new Scheduler())
                {
                    scheduler.Connect(clusterName);

                    ISchedulerJob job = scheduler.CreateJob();
                    job.Name = "vvv";
                    job.UnitType = JobUnitType.Core;

                    for (int i = 0; i < num; i++)
                    {
                        ISchedulerTask task = job.CreateTask();
                        task.Name = "vvv";
                        task.CommandLine = "Project01.exe " + "/RecordIds:" + RecordSet[i] + " /ValuationDate:" + valuationdates[j];
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
