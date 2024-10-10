using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Conductor.Client.Interfaces;
using Conductor.Client.Models;
using Conductor.Client.Worker;
using Task = Conductor.Client.Models.Task;

namespace ConductorPollingTest
{
    internal class ForkWorker : IWorkflowTask
    {
        public async Task<TaskResult> Execute(Task task, CancellationToken token = default)
        {
            var result = new TaskResult();


            var tasks = Enumerable.Range(0, 12).Select(n => new
            {
                name = "TEST_subworkflow",
                taskReferenceName = n.ToString(),
                type = "SUB_WORKFLOW",
                subWorkflowParam = new
                {
                    name = "TEST_SUBWORKFLOW",
                    version = 1
                }

            }).ToArray();
            var inputs = Enumerable.Range(0, 12).ToDictionary(n => n.ToString(), n => new { n = n.ToString() });

            result.Status = TaskResult.StatusEnum.COMPLETED;
            result.OutputData = new()
            {
                { "dynamic_tasks", tasks},
                { "dynamic_tasks_i", inputs}
            };

            return result;
        }

        public TaskResult Execute(Task task)
        {
            throw new NotImplementedException();
        }


        public string TaskType => "TEST_fork";

        public WorkflowTaskExecutorConfiguration WorkerSettings { get;}
    }
}
