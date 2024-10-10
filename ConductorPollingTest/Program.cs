using Conductor.Client;
using Conductor.Client.Models;
using Task = System.Threading.Tasks.Task;

var configuration = new Configuration()
{
    BasePath = "http://localhost:8080/api"
};

var workflowClient = configuration.GetClient<Conductor.Api.WorkflowResourceApi>();
var taskClient = configuration.GetClient<Conductor.Api.TaskResourceApi>();
var metadataClient = configuration.GetClient<Conductor.Api.MetadataResourceApi>();

await metadataClient.RegisterTaskDefAsync([
    new()
    {
        Name = "TEST_fork",
        OwnerEmail = "undefined@undefined.com"
    },
    new()
    {
        Name = "TEST_worker",
        OwnerEmail = "undefined@undefined.com"
    }
]);

await metadataClient.UpdateWorkflowDefinitionsAsync([
    new(name: "TEST_subworkflow", tasks: [
        new(name: "TEST_worker", taskReferenceName: "test_worker")
        {
            Type = "SIMPLE"
        }
    ], timeoutSeconds: 60)
    {
        OwnerEmail = "undefined@undefined.com",
        Version = 1
    },
    new(name: "TEST_workflow", tasks: [
        new(name: "TEST_fork", taskReferenceName: "test_fork")
        {
            Type = "SIMPLE",
        },
        new(name: "FORK_JOIN_DYNAMIC", taskReferenceName: "fork")
        {
            InputParameters = new()
            {
                { "dynamic_tasks", "${test_fork.output.dynamic_tasks}" },
                { "dynamic_tasks_i", "${test_fork.output.dynamic_tasks_i}" },
            },
            Type = "FORK_JOIN_DYNAMIC",
            DynamicForkTasksParam = "dynamic_tasks",
            DynamicForkTasksInputParamName = "dynamic_tasks_i",
        },
        new(name: "JOIN", taskReferenceName: "JOIN_fork")
        {
            Type = "JOIN"
        }
    ], timeoutSeconds: 60)
    {
        OwnerEmail = "undefined@undefined.com",
        Version = 1
    }
]);



_ = Task.Run(async () =>
{

    while (true)
    {
        var key = Console.ReadKey(true);

        if (key.Key == ConsoleKey.Enter)
        {
            Console.WriteLine("Starting workflow");

            await workflowClient.StartWorkflowAsync(new()
            {
                Name = "TEST_workflow",
                Version = 1
            });
        }
    }
});


while (true)
{
    var queues = await taskClient.AllAsync();
    var testWorkerQueueCount = queues.GetValueOrDefault("TEST_worker", 0);
    var testForkQueueCount = queues.GetValueOrDefault("TEST_fork", 0);

    Console.WriteLine(testWorkerQueueCount);

    if (testForkQueueCount > 0)
    {
        taskClient.PollAsync("TEST_fork").ContinueWith(async response =>
        {
            var polledTask = await response;

            if (polledTask == null)
                return;

            var tasks = Enumerable.Range(0, 12).Select(n => new
            {
                name = "TEST_subworkflow",
                taskReferenceName = n.ToString(),
                type = "SUB_WORKFLOW",
                subWorkflowParam = new
                {
                    name = "TEST_subworkflow",
                    version = 1
                }

            }).ToArray();
            var inputs = Enumerable.Range(0, 12).ToDictionary(n => n.ToString(), n => new { n = n.ToString() });

            await taskClient.UpdateTaskAsync(new()
            {
                WorkflowInstanceId = polledTask.WorkflowInstanceId,
                TaskId = polledTask.TaskId,
                Status = TaskResult.StatusEnum.COMPLETED,
                OutputData = new()
                {
                    { "dynamic_tasks", tasks},
                    { "dynamic_tasks_i", inputs}
                }
            });
        });
    }

    if (testWorkerQueueCount > 0)
    {
        taskClient.PollAsync("TEST_worker").ContinueWith(async response =>
        {
            var polledTask = await response;
            
            if (polledTask == null)
                return;


            Console.WriteLine("Polled TEST_worker");
            await taskClient.UpdateTaskAsync(new()
            {
                WorkflowInstanceId = polledTask.WorkflowInstanceId,
                TaskId = polledTask.TaskId,
                Status = TaskResult.StatusEnum.COMPLETED
            });

            Console.WriteLine("Completed TEST_worker");
        });
    }

    await Task.Delay(200);
}
