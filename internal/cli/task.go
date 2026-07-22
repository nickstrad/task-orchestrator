package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"text/tabwriter"

	"github.com/google/uuid"
	"github.com/nickstrad/task-orchestrator/internal/task"
	"github.com/spf13/cobra"
)

// exampleTaskEventJSON is shown in the start/run help so the expected shape is
// visible without opening a file. State is an integer enum: see stateLegend.
const exampleTaskEventJSON = `  {
    "State": 2,
    "Task": {
      "Name": "echo-server",
      "State": 1,
      "Image": "timboring/echo-server:latest",
      "ExposedPorts": { "7777/tcp": {} },
      "HealthCheck": "/health"
    }
  }`

const stateLegend = "task states: 0=Pending 1=Scheduled 2=Running 3=Completed 4=Failed"

func taskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "task",
		Short: "Submit, stop, and inspect tasks on a running manager",
		Long: `Client commands for driving tasks against a running manager (or a
worker directly — both expose the same /tasks endpoints). Each is a one-shot
HTTP call that exits as soon as it returns.`,
	}
	cmd.AddCommand(
		newStartCommand("start"),
		newStopCommand("stop"),
		newStatusCommand("status"),
	)
	return cmd
}

// newStartCommand builds the task-submission command. It is registered both as
// `task start` and, via run.go, as the top-level `run`, so the verb differs but
// the behavior is identical.
func newStartCommand(use string) *cobra.Command {
	var (
		tgt    target
		file   string
		dryRun bool
	)
	cmd := &cobra.Command{
		Use:   use + " -f FILE",
		Short: "Submit a task from a JSON or YAML file",
		Long: `Submit a task to the manager. The task event is read from a JSON or
YAML file (or stdin with -f -). Any of JSON's or YAML's field names work,
matched case-insensitively; omitted IDs are filled with fresh UUIDs.

` + stateLegend + `

Example JSON (see examples/task-event.json and examples/task-event.yaml):

` + exampleTaskEventJSON,
		Example: `  orchestrator ` + use + ` -f examples/task-event.json
  orchestrator ` + use + ` -f examples/task-event.yaml --port 5555
  cat task.json | orchestrator ` + use + ` -f -
  orchestrator ` + use + ` -f examples/task-event.json --dry-run   # print, don't send`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if file == "" {
				return fmt.Errorf("a task file is required: pass -f FILE (or -f - for stdin)")
			}
			te, err := loadTaskEvent(file)
			if err != nil {
				return err
			}
			return submitTaskEvent(tgt, te, dryRun, cmd.OutOrStdout())
		},
	}
	addTargetFlags(cmd, &tgt)
	cmd.Flags().StringVarP(&file, "file", "f", "", "task event file (JSON or YAML), or - for stdin")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print the task event that would be sent instead of sending it")
	return cmd
}

// newStopCommand builds the stop-by-id command, registered as both `task stop`
// and the top-level `stop`.
func newStopCommand(use string) *cobra.Command {
	var tgt target
	cmd := &cobra.Command{
		Use:   use + " TASK_ID",
		Short: "Stop a running task by id",
		Long: `Stop a task by id. The manager marks it Completed and schedules its
container to be stopped and removed. The id is the one printed when the task was
submitted, or shown by "orchestrator task status".`,
		Example: `  orchestrator ` + use + ` 4b1e9f0c-2a1e-4c3b-9d5f-6a7b8c9d0e1f`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := uuid.Parse(args[0])
			if err != nil {
				return fmt.Errorf("%q is not a valid task id: %w", args[0], err)
			}
			return stopTask(tgt, id, cmd.OutOrStdout())
		},
	}
	addTargetFlags(cmd, &tgt)
	return cmd
}

func newStatusCommand(use string) *cobra.Command {
	var tgt target
	cmd := &cobra.Command{
		Use:     use,
		Short:   "List tasks and their states",
		Long:    `List every task the manager knows about and its current state.`,
		Aliases: []string{"list", "ls"},
		Example: `  orchestrator task ` + use + ` --port 5555`,
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return printTaskStatus(tgt, cmd.OutOrStdout())
		},
	}
	addTargetFlags(cmd, &tgt)
	return cmd
}

// addTargetFlags attaches the --host/--port pair that points a client command
// at a manager or worker. They live on each leaf command so the same builder
// works whether it is nested under `task` or registered at the top level.
func addTargetFlags(cmd *cobra.Command, tgt *target) {
	cmd.Flags().StringVar(&tgt.host, "host", "localhost", "host of the manager (or worker) to talk to")
	cmd.Flags().IntVarP(&tgt.port, "port", "p", 5555, "port of the manager (or worker) to talk to")
}

func stopTask(tgt target, id uuid.UUID, out io.Writer) error {
	url := fmt.Sprintf("%s/%s", tgt.tasksURL(), id)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("building stop request: %w", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sending stop to %s: %w", url, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		fmt.Fprintf(out, "task %s scheduled to stop\n", id)
		return nil
	case http.StatusNotFound:
		return fmt.Errorf("task %s not found on the manager", id)
	default:
		return fmt.Errorf("stop rejected: %s", readAPIError(resp))
	}
}

func printTaskStatus(tgt target, out io.Writer) error {
	resp, err := httpClient.Get(tgt.tasksURL())
	if err != nil {
		return fmt.Errorf("listing tasks from %s: %w", tgt.tasksURL(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("could not list tasks: %s", readAPIError(resp))
	}

	var tasks []task.Task
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return fmt.Errorf("decoding task list: %w", err)
	}
	if len(tasks) == 0 {
		fmt.Fprintln(out, "no tasks")
		return nil
	}

	tw := tabwriter.NewWriter(out, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tNAME\tSTATE\tIMAGE\tCONTAINER")
	for _, t := range tasks {
		container := t.ContainerID
		if container == "" {
			container = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", t.ID, t.Name, t.State, t.Image, container)
	}
	return tw.Flush()
}
