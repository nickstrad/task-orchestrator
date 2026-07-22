package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/spf13/cobra"
)

// statsCmd reads a worker's collected metrics. It targets a worker rather than
// the manager, so it defaults to the worker port and hits /stats directly.
func statsCmd() *cobra.Command {
	var (
		host string
		port int
	)
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Show a worker's collected stats",
		Long: `Fetch and print the stats a worker has collected (memory, cpu, disk,
task count). Point --host/--port at the worker, not the manager. A worker that
has not finished its first collection pass yet returns "stats not collected
yet".`,
		Example: `  orchestrator stats --port 5556`,
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			url := fmt.Sprintf("http://%s:%d/stats", host, port)
			resp, err := httpClient.Get(url)
			if err != nil {
				return fmt.Errorf("fetching stats from %s: %w", url, err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("could not get stats: %s", readAPIError(resp))
			}

			// Print the payload verbatim (pretty-printed) rather than coupling
			// the CLI to worker.Stats' internal shape.
			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("reading stats: %w", err)
			}
			var pretty bytes.Buffer
			if err := json.Indent(&pretty, raw, "", "  "); err != nil {
				return fmt.Errorf("formatting stats: %w", err)
			}
			fmt.Fprintln(cmd.OutOrStdout(), pretty.String())
			return nil
		},
	}
	cmd.Flags().StringVar(&host, "host", "localhost", "host of the worker to talk to")
	cmd.Flags().IntVarP(&port, "port", "p", 5556, "port of the worker to talk to")
	return cmd
}
