package cli

// dbTypeUsage is the shared help string for the --db-type flag on the worker
// and manager commands, so the two never drift out of sync. The values are
// validated by store.GetDBs when the worker/manager is built, not here.
const dbTypeUsage = "store backend: IN_MEMORY (lost on exit) or PERSISTENT (bbolt file)"
