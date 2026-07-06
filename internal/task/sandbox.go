package task

type Sandbox interface {
	Run() *SandboxResult
	Stop() *SandboxResult
}

type SandboxResult struct {
	Error  TaskError
	Action string
	Result string
}
