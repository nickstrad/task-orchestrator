package node

type Node struct {
	Name            string
	API             string
	Cores           int
	Memory          int
	MemoryAllocated int
	Disk            int
	DiskAllocated   int
	Role            string
	TaskCount       int
}

func NewNode(name, addr, role string) Node {
	return Node{
		Name: name,
		API:  addr,
		Role: role,
	}
}
