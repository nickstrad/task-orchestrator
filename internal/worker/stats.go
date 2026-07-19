package worker

import (
	"log/slog"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
)

type Stats struct {
	MemStats           *mem.VirtualMemoryStat
	DiskStats          *disk.UsageStat
	CPUUsagePercentage float64
	LoadStats          *load.AvgStat
	TaskCount          int
}

func NewStats(logger *slog.Logger) *Stats {
	memStats, err := mem.VirtualMemory()
	if err != nil {
		logger.Warn("unable to get mem stats for worker", "err", err)
		memStats = &mem.VirtualMemoryStat{}
	}
	diskStats, err := disk.Usage("/")
	if err != nil {
		logger.Warn("unable to get disk stats for worker", "err", err)
		diskStats = &disk.UsageStat{}
	}
	// percpu=false → one aggregate value; interval=0 → non-blocking,
	// reports usage since the previous Percent call (since boot on first call).
	cpuPercents, err := cpu.Percent(0, false)
	cpuPercent := -1.0
	if len(cpuPercents) > 0 {
		cpuPercent = cpuPercents[0]
	}

	if err != nil {
		logger.Warn("unable to get cpu usage percentage for worker", "err", err)
	}

	loadStats, err := load.Avg()
	if err != nil {
		logger.Warn("unable to get load stats for worker", "err", err)
		loadStats = &load.AvgStat{}
	}

	return &Stats{
		MemStats:           memStats,
		DiskStats:          diskStats,
		CPUUsagePercentage: cpuPercent,
		LoadStats:          loadStats,
	}
}

func (s *Stats) MemTotalKb() uint64 {
	return s.MemStats.Total
}

func (s *Stats) MemAvailableKb() uint64 {
	return s.MemStats.Available
}

func (s *Stats) MemUsedKb() uint64 {
	return s.MemStats.Total - s.MemStats.Available
}

func (s *Stats) MemUsedPercent() uint64 {
	return s.MemStats.Available / s.MemStats.Total
}

func (s *Stats) DiskTotal() uint64 {
	return s.DiskStats.Total
}
func (s *Stats) DiskFree() uint64 {
	return s.DiskStats.Free
}
func (s *Stats) DiskUsed() uint64 {
	return s.DiskStats.Used
}

func (s *Stats) CPUUsage() float64 {
	return s.CPUUsagePercentage
}
