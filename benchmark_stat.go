package paxi

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"sort"
	"time"
)

// Stat stores the statistics data for benchmarking results
type Stat struct {
	Data   []float64
	Size   int
	Mean   float64
	StdDev float64
	Min    float64
	Max    float64
	Median float64
	P95    float64
	P99    float64
	P999   float64
}

// WriteFile writes stat to new file in path
func (s Stat) WriteFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range s.Data {
		fmt.Fprintln(w, line)
	}
	return w.Flush()
}

func (s Stat) String() string {
	return fmt.Sprintf("size = %d\nmean = %f\nstddev = %f\nmin = %f\nmax = %f\nmedian = %f\np95 = %f\np99 = %f\np999 = %f\n", s.Size, s.Mean, s.StdDev, s.Min, s.Max, s.Median, s.P95, s.P99, s.P999)
}

// Statistic function creates Stat object from raw latency data
func Statistic(latency []time.Duration) Stat {
	ms := make([]float64, 0)
	if len(latency) == 0 {
		return Stat{
			Data:   ms,
			Size:   0,
			Mean:   0.0,
			StdDev: 0.0,
			Min:    0.0,
			Max:    0.0,
			Median: 0.0,
			P95:    0.0,
			P99:    0.0,
			P999:   0.0,
		}
	}

	for _, l := range latency {
		ms = append(ms, float64(l.Nanoseconds())/1000000.0)
	}
	sort.Float64s(ms)
	sum := 0.0
	for _, m := range ms {
		sum += m
	}
	size := len(ms)
	stat := Stat{
		Data:   ms,
		Size:   size,
		Mean:   sum / float64(size),
		StdDev: 0.0,
		Min:    ms[0],
		Max:    ms[size-1],
		Median: ms[int(0.5*float64(size))],
		P95:    ms[int(0.95*float64(size))],
		P99:    ms[int(0.99*float64(size))],
		P999:   ms[int(0.999*float64(size))],
	}
	sumAbsSq := 0.0
	for _, m := range ms {
		x := math.Abs(m - stat.Mean)
		sumAbsSq += math.Pow(x, 2)
	}
	stat.StdDev = math.Sqrt(sumAbsSq/float64(size))
	return stat
}
