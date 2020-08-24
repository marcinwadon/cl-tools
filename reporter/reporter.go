package reporter

import (
	"fmt"
	homedir "github.com/mitchellh/go-homedir"
	"os"
	"sync"
)

type Reporter interface {
	ReportS3Gap(height int64)
	ReportESGap(height int64)
}

type reporter struct {
	path string
	lock sync.RWMutex
}

func NewReporter() Reporter {
	dir, _ := homedir.Dir()

	return &reporter {
		path: dir + "/.cl",
		lock: sync.RWMutex{},
	}
}

func (r *reporter) ReportS3Gap(height int64) {
	msg := fmt.Sprintf("Height: %d\n", height)

	r.lock.Lock()
	f, _ := os.OpenFile(r.path + "/gaps_s3.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	_, _ = f.WriteString(msg)
	r.lock.Unlock()
}

func (r *reporter) ReportESGap(height int64) {
	msg := fmt.Sprintf("Height: %d\n", height)

	r.lock.Lock()
	f, _ := os.OpenFile(r.path + "/gaps_es.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	_, _ = f.WriteString(msg)
	r.lock.Unlock()
}
