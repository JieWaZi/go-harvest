package harvest

import (
	"testing"
)

func TestNewJob(t *testing.T) {
	job:= NewJob("haha")
	job.Start()
}