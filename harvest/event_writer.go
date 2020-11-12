package harvest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"go_harvest/service"
	"time"
)

type EventWriter struct {
	eventMessage        EventMessage
	useMQEventOutWriter bool
	outputMQ            service.MQ
}

type EventMessage struct {
	JobName      string
	JobUid       string
	JobStatus    string
	JobStartTime time.Time
	JobInfo      string
}

func (e EventWriter) sendEvent(status, info string) error {
	e.eventMessage.JobStatus = status
	e.eventMessage.JobInfo = info
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(e.eventMessage)
	if err != nil {
		logrus.Errorf("encode err:%s", err.Error())
		return err
	}
	if e.useMQEventOutWriter {
		if e.outputMQ == nil {
			return errors.New("mq is nil,please init first")
		}
		err := e.outputMQ.SendEventMessage(fmt.Sprintf("%s-%s", e.eventMessage.JobName, e.eventMessage.JobUid), buf.Bytes())
		if err != nil {
			return err
		}
	} else {
		logrus.Infof("【JOB EVENT】[NAME]:%s  [STATUS]:%s  [INFO]:%s", e.eventMessage.JobName, e.eventMessage.JobStatus, e.eventMessage.JobInfo)
	}

	return err
}
