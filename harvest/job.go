package harvest

import (
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"go_harvest/service"
	"time"
)

type Job struct {
	name       string      `json:"jobName"`
	uid        string      `json:"jobUid"`
	status     string      `json:"jobStatus"`
	jobOptions *JobOptions `json:"-"`
	startTime  time.Time   `json:"startTime"`

	executor        Executor
	executorContext *ExecutorContext

	eventWriter EventWriter
}

type JobOptions struct {
	MQ                  service.MQ
	DB                  service.DB
	UseMQEventOutWriter bool
}

type JobOption func(*JobOptions)

func NewJob(jobName string, options ...JobOption) Job {
	defaultOption := &JobOptions{
		MQ:                  nil,
		DB:                  nil,
		UseMQEventOutWriter: false,
	}
	for _, option := range options {
		option(defaultOption)
	}

	uid := uuid.New().String()
	startTime := time.Now()

	return Job{
		name:       jobName,
		uid:        uid,
		startTime:  startTime,
		jobOptions: defaultOption,
		eventWriter: EventWriter{
			eventMessage: EventMessage{
				JobName:      jobName,
				JobUid:       uid,
				JobStartTime: startTime,
			},
			useMQEventOutWriter: defaultOption.UseMQEventOutWriter,
			outputMQ:            defaultOption.MQ,
		},
	}
}

func WithKafka(kafka service.Kafka) JobOption {
	return func(o *JobOptions) {
		o.MQ = &kafka
	}
}

func WithMySQL(mysql service.MySQL) JobOption {
	return func(o *JobOptions) {
		o.DB = mysql
	}
}

func WithMQEventOutWriter() JobOption {
	return func(o *JobOptions) {
		o.UseMQEventOutWriter = true
	}
}

func (j *Job) optionInit() error {
	// init DB
	if j.jobOptions.DB != nil {
		err := j.jobOptions.DB.Init()
		if err != nil {
			logrus.Errorf("init db err:%s", err.Error())
			return err
		}

	}
	// init MQ
	if j.jobOptions.MQ != nil {
		err := j.jobOptions.MQ.Init()
		if err != nil {
			logrus.Errorf("init mq err:%s", err.Error())
			return err
		}
		j.eventWriter.outputMQ = j.jobOptions.MQ
		j.executorContext.outputMQ = j.jobOptions.MQ
	}

	return nil
}

func (j *Job) SetExecutor(executor Executor) {
	j.executor = executor
}

func (j *Job) Start() error {
	var err error
	j.executorContext, err = newExecutorContext(j.executor, j.eventWriter)
	if err != nil {
		return err
	}
	err = j.optionInit()
	if err != nil {
		return err
	}

	err = j.executorContext.startExecutor()
	if err != nil {
		return err
	}

	return nil
}
