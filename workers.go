package goworker

import (
	"encoding/json"
	"fmt"
	"github.com/satori/go.uuid"
	"time"
)

var (
	workers map[string]workerFunc
)

func init() {
	workers = make(map[string]workerFunc)
}

// Register registers a goworker worker function. Class
// refers to the Ruby name of the class which enqueues the
// job. Worker is a function which accepts a queue and an
// arbitrary array of interfaces as arguments.
func Register(class string, worker workerFunc) {
	workers[class] = worker
}

func GenerateJobID(job *Job, id string) error {
	if id != "" {
		u, err := uuid.NewV4()
		if err != nil {
			return err
		}
		job.Payload.JobID = u.String()
	} else {
		job.Payload.JobID = id
	}

	return nil
}

// 添加到队列
func Enqueue(job *Job, isPriority bool, uuid string) (*JobInfo, error) {

	GenerateJobID(job, uuid)

	err := Init()
	if err != nil {
		return nil, err
	}
	conn, err := GetConn()
	if err != nil {
		logger.Criticalf("Error on getting connection on enqueue")
		return nil, err
	}
	defer PutConn(conn)

	buffer, err := json.Marshal(job.Payload)
	if err != nil {
		logger.Criticalf("Cant marshal payload on enqueue")
		return nil, err
	}

	// 支持优先队列
	command := "RPUSH"
	if isPriority == true {
		command = "LPUSH"
	}
	err = conn.Send(command, fmt.Sprintf("%squeue:%s", workerSettings.Namespace, job.Queue), buffer)
	if err != nil {
		logger.Criticalf("Cant push to queue")
		return nil, err
	}

	err = conn.Send("SADD", fmt.Sprintf("%squeues", workerSettings.Namespace), job.Queue)
	if err != nil {
		logger.Criticalf("Cant register queue to list of use queues")
		return nil, err
	}

	// 记录jobid
	err = conn.Send("SADD", fmt.Sprintf("%s:jobs", workerSettings.Namespace), job.Payload.JobID)
	if err != nil {
		logger.Criticalf("Cant register jobs to list of use queues")
		return nil, err
	}

	// 记录jobinfo
	jobInfo := &JobInfo{
		JobId:      job.Payload.JobID,
		CreateTime: time.Now().Unix(),
		State:      "Pending",
	}
	jobInfoStr, err := json.Marshal(jobInfo)
	err = conn.Send("SET", fmt.Sprintf("%s:jobs:%s", workerSettings.Namespace, job.Payload.JobID), jobInfoStr)
	if err != nil {
		logger.Criticalf("Cant register job info to list of use queues")
		return nil, err
	}
	return jobInfo, conn.Flush()
}

// 根据队列名获取任务数量
func GetJobsNumberByQueueName(queueName string) (int64, error) {
	err := Init()
	if err != nil {
		return 0, err
	}

	conn, err := GetConn()
	if err != nil {
		logger.Criticalf("Error on getting connection on enqueue")
		return 0, err
	}
	defer PutConn(conn)

	err = conn.Send("LLEN", fmt.Sprintf("%squeue:%s", workerSettings.Namespace, queueName))
	if err != nil {
		logger.Criticalf("Cant get llen queue")
		return 0, err
	}
	conn.Flush()
	jobsCount, err := conn.Receive() // reply from GET

	return jobsCount.(int64), nil
}
