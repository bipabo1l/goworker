package goworker

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
	"github.com/garyburd/redigo/redis"
)

type worker struct {
	process
}

func newWorker(id string, queues []string) (*worker, error) {
	process, err := newProcess(id, queues)
	if err != nil {
		return nil, err
	}
	return &worker{
		process: *process,
	}, nil
}

func (w *worker) MarshalJSON() ([]byte, error) {
	return json.Marshal(w.String())
}

// 获取job info
//func (w *worker) GetJobInfo(jobId string) (*JobInfo, error) {
//	conn, err := GetConn()
//	if err != nil {
//		logger.Criticalf("Error on getting connection in worker %v: %v", w, err)
//		return nil, err
//	} else {
//		w.open(conn)
//		PutConn(conn)
//	}
//	jobInfoStr, err := redis.String(conn.Do("GET", fmt.Sprintf("%s:jobs:%s", workerSettings.Namespace, jobId)))
//	if err != nil {
//		logger.Criticalf("读取job状态失败")
//		return nil, err
//	}
//	jobInfo := &JobInfo{}
//	err = json.Unmarshal([]byte(jobInfoStr), jobInfo)
//	if err != nil {
//		return nil, err
//	}
//	return jobInfo, nil
//}

// 更新job
//func (w *worker)UpdateJobInfo(job *Job, jobInfo JobInfo) (error){
//
//	conn, err := GetConn()
//	if err != nil {
//		logger.Criticalf("Error on getting connection in worker %v: %v", w, err)
//		return  err
//	} else {
//		w.open(conn)
//		PutConn(conn)
//	}
//
//	jInfo, err := json.Marshal(jobInfo)
//	if err != nil {
//		return err
//	}
//	err = conn.Send("SET", fmt.Sprintf("%s:jobs:%s", workerSettings.Namespace, job.Payload.JobID), jInfo)
//	if err != nil {
//		logger.Criticalf("Update job failed")
//		return err
//	}
//
//	conn.Flush()
//	return nil
//}

func (w *worker) start(conn *RedisConn, job *Job) error {
	work := &work{
		Queue:   job.Queue,
		RunAt:   time.Now(),
		Payload: job.Payload,
	}

	buffer, err := json.Marshal(work)
	if err != nil {
		return err
	}

	conn.Send("SET", fmt.Sprintf("%sworker:%s", workerSettings.Namespace, w), buffer)
	logger.Debugf("Processing %s since %s [%v]", work.Queue, work.RunAt, work.Payload.Class)

	// 更新job状态
	//jobInfo, err := w.GetJobInfo(job.Payload.JobID)
	//if err != nil {
	//	logger.Criticalf("获取jobinfo失败:", err)
	//	return err
	//}
	//jobInfo.StartTime = time.Now().Unix()
	//jobInfo.State = "Started"
	//w.UpdateJobInfo(job, *jobInfo)
	return w.process.start(conn)
}



func (w *worker) fail(conn *RedisConn, job *Job, err error) error {
	failure := &failure{
		FailedAt:  time.Now(),
		Payload:   job.Payload,
		Exception: "Error",
		Error:     err.Error(),
		Worker:    w,
		Queue:     job.Queue,
	}
	buffer, err := json.Marshal(failure)
	if err != nil {
		return err
	}
	conn.Send("RPUSH", fmt.Sprintf("%sfailed", workerSettings.Namespace), buffer)

	// 更新job状态
	//jobInfo, err := w.GetJobInfo(job.Payload.JobID)
	//if err != nil {
	//	logger.Criticalf("获取jobinfo失败:", err)
	//	return err
	//}
	//jobInfo.EndTime = time.Now().Unix()
	//jobInfo.State = "Failed"
	//w.UpdateJobInfo(job, *jobInfo)

	return w.process.fail(conn)
}

func (w *worker) succeed(conn *RedisConn, job *Job) error {
	conn.Send("INCR", fmt.Sprintf("%sstat:processed", workerSettings.Namespace))
	conn.Send("INCR", fmt.Sprintf("%sstat:processed:%s", workerSettings.Namespace, w))

	// 更新job状态
	//jobInfo, err := w.GetJobInfo( job.Payload.JobID)
	//if err != nil {
	//	logger.Criticalf("获取jobinfo失败:", err)
	//	return err
	//}
	//jobInfo.EndTime = time.Now().Unix()
	//jobInfo.State = "Succeed"
	//w.UpdateJobInfo(job, *jobInfo)

	return nil
}

func (w *worker) finish(conn *RedisConn, job *Job, err error) error {
	if err != nil {
		w.fail(conn, job, err)
	} else {
		w.succeed(conn, job)
	}
	return w.process.finish(conn)
}

func (w *worker) work(jobs <-chan *Job, monitor *sync.WaitGroup) {
	conn, err := GetConn()
	if err != nil {
		logger.Criticalf("Error on getting connection in worker %v: %v", w, err)
		return
	} else {
		w.open(conn)
		PutConn(conn)
	}

	monitor.Add(1)

	go func() {
		defer func() {
			defer monitor.Done()

			conn, err := GetConn()
			if err != nil {
				logger.Criticalf("Error on getting connection in worker %v: %v", w, err)
				return
			} else {
				w.close(conn)
				PutConn(conn)
			}
		}()
		for job := range jobs {
			if workerFunc, ok := workers[job.Payload.Class]; ok {
				w.run(job, workerFunc)

				logger.Debugf("done: (Job{%s} | %s | %v)", job.Queue, job.Payload.Class, job.Payload.Args)
			} else {
				errorLog := fmt.Sprintf("No worker for %s in queue %s with args %v", job.Payload.Class, job.Queue, job.Payload.Args)
				logger.Critical(errorLog)

				conn, err := GetConn()
				if err != nil {
					logger.Criticalf("Error on getting connection in worker %v: %v", w, err)
					return
				} else {
					w.finish(conn, job, errors.New(errorLog))
					PutConn(conn)
				}
			}
		}
	}()
}

func (w *worker) run(job *Job, workerFunc workerFunc) {
	var err error
	defer func() {
		conn, errCon := GetConn()
		if errCon != nil {
			logger.Criticalf("Error on getting connection in worker on finish %v: %v", w, errCon)
			return
		} else {
			w.finish(conn, job, err)
			PutConn(conn)
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprint(r))
		}
	}()

	conn, err := GetConn()
	if err != nil {
		logger.Criticalf("Error on getting connection in worker on start %v: %v", w, err)
		return
	} else {
		w.start(conn, job)
		PutConn(conn)
	}
	err = workerFunc(job.Queue, job.Payload.Args...)
}