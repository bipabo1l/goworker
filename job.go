package goworker

type Job struct {
	Queue   string
	Payload Payload
}

type JobInfo struct {
	JobId      string `json:"JobId"`
	Property   string `json:"Property"`
	CreateTime int64  `json:"CreateTime"`
	StartTime  int64  `json:"StartTime"`
	EndTime    int64  `json:"EndTime"`
	State      string `json:"State"`
}
