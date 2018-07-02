package goworker

type Payload struct {
	JobID string        `json:"jobid"`
	Class string        `json:"class"`
	Args  []interface{} `json:"args"`
}
