package types

type Status struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

type Cluster struct {
	Version        string `json:"version"`
	Commit         string `json:"commit"`
	KafkaClusterId string `json:"kafka_cluster_id"`
}

type Connectors []string

type ConnectorConfig map[string]string

type Connector struct {
	Name            string            `json:"name"`
	ConnectorConfig map[string]string `json:"config"`
}

type ConnectorStatus struct {
	Name      string `json:"name"`
	Connector struct {
		State    string `json:"state"`
		WorkerId string `json:"worker_id"`
	} `json:"connector"`
	Tasks []struct {
		Id       int    `json:"id"`
		State    string `json:"state"`
		WorkerId string `json:"worker_id"`
	} `json:"tasks"`
	Type string `json:"type"`
}

type Tasks []Task

type Task struct {
	Id struct {
		Connector string `json:"connector"`
		Task      int    `json:"task"`
	} `json:"id"`
	Config map[string]string `json:"config"`
}

type TaskStatus struct {
	State    string `json:"state"`
	Id       int    `json:"id"`
	WorkerId string `json:"worker_id"`
}
