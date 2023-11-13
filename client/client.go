package client

import (
	"bytes"
	"encoding/json"
	"github.com/eskaton/kafka-connect-client/types"
	"io"
	"net/http"
	"strconv"
)

type Client struct {
	url string
}

type Status struct {
	Status        string
	StatusCode    int
	ServerMessage string
}

func New(url string) *Client {
	return &Client{url: url}
}

func (client *Client) Cluster() (*types.Cluster, *Status, error) {
	return get[types.Cluster](client.url)
}

func (client *Client) Connectors() (*types.Connectors, *Status, error) {
	return get[types.Connectors](client.url + "/connectors")
}

func (client *Client) Connector(name string) (*types.Connector, *Status, error) {
	return get[types.Connector](client.url + "/connectors/" + name)
}

func (client *Client) ConnectorStatus(name string) (*types.ConnectorStatus, *Status, error) {
	return get[types.ConnectorStatus](client.url + "/connectors/" + name + "/status")
}

func (client *Client) ConnectorPause(name string) (*Status, error) {
	_, status, err := put[any, any](client.url+"/connectors/"+name+"/pause", []byte{})

	return status, err
}

func (client *Client) ConnectorResume(name string) (*Status, error) {
	_, status, err := put[any, any](client.url+"/connectors/"+name+"/resume", []byte{})

	return status, err
}

func (client *Client) ConnectorRestart(name string, includeTasks bool, onlyFailed bool) (*types.ConnectorStatus, *Status, error) {
	url := client.url + "/connectors/" + name + "/restart?includeTasks=" + strconv.FormatBool(includeTasks) + "&onlyFailed=" + strconv.FormatBool(onlyFailed)

	return post[types.ConnectorStatus](url, []byte{})
}

func (client *Client) ConnectorDelete(name string) (*Status, error) {
	return remove(client.url + "/connectors/" + name)
}

func (client *Client) ConnectorCreate(connector types.Connector) (*types.Connector, *Status, error) {
	content, err := json.Marshal(connector)

	if err != nil {
		return nil, nil, err
	}

	return post[types.Connector](client.url+"/connectors/", content)
}

func (client *Client) ConnectorUpdateConfig(name string, config types.ConnectorConfig) (*Status, error) {
	content, err := json.Marshal(config)

	if err != nil {
		return nil, err
	}

	_, status, err := put[types.ConnectorConfig, types.Connector](client.url+"/connectors/"+name+"/config", content)

	return status, err
}

func (client *Client) Tasks(name string) (*types.Tasks, *Status, error) {
	return get[types.Tasks](client.url + "/connectors/" + name + "/tasks")
}

func (client *Client) TaskStatus(name string, task string) (*types.TaskStatus, *Status, error) {
	return get[types.TaskStatus](client.url + "/connectors/" + name + "/tasks/" + task + "/status")
}

func (client *Client) TaskRestart(name string, task string) (*Status, error) {
	_, status, err := post[any](client.url+"/connectors/"+name+"/tasks/"+task+"/restart", []byte{})

	return status, err
}

func post[T any](url string, body []byte) (*T, *Status, error) {
	var emptyResult T

	request, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))

	if err != nil {
		return nil, nil, err
	}

	request.Header.Set("Content-Type", "application/json")

	client := http.Client{}

	response, err := client.Do(request)

	if err != nil {
		return nil, nil, err
	}

	if response.StatusCode >= 400 {
		return handleHttpError(response, emptyResult)
	}

	responseBody, err := io.ReadAll(response.Body)

	status := Status{response.Status, response.StatusCode, ""}

	if status.StatusCode < 200 || status.StatusCode >= 300 || err != nil || len(responseBody) == 0 {
		return nil, &status, err
	}

	var responseObject T

	err = json.Unmarshal(responseBody, &responseObject)

	if err != nil {
		return nil, &status, err
	}

	return &responseObject, &status, nil
}

func handleHttpError[T any](response *http.Response, _ T) (*T, *Status, error) {
	responseBody, err := io.ReadAll(response.Body)

	if err == nil && len(responseBody) > 0 {
		var responseStatus types.Status

		err = json.Unmarshal(responseBody, &responseStatus)

		if err == nil {
			status := Status{response.Status, response.StatusCode, responseStatus.Message}

			return nil, &status, nil
		}
	}

	status := Status{response.Status, response.StatusCode, ""}

	return nil, &status, nil
}

func put[I any, O any](url string, body []byte) (*O, *Status, error) {
	var emptyResult O

	request, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(body))

	if err != nil {
		return nil, nil, err
	}

	request.Header.Set("Content-Type", "application/json")

	client := http.Client{}

	response, err := client.Do(request)

	if err != nil {
		return nil, nil, err
	}

	if response.StatusCode >= 400 {
		return handleHttpError(response, emptyResult)
	}

	responseBody, err := io.ReadAll(response.Body)

	status := Status{response.Status, response.StatusCode, ""}

	if status.StatusCode < 200 || status.StatusCode >= 300 || err != nil || len(responseBody) == 0 {
		return nil, &status, err
	}

	var responseObject O

	err = json.Unmarshal(responseBody, &responseObject)

	if err != nil {
		return nil, &status, err
	}

	return &responseObject, &status, nil
}

func remove(url string) (*Status, error) {
	request, err := http.NewRequest(http.MethodDelete, url, bytes.NewBuffer([]byte{}))

	if err != nil {
		return nil, err
	}

	client := http.Client{}

	response, err := client.Do(request)

	return &Status{response.Status, response.StatusCode, ""}, err
}

func get[T any](url string) (*T, *Status, error) {
	response, err := http.Get(url)

	if err != nil {
		return nil, nil, err
	}

	status := Status{response.Status, response.StatusCode, ""}

	if status.StatusCode < 200 || status.StatusCode >= 300 {
		return nil, &status, err
	}

	body, err := io.ReadAll(response.Body)

	if err != nil {
		return nil, &status, err
	}

	var responseObject T

	err = json.Unmarshal(body, &responseObject)

	if err != nil {
		return nil, &status, err
	}

	return &responseObject, &status, nil
}
