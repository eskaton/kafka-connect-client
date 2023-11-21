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
	url      string
	user     string
	password string
}

type Status struct {
	Status        string
	StatusCode    int
	ServerMessage string
}

func New(url string, user string, password string) *Client {
	return &Client{url: url, user: user, password: password}
}

func (client *Client) Cluster() (*types.Cluster, *Status, error) {
	return get[types.Cluster](client, "")
}

func (client *Client) Connectors() (*types.Connectors, *Status, error) {
	return get[types.Connectors](client, "/connectors")
}

func (client *Client) Connector(name string) (*types.Connector, *Status, error) {
	return get[types.Connector](client, "/connectors/"+name)
}

func (client *Client) ConnectorStatus(name string) (*types.ConnectorStatus, *Status, error) {
	return get[types.ConnectorStatus](client, "/connectors/"+name+"/status")
}

func (client *Client) ConnectorPause(name string) (*Status, error) {
	_, status, err := put[any, any](client, "/connectors/"+name+"/pause", []byte{})

	return status, err
}

func (client *Client) ConnectorResume(name string) (*Status, error) {
	_, status, err := put[any, any](client, "/connectors/"+name+"/resume", []byte{})

	return status, err
}

func (client *Client) ConnectorRestart(name string, includeTasks bool, onlyFailed bool) (*types.ConnectorStatus, *Status, error) {
	url := "/connectors/" + name + "/restart?includeTasks=" + strconv.FormatBool(includeTasks) + "&onlyFailed=" + strconv.FormatBool(onlyFailed)

	return post[types.ConnectorStatus](client, url, []byte{})
}

func (client *Client) ConnectorDelete(name string) (*Status, error) {
	return remove(client, "/connectors/"+name)
}

func (client *Client) ConnectorCreate(connector types.Connector) (*types.Connector, *Status, error) {
	content, err := json.Marshal(connector)

	if err != nil {
		return nil, nil, err
	}

	return post[types.Connector](client, "/connectors/", content)
}

func (client *Client) ConnectorUpdateConfig(name string, config types.ConnectorConfig) (*Status, error) {
	content, err := json.Marshal(config)

	if err != nil {
		return nil, err
	}

	_, status, err := put[types.ConnectorConfig, types.Connector](client, "/connectors/"+name+"/config", content)

	return status, err
}

func (client *Client) Tasks(name string) (*types.Tasks, *Status, error) {
	return get[types.Tasks](client, "/connectors/"+name+"/tasks")
}

func (client *Client) TaskStatus(name string, task string) (*types.TaskStatus, *Status, error) {
	return get[types.TaskStatus](client, "/connectors/"+name+"/tasks/"+task+"/status")
}

func (client *Client) TaskRestart(name string, task string) (*Status, error) {
	_, status, err := post[any](client, "/connectors/"+name+"/tasks/"+task+"/restart", []byte{})

	return status, err
}

func post[T any](client *Client, url string, body []byte) (*T, *Status, error) {
	var emptyResult T

	request, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))

	if err != nil {
		return nil, nil, err
	}

	addAuth(client, request)

	request.Header.Set("Content-Type", "application/json")

	httpClient := http.Client{}

	response, err := httpClient.Do(request)

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

func put[I any, O any](client *Client, url string, body []byte) (*O, *Status, error) {
	var emptyResult O

	request, err := http.NewRequest(http.MethodPut, client.url+url, bytes.NewBuffer(body))

	if err != nil {
		return nil, nil, err
	}

	addAuth(client, request)

	request.Header.Set("Content-Type", "application/json")

	httpClient := http.Client{}

	response, err := httpClient.Do(request)

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

func remove(client *Client, url string) (*Status, error) {
	request, err := http.NewRequest(http.MethodDelete, client.url+url, bytes.NewBuffer([]byte{}))

	if err != nil {
		return nil, err
	}

	addAuth(client, request)

	httpClient := http.Client{}

	response, err := httpClient.Do(request)

	return &Status{response.Status, response.StatusCode, ""}, err
}

func get[T any](client *Client, url string) (*T, *Status, error) {
	request, err := http.NewRequest(http.MethodGet, client.url+url, bytes.NewBuffer([]byte{}))

	if err != nil {
		return nil, nil, err
	}

	addAuth(client, request)

	httpClient := http.Client{}

	response, err := httpClient.Do(request)

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

func addAuth(client *Client, request *http.Request) {
	if len(client.user) > 0 && len(client.password) > 0 {
		request.SetBasicAuth(client.user, client.password)
	}
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
