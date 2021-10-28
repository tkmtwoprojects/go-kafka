//
// Package connect provides support for the Apache Kafka Connect REST API
// documented at https://docs.confluent.io/platform/current/connect/references/restapi.html
//
// Coverage is not complete.  This package only supports what is necessary to do CRUD
// operations against connectors.  This is a starting point for Terraform providers
// that support modern or enhanced authentication (specifically oauth2).
//

package connect

import (
	"net/http"
	"net/url"
	"time"

	"github.com/go-resty/resty/v2"
)

const (
	userAgent = "go-kafka"
)

type Client struct {
	httpClient  *http.Client
	restyClient *resty.Client

	common service //Reuse a single struct instead of allocating one for each service on the heap.

	//
	//Protocol, hostname, port with trailing slash.  For example:
	//  http://localhost/
	//  http://localhost:8083/
	//  http://localhost:8083/some/mapped/path/
	//
	BaseURL *url.URL

	//User agent used when communicating with the APIs
	UserAgent string

	Clusters   *ClustersService
	Connectors *ConnectorsService
}

type service struct {
	client *Client
}

func NewClient(httpClient *http.Client, baseURL string) (*Client, error) {
	if httpClient == nil {
		httpClient = &http.Client{}
	}

	baseEndpoint, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	c := &Client{httpClient: httpClient, restyClient: resty.NewWithClient(httpClient)}
	c.common.client = c

	//
	//TODO: make timeouts and retries configurable for troublesome environments
	//
	c.restyClient.SetHeader("Accept", "application/json")
	c.restyClient.SetRetryCount(5)
	c.restyClient.SetRetryWaitTime(500 * time.Millisecond)
	c.restyClient.SetRetryMaxWaitTime(5 * time.Second)
	c.restyClient.SetTimeout(10 * time.Second)
	c.restyClient.AddRetryCondition(
		func(r *resty.Response, err error) bool {
			return r.StatusCode() == 409
		},
	)

	c.BaseURL = baseEndpoint

	c.UserAgent = userAgent

	c.Clusters = (*ClustersService)(&c.common)
	c.Connectors = (*ConnectorsService)(&c.common)
	return c, nil
}

/*
type ErrorResponse struct {
	ErrorCode int    `json:"error_code,omitempty"`
	Message   string `json:"message,omitempty"`
}
*/
