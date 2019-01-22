package godruid

import (
	"bytes"
	"context"
	"fmt"
	json "github.com/json-iterator/go"
	"io/ioutil"
	"net"
	"net/http"
	"time"
)

const (
	DefaultEndPoint = "/druid/v2"
)

var DefaultTransport *http.Transport = &http.Transport{
	Dial: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).Dial,
	TLSHandshakeTimeout: 10 * time.Second,
}

type Client struct {
	url          string
	debug        bool
	lastRequest  string
	lastResponse string
	httpClient   *http.Client
}

func NewClient(urlStr string, httpClient *http.Client) *Client {
	client := &Client{
		url:        urlStr,
		httpClient: &http.Client{Transport: DefaultTransport},
	}
	if httpClient != nil {
		client.httpClient = httpClient
	}
	return client

}
func (c *Client) SetDebug(debug bool) {
	c.debug = debug
}
func (c *Client) GetDebug() bool {
	return c.debug
}

func (c *Client) Query(ctx context.Context, query Query) (res interface{}, err error) {
	query.setup()
	var reqJson []byte
	if c.debug {
		reqJson, err = json.MarshalIndent(query, "", "  ")
		fmt.Println("req => ", string(reqJson))
	} else {
		reqJson, err = json.Marshal(query)
	}
	if err != nil {
		return
	}
	result, err := c.QueryRaw(ctx, reqJson)
	if err != nil {
		return
	}
	if c.debug {
		fmt.Println("res => ", string(result))
	}
	return query.onResponse(result)
}

func (c *Client) QueryRaw(ctx context.Context, req []byte) (result []byte, err error) {
	queryUrl := c.url
	if c.debug {
		queryUrl += "?pretty"
		c.lastRequest = string(req)
	}
	if err != nil {
		return
	}

	request, err := http.NewRequest("POST", queryUrl, bytes.NewBuffer(req))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	if ctx != nil {
		request = request.WithContext(ctx)
	}
	resp, err := c.httpClient.Do(request)
	defer func() {
		resp.Body.Close()
	}()

	if err != nil {
		return nil, err
	}

	result, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	if c.debug {
		c.lastResponse = string(result)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", resp.Status, string(result))
	}

	return
}
