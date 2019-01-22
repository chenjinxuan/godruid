package godruid

import (
	"bytes"
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
		Timeout:   20 * time.Second,
		KeepAlive: 30 * time.Second,
	}).Dial,
	TLSHandshakeTimeout: 20 * time.Second,
}

type Client struct {
	Url          string
	Debug        bool
	LastRequest  string
	LastResponse string
	HttpClient   *http.Client
}

func NewClient(urlStr string, httpClient *http.Client) *Client {
	client := &Client{
		Url:        urlStr,
		HttpClient: &http.Client{Transport: DefaultTransport},
	}
	if httpClient != nil {
		client.HttpClient = httpClient
	}
	return client

}

func (c *Client) Query(query Query) (res interface{}, err error) {
	query.setup()
	var reqJson []byte
	if c.Debug {
		reqJson, err = json.MarshalIndent(query, "", "  ")
	} else {
		reqJson, err = json.Marshal(query)
	}
	if err != nil {
		return
	}
	result, err := c.QueryRaw(reqJson)
	if err != nil {
		return
	}
	return query.onResponse(result)
}

func (c *Client) QueryRaw(req []byte) (result []byte, err error) {
	queryUrl := c.Url
	if c.Debug {
		queryUrl += "?pretty"
		c.LastRequest = string(req)
	}
	if err != nil {
		return
	}

	request, err := http.NewRequest("POST", queryUrl, bytes.NewBuffer(req))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")

	resp, err := c.HttpClient.Do(request)
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
	if c.Debug {
		c.LastResponse = string(result)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", resp.Status, string(result))
	}

	return
}
