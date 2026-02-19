// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package client

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// AuthHeader returns an Authorization header value from apiKey (for type api_key) or username+password (for basic).
func AuthHeader(typ, apiKey, username, password string) string {
	if typ == "basic" && username != "" {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
	}
	if apiKey != "" {
		if strings.HasPrefix(strings.ToLower(apiKey), "apikey ") {
			return apiKey
		}
		return "ApiKey " + apiKey
	}
	return ""
}

const apiPrefix = "/api/automatic_import_v2"

// Client calls the Automatic Import V2 HTTP API.
type Client struct {
	baseURL    string
	authHeader string
	http       *http.Client
	retries    int
}

// New builds a client. baseURL is Kibana root (e.g. http://localhost:5601). authHeader is "ApiKey <key>" or "Basic <base64>".
func New(baseURL string, authHeader string, timeout time.Duration, retries int) *Client {
	baseURL = strings.TrimSuffix(baseURL, "/")
	if retries <= 0 {
		retries = 1
	}
	return &Client{
		baseURL:    baseURL,
		authHeader: authHeader,
		http: &http.Client{
			Timeout: timeout,
		},
		retries: retries,
	}
}

func (c *Client) do(method, path string, body interface{}, out interface{}) error {
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(b)
	}
	u := c.baseURL + path
	req, err := http.NewRequest(method, u, bodyReader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("kbn-xsrf", "true")
	req.Header.Set("elastic-api-version", "1")
	if c.authHeader != "" {
		req.Header.Set("Authorization", c.authHeader)
	}
	var lastErr error
	for attempt := 0; attempt < c.retries; attempt++ {
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			if attempt < c.retries-1 {
				time.Sleep(time.Duration(attempt+1) * time.Second)
			}
			continue
		}
		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if out != nil && len(bodyBytes) > 0 {
				if err := json.Unmarshal(bodyBytes, out); err != nil {
					return fmt.Errorf("decode response: %w", err)
				}
			}
			return nil
		}
		lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
		if resp.StatusCode >= 500 && attempt < c.retries-1 {
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}
		return lastErr
	}
	return lastErr
}

// IntegrationSummary is one integration in the list.
type IntegrationSummary struct {
	IntegrationID             string `json:"integrationId"`
	Title                     string `json:"title"`
	TotalDataStreamCount      int    `json:"totalDataStreamCount"`
	SuccessfulDataStreamCount int    `json:"successfulDataStreamCount"`
	Status                    string `json:"status"`
}

// ListIntegrations returns all integrations.
func (c *Client) ListIntegrations() ([]IntegrationSummary, error) {
	var out []IntegrationSummary
	if err := c.do(http.MethodGet, apiPrefix+"/integrations", nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// GetIntegrationResponse is the get-integration response.
type GetIntegrationResponse struct {
	IntegrationResponse IntegrationResponse `json:"integrationResponse"`
}

// IntegrationResponse is the full integration with data streams.
type IntegrationResponse struct {
	IntegrationID string        `json:"integrationId"`
	Title        string        `json:"title"`
	DataStreams  []DataStreamSummary `json:"dataStreams"`
	Status       string        `json:"status"`
}

// DataStreamSummary is one data stream in an integration.
type DataStreamSummary struct {
	DataStreamID string `json:"dataStreamId"`
	Status       string `json:"status"`
}

// GetIntegration returns one integration by ID.
func (c *Client) GetIntegration(integrationID string) (*IntegrationResponse, error) {
	path := apiPrefix + "/integrations/" + url.PathEscape(integrationID)
	var out GetIntegrationResponse
	if err := c.do(http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	return &out.IntegrationResponse, nil
}

// CreateIntegrationRequest is the PUT body for create/update integration.
type CreateIntegrationRequest struct {
	IntegrationID string                `json:"integrationId"`
	Title         string                `json:"title"`
	Description   string                `json:"description"`
	Logo          string                `json:"logo,omitempty"`
	ConnectorID   string                `json:"connectorId"`
	DataStreams   []CreateDataStreamReq `json:"dataStreams,omitempty"`
}

// CreateDataStreamReq is one data stream in the create request.
type CreateDataStreamReq struct {
	DataStreamID string       `json:"dataStreamId"`
	Title        string       `json:"title"`
	Description  string       `json:"description"`
	InputTypes   []InputType  `json:"inputTypes"`
	RawSamples   []string     `json:"rawSamples,omitempty"`
}

// InputType is the name of a data stream input type (e.g. filestream, kafka).
type InputType struct {
	Name string `json:"name"`
}

// CreateIntegrationResponse is the create response.
type CreateIntegrationResponse struct {
	IntegrationID string `json:"integration_id"`
}

// CreateIntegration creates or updates an integration and optionally data streams.
func (c *Client) CreateIntegration(req CreateIntegrationRequest) (*CreateIntegrationResponse, error) {
	var out CreateIntegrationResponse
	if err := c.do(http.MethodPut, apiPrefix+"/integrations", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// UploadSamplesRequest is the POST body for upload samples.
type UploadSamplesRequest struct {
	Samples       []string      `json:"samples"`
	OriginalSource OriginalSource `json:"originalSource"`
}

// OriginalSource describes where samples came from.
type OriginalSource struct {
	SourceType  string `json:"sourceType"`  // e.g. "file"
	SourceValue string `json:"sourceValue"` // e.g. "samples.ndjson"
}

// UploadSamples uploads samples to a data stream. sourceValue is the filename (e.g. "samples.log" or "samples.ndjson"); if empty, "samples.ndjson" is used.
func (c *Client) UploadSamples(integrationID, dataStreamID string, samples []string, sourceValue string) error {
	if sourceValue == "" {
		sourceValue = "samples.ndjson"
	}
	path := apiPrefix + "/integrations/" + url.PathEscape(integrationID) + "/data_streams/" + url.PathEscape(dataStreamID) + "/upload"
	body := UploadSamplesRequest{
		Samples:        samples,
		OriginalSource: OriginalSource{SourceType: "file", SourceValue: sourceValue},
	}
	return c.do(http.MethodPost, path, body, nil)
}

// DataStreamResults is the get-results response.
type DataStreamResults struct {
	IngestPipeline interface{}   `json:"ingestPipeline,omitempty"`
	PipelineDocs   interface{}   `json:"pipeline_docs,omitempty"`
	Status         string        `json:"status,omitempty"`
	Duration       time.Duration `json:"duration,omitempty"`
}

// GetDataStreamResults returns results for a data stream.
func (c *Client) GetDataStreamResults(integrationID, dataStreamID string) (*DataStreamResults, error) {
	path := apiPrefix + "/integrations/" + url.PathEscape(integrationID) + "/data_streams/" + url.PathEscape(dataStreamID) + "/results"
	var out DataStreamResults
	if err := c.do(http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeleteDataStream deletes a data stream.
func (c *Client) DeleteDataStream(integrationID, dataStreamID string) error {
	path := apiPrefix + "/integrations/" + url.PathEscape(integrationID) + "/data_streams/" + url.PathEscape(dataStreamID)
	return c.do(http.MethodDelete, path, nil, nil)
}
