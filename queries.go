package godruid

import (
	json "github.com/json-iterator/go"
)

// Check http://druid.io/docs/0.6.154/Querying.html#query-operators for detail description.

// The Query interface stands for any kinds of druid query.
type Query interface {
	setup()
	onResponse(content []byte) (interface{}, error)
	GetRawJSON() []byte
	GetQueryType() string
}

const (
	TIMESERIES      string = "timeseries"
	TOPN            string = "topN"
	SEARCH          string = "search"
	GROUPBY         string = "groupBy"
	SEGMENTMETADATA string = "segmentMetadata"
	TIMEBOUNDARY    string = "timeBoundary"
	SELECT          string = "select"
	SCAN            string = "scan"
)

// Context constants
const (
	TIMEOUT          = "timeout"
	SKIPEMPTYBUCKETS = "skipEmptyBuckets"
	QUERYID          = "queryId"
)

// ---------------------------------
// GroupBy Query
// ---------------------------------

type QueryGroupBy struct {
	QueryType        string                 `json:"queryType"`
	DataSource       string                 `json:"dataSource"`
	Dimensions       []DimSpec              `json:"dimensions"`
	Granularity      Granlarity             `json:"granularity"`
	LimitSpec        *Limit                 `json:"limitSpec,omitempty"`
	Having           *Having                `json:"having,omitempty"`
	Filter           *Filter                `json:"filter,omitempty"`
	Aggregations     []Aggregation          `json:"aggregations"`
	PostAggregations []PostAggregation      `json:"postAggregations,omitempty"`
	Intervals        Intervals              `json:"intervals"`
	Context          map[string]interface{} `json:"context,omitempty"`
	VirtualColumns   []VirtualColumn        `json:"virtualColumns,omitempty"`
	QueryResult      []GroupbyItem          `json:"-"`
	RawJSON          []byte
}

type GroupbyItem struct {
	Version   string                 `json:"version"`
	Timestamp string                 `json:"timestamp"`
	Event     map[string]interface{} `json:"event"`
}

func (q *QueryGroupBy) setup()             { q.QueryType = GROUPBY }
func (q *QueryGroupBy) GetRawJSON() []byte { return q.RawJSON }
func (q *QueryGroupBy) onResponse(content []byte) (interface{}, error) {
	res := new([]GroupbyItem)
	err := json.Unmarshal(content, res)
	if err != nil {
		return nil, err
	}
	q.QueryResult = *res
	q.RawJSON = content
	return q.QueryResult, nil
}

func (q *QueryGroupBy) GetQueryType() string {
	return q.QueryType
}

// ---------------------------------
// Search Query
// ---------------------------------

type QuerySearch struct {
	QueryType        string                 `json:"queryType"`
	DataSource       string                 `json:"dataSource"`
	Granularity      Granlarity             `json:"granularity"`
	Filter           *Filter                `json:"filter,omitempty"`
	Intervals        Intervals              `json:"intervals"`
	SearchDimensions []string               `json:"searchDimensions,omitempty"`
	Query            *SearchQuery           `json:"query"`
	Sort             *SearchSort            `json:"sort"`
	Context          map[string]interface{} `json:"context,omitempty"`
	VirtualColumns   []VirtualColumn        `json:"virtualColumns,omitempty"`
	QueryResult      []SearchItem           `json:"-"`
	RawJSON          []byte
}

type SearchItem struct {
	Timestamp string     `json:"timestamp"`
	Result    []DimValue `json:"result"`
}

type DimValue struct {
	Dimension string `json:"dimension"`
	Value     string `json:"value"`
}

func (q *QuerySearch) setup()             { q.QueryType = SEARCH }
func (q *QuerySearch) GetRawJSON() []byte { return q.RawJSON }
func (q *QuerySearch) onResponse(content []byte) (interface{}, error) {
	res := new([]SearchItem)
	err := json.Unmarshal(content, res)
	if err != nil {
		return nil, err
	}
	q.QueryResult = *res
	q.RawJSON = content
	return q.QueryResult, nil
}
func (q *QuerySearch) GetQueryType() string {
	return q.QueryType
}

// ---------------------------------
// SegmentMetadata Query
// ---------------------------------

type QuerySegmentMetadata struct {
	QueryType      string                 `json:"queryType"`
	DataSource     string                 `json:"dataSource"`
	Intervals      Intervals              `json:"intervals"`
	ToInclude      *ToInclude             `json:"toInclude,omitempty"`
	Merge          interface{}            `json:"merge,omitempty"`
	Context        map[string]interface{} `json:"context,omitempty"`
	VirtualColumns []VirtualColumn        `json:"virtualColumns,omitempty"`
	QueryResult    []SegmentMetaData      `json:"-"`
	RawJSON        []byte
}

type SegmentMetaData struct {
	Id        string                `json:"id"`
	Intervals Intervals             `json:"intervals"`
	Columns   map[string]ColumnItem `json:"columns"`
}

type ColumnItem struct {
	Type        string      `json:"type"`
	Size        int         `json:"size"`
	Cardinality interface{} `json:"cardinality"`
}

func (q *QuerySegmentMetadata) setup()             { q.QueryType = "segmentMetadata" }
func (q *QuerySegmentMetadata) GetRawJSON() []byte { return q.RawJSON }
func (q *QuerySegmentMetadata) onResponse(content []byte) (interface{}, error) {
	res := new([]SegmentMetaData)
	err := json.Unmarshal(content, res)
	if err != nil {
		return nil, err
	}
	q.QueryResult = *res
	q.RawJSON = content
	return q.QueryResult, nil
}
func (q *QuerySegmentMetadata) GetQueryType() string {
	return q.QueryType
}

// ---------------------------------
// TimeBoundary Query
// ---------------------------------

type QueryTimeBoundary struct {
	QueryType  string                 `json:"queryType"`
	DataSource string                 `json:"dataSource"`
	Bound      string                 `json:"bound,omitempty"`
	Context    map[string]interface{} `json:"context,omitempty"`

	QueryResult []TimeBoundaryItem `json:"-"`
	RawJSON     []byte
}

type TimeBoundaryItem struct {
	Timestamp string       `json:"timestamp"`
	Result    TimeBoundary `json:"result"`
}

type TimeBoundary struct {
	MinTime string `json:"minTime"`
	MaxTime string `json:"minTime"`
}

func (q *QueryTimeBoundary) setup()             { q.QueryType = TIMEBOUNDARY }
func (q *QueryTimeBoundary) GetRawJSON() []byte { return q.RawJSON }
func (q *QueryTimeBoundary) onResponse(content []byte) error {
	res := new([]TimeBoundaryItem)
	err := json.Unmarshal(content, res)
	if err != nil {
		return err
	}
	q.QueryResult = *res
	q.RawJSON = content
	return nil
}
func (q *QueryTimeBoundary) GetQueryType() string {
	return q.QueryType
}

// ---------------------------------
// Timeseries Query
// ---------------------------------

type QueryTimeseries struct {
	QueryType        string                 `json:"queryType"`
	DataSource       string                 `json:"dataSource"`
	Granularity      Granlarity             `json:"granularity"`
	Descending       bool                   `json:"descending"`
	Filter           *Filter                `json:"filter,omitempty"`
	Aggregations     []Aggregation          `json:"aggregations"`
	PostAggregations []PostAggregation      `json:"postAggregations,omitempty"`
	Intervals        Intervals              `json:"intervals"`
	Context          map[string]interface{} `json:"context,omitempty"`
	VirtualColumns   []VirtualColumn        `json:"virtualColumns,omitempty"`
	QueryResult      []Timeseries           `json:"-"`
	RawJSON          []byte
}

type Timeseries struct {
	Timestamp string                 `json:"timestamp"`
	Result    map[string]interface{} `json:"result"`
}

func (q *QueryTimeseries) setup()             { q.QueryType = TIMESERIES }
func (q *QueryTimeseries) GetRawJSON() []byte { return q.RawJSON }
func (q *QueryTimeseries) onResponse(content []byte) (interface{}, error) {
	res := new([]Timeseries)
	err := json.Unmarshal(content, res)
	if err != nil {
		return nil, err
	}
	q.QueryResult = *res
	q.RawJSON = content
	return q.QueryResult, nil
}
func (q *QueryTimeseries) GetQueryType() string {
	return q.QueryType
}

// ---------------------------------
// TopN Query
// ---------------------------------

type QueryTopN struct {
	QueryType        string                 `json:"queryType"`
	DataSource       string                 `json:"dataSource"`
	Granularity      Granlarity             `json:"granularity"`
	Dimension        DimSpec                `json:"dimension"`
	Threshold        int                    `json:"threshold"`
	Metric           interface{}            `json:"metric"` // *TopNMetric
	Filter           *Filter                `json:"filter,omitempty"`
	Aggregations     []Aggregation          `json:"aggregations"`
	PostAggregations []PostAggregation      `json:"postAggregations,omitempty"`
	Intervals        Intervals              `json:"intervals"`
	Context          map[string]interface{} `json:"context,omitempty"`
	VirtualColumns   []VirtualColumn        `json:"virtualColumns,omitempty"`
	QueryResult      []TopNItem             `json:"-"`
	RawJSON          []byte
}

type TopNItem struct {
	Timestamp string                   `json:"timestamp"`
	Result    []map[string]interface{} `json:"result"`
}

func (q *QueryTopN) setup()             { q.QueryType = TOPN }
func (q *QueryTopN) GetRawJSON() []byte { return q.RawJSON }
func (q *QueryTopN) onResponse(content []byte) (interface{}, error) {
	res := new([]TopNItem)
	err := json.Unmarshal(content, res)
	if err != nil {
		return nil, err
	}
	q.QueryResult = *res
	q.RawJSON = content
	return q.QueryResult, nil
}
func (q *QueryTopN) GetQueryType() string {
	return q.QueryType
}

// ---------------------------------
// Select Query
// ---------------------------------

type QuerySelect struct {
	QueryType      string                 `json:"queryType"`
	DataSource     string                 `json:"dataSource"`
	Intervals      Intervals              `json:"intervals"`
	Filter         *Filter                `json:"filter,omitempty"`
	Dimensions     []DimSpec              `json:"dimensions"`
	Metrics        []string               `json:"metrics"`
	Granularity    Granlarity             `json:"granularity"`
	PagingSpec     map[string]interface{} `json:"pagingSpec,omitempty"`
	Context        map[string]interface{} `json:"context,omitempty"`
	VirtualColumns []VirtualColumn        `json:"virtualColumns,omitempty"`
	QueryResult    SelectBlob             `json:"-"`
	RawJSON        []byte
}

// Select json blob from druid comes back as following:
// http://druid.io/docs/latest/querying/select-query.html
// the interesting results are in events blob which we
// call as 'SelectEvent'.
type SelectBlob struct {
	Timestamp string       `json:"timestamp"`
	Result    SelectResult `json:"result"`
}

type SelectResult struct {
	PagingIdentifiers map[string]interface{} `json:"pagingIdentifiers"`
	Events            []SelectEvent          `json:"events"`
}

type SelectEvent struct {
	SegmentId string                 `json:"segmentId"`
	Offset    int64                  `json:"offset"`
	Event     map[string]interface{} `json:"event"`
}

func (q *QuerySelect) setup()             { q.QueryType = SELECT }
func (q *QuerySelect) GetRawJSON() []byte { return q.RawJSON }
func (q *QuerySelect) onResponse(content []byte) (interface{}, error) {
	res := new([]SelectBlob)
	err := json.Unmarshal(content, res)
	if err != nil {
		return nil, err
	}
	if len(*res) == 0 {
		q.QueryResult = SelectBlob{}
	} else {
		q.QueryResult = (*res)[0]
	}
	q.RawJSON = content
	return q.QueryResult, nil
}
func (q *QuerySelect) GetQueryType() string {
	return q.QueryType
}

// ---------------------------------
// Scan Query
// ---------------------------------

type QueryScan struct {
	QueryType      string                 `json:"queryType"`
	DataSource     string                 `json:"dataSource"`
	Limit          int                    `json:"limit,omitempty"`
	Columns        []string               `json:"columns,omitempty"`
	ResultFormat   string                 `json:"resultFormat,omitempty"`
	Metric         interface{}            `json:"metric"` // *TopNMetric
	Filter         *Filter                `json:"filter,omitempty"`
	Intervals      Intervals              `json:"intervals"`
	Context        map[string]interface{} `json:"context,omitempty"`
	VirtualColumns []VirtualColumn        `json:"virtualColumns,omitempty"`
	QueryResult    []ScanBlob             `json:"-"`
	RawJSON        []byte
}

type ScanBlob struct {
	SegmentID string                   `json:"segmentId"`
	Columns   []string                 `json:"columns"`
	Events    []map[string]interface{} `json:"events"`
}

func (q *QueryScan) setup()             { q.QueryType = SCAN }
func (q *QueryScan) GetRawJSON() []byte { return q.RawJSON }
func (q *QueryScan) onResponse(content []byte) (interface{}, error) {
	res := new([]ScanBlob)
	err := json.Unmarshal(content, res)
	if err != nil {
		return nil, err
	}
	q.QueryResult = *res
	q.RawJSON = content
	return q.QueryResult, nil
}

func (q *QueryScan) GetQueryType() string {
	return q.QueryType
}

type VirtualColumn struct {
	Type       string                  `json:"type"`
	Name       string                  `json:"name"`
	Expression string                  `json:"expression"`
	OutputType VirtualColumnOutputType `json:"outputType"`
}

func NewVirtualColumn(name string, expression string, outputType VirtualColumnOutputType) VirtualColumn {
	return VirtualColumn{
		Type:       "expression",
		Name:       name,
		Expression: expression,
		OutputType: outputType,
	}
}

type VirtualColumnOutputType string

const (
	VirtualColumnLong   VirtualColumnOutputType = "LONG"
	VirtualColumnFloat  VirtualColumnOutputType = "FLOAT"
	VirtualColumnDouble VirtualColumnOutputType = "DOUBLE"
	VirtualColumnString VirtualColumnOutputType = "STRING"
)
