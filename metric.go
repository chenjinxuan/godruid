package godruid

type Metric interface{}

type MetricNumeric struct {
	Metric interface{} `json:"metric,omitempty"`
	Type   Ordering    `json:"type,omitempty"`
}

func MetricSetNumeric(metric string) Metric {
	return MetricNumeric{
		Type:   NUMERIC,
		Metric: metric,
	}
}

func MetricSetInvertedNumeric(metric Metric) Metric {
	return MetricNumeric{
		Type:   "inverted",
		Metric: metric,
	}
}
