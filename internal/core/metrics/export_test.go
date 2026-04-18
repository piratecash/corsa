package metrics

// CollectTrafficSample exposes the unexported method for external test package.
func (c *Collector) CollectTrafficSample() {
	c.collectTrafficSample()
}

// CollectorTraffic returns the internal TrafficHistory for test assertions.
func (c *Collector) CollectorTraffic() *TrafficHistory {
	return c.traffic
}

// TrafficHistoryCapacityForTest exports the unexported capacity constant.
const TrafficHistoryCapacityForTest = trafficHistoryCapacity
