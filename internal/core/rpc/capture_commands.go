package rpc

import (
	"fmt"
	"strconv"
	"strings"
)

// RegisterCaptureCommands registers recordPeerTrafficByConnID,
// recordPeerTrafficByIP, recordAllPeerTraffic and stopPeerTrafficRecording.
// When the CaptureProvider is nil, commands are registered as unavailable.
func RegisterCaptureCommands(t *CommandTable, cp CaptureProvider) {
	startByConnIDInfo := CommandInfo{
		Name:        "recordPeerTrafficByConnID",
		Description: "Start recording peer traffic for given connection IDs",
		Category:    "diagnostic",
		Usage:       "conn_ids=41,42 [format=compact|pretty]",
	}
	startByIPInfo := CommandInfo{
		Name:        "recordPeerTrafficByIP",
		Description: "Start recording peer traffic for given remote IPs",
		Category:    "diagnostic",
		Usage:       "ips=203.0.113.10,198.51.100.7 [format=compact|pretty]",
	}
	startAllInfo := CommandInfo{
		Name:        "recordAllPeerTraffic",
		Description: "Start recording all peer traffic",
		Category:    "diagnostic",
		Usage:       "[format=compact|pretty]",
	}
	stopInfo := CommandInfo{
		Name:        "stopPeerTrafficRecording",
		Description: "Stop recording peer traffic",
		Category:    "diagnostic",
		Usage:       "conn_ids=41,42 | ips=203.0.113.10 | scope=all",
	}

	if cp == nil {
		t.RegisterUnavailable(startByConnIDInfo)
		t.RegisterUnavailable(startByIPInfo)
		t.RegisterUnavailable(startAllInfo)
		t.RegisterUnavailable(stopInfo)
		return
	}

	t.Register(startByConnIDInfo, func(req CommandRequest) CommandResponse {
		connIDsStr, _ := req.Args["conn_ids"].(string)
		if strings.TrimSpace(connIDsStr) == "" {
			return validationError(fmt.Errorf("conn_ids is required"))
		}
		connIDs, err := parseUint64CSV(connIDsStr)
		if err != nil {
			return validationError(fmt.Errorf("invalid conn_ids: %w", err))
		}
		format, _ := req.Args["format"].(string)

		data, err := cp.StartCaptureByConnIDs(connIDs, format)
		if err != nil {
			return internalError(err)
		}
		return CommandResponse{Data: data}
	})

	t.Register(startByIPInfo, func(req CommandRequest) CommandResponse {
		ipsStr, _ := req.Args["ips"].(string)
		if strings.TrimSpace(ipsStr) == "" {
			return validationError(fmt.Errorf("ips is required"))
		}
		ips := strings.Split(ipsStr, ",")
		for i := range ips {
			ips[i] = strings.TrimSpace(ips[i])
		}
		format, _ := req.Args["format"].(string)

		data, err := cp.StartCaptureByIPs(ips, format)
		if err != nil {
			return internalError(err)
		}
		return CommandResponse{Data: data}
	})

	t.Register(startAllInfo, func(req CommandRequest) CommandResponse {
		format, _ := req.Args["format"].(string)

		data, err := cp.StartCaptureAll(format)
		if err != nil {
			return internalError(err)
		}
		return CommandResponse{Data: data}
	})

	t.Register(stopInfo, func(req CommandRequest) CommandResponse {
		connIDsStr, _ := req.Args["conn_ids"].(string)
		ipsStr, _ := req.Args["ips"].(string)
		scope, _ := req.Args["scope"].(string)

		hasConnIDs := strings.TrimSpace(connIDsStr) != ""
		hasIPs := strings.TrimSpace(ipsStr) != ""
		hasScope := strings.TrimSpace(scope) != ""

		// Plan §5.3: no arguments = validation error.
		if !hasConnIDs && !hasIPs && !hasScope {
			return validationError(fmt.Errorf("at least one of conn_ids, ips, or scope is required"))
		}

		if hasConnIDs {
			connIDs, err := parseUint64CSV(connIDsStr)
			if err != nil {
				return validationError(fmt.Errorf("invalid conn_ids: %w", err))
			}
			data, err := cp.StopCaptureByConnIDs(connIDs)
			if err != nil {
				return internalError(err)
			}
			return CommandResponse{Data: data}
		}

		if hasIPs {
			ips := strings.Split(ipsStr, ",")
			for i := range ips {
				ips[i] = strings.TrimSpace(ips[i])
			}
			data, err := cp.StopCaptureByIPs(ips)
			if err != nil {
				return internalError(err)
			}
			return CommandResponse{Data: data}
		}

		if scope == "all" {
			data, err := cp.StopCaptureAll()
			if err != nil {
				return internalError(err)
			}
			return CommandResponse{Data: data}
		}

		return validationError(fmt.Errorf("scope must be 'all', got %q", scope))
	})
}

// parseUint64CSV splits a comma-separated string of integers.
func parseUint64CSV(s string) ([]uint64, error) {
	parts := strings.Split(s, ",")
	result := make([]uint64, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		v, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid id %q: %w", part, err)
		}
		result = append(result, v)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("empty list")
	}
	return result, nil
}
