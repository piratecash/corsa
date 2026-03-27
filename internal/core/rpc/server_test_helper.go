package rpc

import "net/http"

// Test delegates to the underlying Fiber app's Test method.
// This is intended for unit testing only.
func (s *Server) Test(req *http.Request) (*http.Response, error) {
	return s.app.Test(req)
}
