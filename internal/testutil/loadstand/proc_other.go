//go:build !linux

package loadstand

import "context"

type LinuxProcessSampler struct{}

func ReadLinuxIncarnation(context.Context, ProcessID, ProcessUID) (ProcessIncarnation, error) {
	return ProcessIncarnation{}, ErrProcessPlatform
}
func OpenLinuxProcess(context.Context, LinuxProcessExpectation) (*LinuxProcessSampler, error) {
	return nil, ErrProcessPlatform
}
func (*LinuxProcessSampler) Close() error { return nil }
func (*LinuxProcessSampler) Sample(context.Context) (LinuxProcessSample, error) {
	return LinuxProcessSample{}, ErrProcessPlatform
}
