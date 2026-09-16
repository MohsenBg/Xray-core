// Package shared releases process-global transport client state that is
// shared across all core instances in this process: pooled XHTTP/gRPC
// dialers and raw conns stuck mid-dial.
//
// Dial-time timeouts bound how long a dead destination can park resources;
// this package is the explicit scan-end cleanup. It cannot live in package
// core (import cycle via transport/internet/reality), so it lives here as
// a leaf: it is only safe to call when no concurrent Dial is in flight.
package shared

import (
	"time"

	"github.com/xtls/xray-core/common/errors"
	vlessoutbound "github.com/xtls/xray-core/proxy/vless/outbound"
	vmessoutbound "github.com/xtls/xray-core/proxy/vmess/outbound"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/grpc"
	"github.com/xtls/xray-core/transport/internet/splithttp"
)

// ScanTuning carries scanner-friendly transport limits. Zero values leave
// the corresponding upstream default untouched, so plain library users see
// no behavior change.
type ScanTuning struct {
	// DialTimeout caps TCP connect time (upstream default 16s).
	DialTimeout time.Duration
	// MaxConcurrentDials caps concurrent system dials process-wide
	// (upstream default 0 = unlimited).
	MaxConcurrentDials int
	// DialMaxAttempts caps VLESS/VMess dial attempts per dispatch
	// (upstream default 5).
	DialMaxAttempts int
}

// ApplyScanTuning installs t process-wide. Call once when creating the
// scanning service, before any scan runs.
func ApplyScanTuning(t ScanTuning) {
	if t.DialTimeout > 0 {
		internet.SystemDialTimeout = t.DialTimeout
	}
	if t.MaxConcurrentDials > 0 {
		internet.MaxConcurrentDials = t.MaxConcurrentDials
	}
	if t.DialMaxAttempts > 0 {
		vlessoutbound.DialMaxAttempts = t.DialMaxAttempts
		vmessoutbound.DialMaxAttempts = t.DialMaxAttempts
	}
}

// CloseAll closes every shared resource: tracked in-flight dials first so
// parked handshakes exit immediately, then the pooled XHTTP and gRPC
// transports, clearing their global dialer maps.
func CloseAll() error {
	// Unblock stuck dials first; the pools below only free idle conns.
	internet.CloseTrackedDialConns()

	var errs []error
	if err := splithttp.CloseAll(); err != nil {
		errs = append(errs, err)
	}
	if err := grpc.CloseAll(); err != nil {
		errs = append(errs, err)
	}

	return errors.Combine(errs...)
}
