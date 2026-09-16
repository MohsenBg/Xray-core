package internet

import (
	sync "sync"

	"github.com/xtls/xray-core/common/net"
)

var dialTracker = struct {
	sync.Mutex
	conns map[net.Conn]struct{}
}{conns: make(map[net.Conn]struct{})}

func TrackDial(c net.Conn) (release func()) {
	dialTracker.Lock()
	dialTracker.conns[c] = struct{}{}
	dialTracker.Unlock()
	return func() {
		dialTracker.Lock()
		delete(dialTracker.conns, c)
		dialTracker.Unlock()
	}
}

// CloseTrackedDialConns force-closes every dial that is still tracked, i.e.
// stuck between TCP connect and dial completion (e.g. a TLS handshake with
// no ServerHello). Closing the raw conn unblocks the parked reader even
// when the dial ctx never fires. Safe to call concurrently; Close on a
// net.Conn is idempotent.
func CloseTrackedDialConns() {
	dialTracker.Lock()
	conns := make([]net.Conn, 0, len(dialTracker.conns))
	for c := range dialTracker.conns {
		conns = append(conns, c)
	}
	dialTracker.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}
