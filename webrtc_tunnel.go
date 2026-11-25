// webrtc_tunnel.go — PeerConnection con DataChannel + TURN forzado + SOCKS5 dialer.
package main

import (
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	webrtc "github.com/pion/webrtc/v3"
)

// WebRtcTunnel — tunnel server-side: PeerConnection + DataChannel + multiplexación TCP.
type WebRtcTunnel struct {
	pc       *webrtc.PeerConnection
	dc       *webrtc.DataChannel
	dcMu     sync.Mutex
	streams  sync.Map // streamID (uint32) → *serverStream
	sendCh   chan []byte
	openCh   chan struct{}
	closed   bool
	closedMu sync.Mutex
}

type serverStream struct {
	writer io.Writer
	conn   net.Conn
}

// NewAnswerer crea el tunnel como answerer (recibe OFFER del APK).
func NewAnswerer(turn *TurnCreds) (*WebRtcTunnel, error) {
	iceServers := make([]webrtc.ICEServer, 0, len(turn.URIs))
	for _, u := range turn.URIs {
		iceServers = append(iceServers, webrtc.ICEServer{
			URLs:       []string{u},
			Username:   turn.Username,
			Credential: turn.Password,
		})
	}
	config := webrtc.Configuration{
		ICEServers:         iceServers,
		ICETransportPolicy: webrtc.ICETransportPolicyRelay, // TURN only
		SDPSemantics:       webrtc.SDPSemanticsUnifiedPlan,
	}
	api := webrtc.NewAPI()
	pc, err := api.NewPeerConnection(config)
	if err != nil {
		return nil, err
	}
	slog.Info("PeerConnection creada: iceTransportPolicy=Relay", "turn_servers", len(iceServers))

	t := &WebRtcTunnel{
		pc:     pc,
		sendCh: make(chan []byte, 1024),
		openCh: make(chan struct{}, 1),
	}

	// on_data_channel: cuando el offerer (APK) crea el DC
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		slog.Info("DataChannel entrante", "label", dc.Label())
		t.dcMu.Lock()
		t.dc = dc
		t.dcMu.Unlock()

		dc.OnOpen(func() {
			slog.Info("DataChannel ABIERTO")
			select {
			case t.openCh <- struct{}{}:
			default:
			}
		})

		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			t.handleIncoming(msg.Data)
		})

		dc.OnClose(func() {
			slog.Info("DataChannel cerrado")
			t.markClosed()
		})
	})

	// Writer task: consume sendCh y escribe al DC
	go t.writerLoop()

	return t, nil
}

// ProcessOffer procesa el OFFER del APK y devuelve el ANSWER SDP.
//
// FIX crítico (2026-11-25): antes se devolvía `answer.SDP` justo después de
// SetLocalDescription, es decir, SIN candidatos ICE (pion los junta de forma
// asíncrona). Sin candidatos del lado del server, la conectividad ICE es
// imposible y el DataChannel nunca abre. Ahora se espera a que el gathering
// termine (GatheringCompletePromise) y se devuelve pc.LocalDescription().SDP,
// que ya incluye todos los candidatos relay (vanilla ICE, sin trickle).
func (t *WebRtcTunnel) ProcessOffer(sdpOffer string) (string, error) {
	err := t.pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  sdpOffer,
	})
	if err != nil {
		return "", err
	}
	gatherComplete := webrtc.GatheringCompletePromise(t.pc)
	answer, err := t.pc.CreateAnswer(nil)
	if err != nil {
		return "", err
	}
	if err := t.pc.SetLocalDescription(answer); err != nil {
		return "", err
	}
	// Esperar candidatos (tope 10 s; con TURN relay suele tardar < 2 s).
	select {
	case <-gatherComplete:
	case <-time.After(10 * time.Second):
		slog.Warn("gathering ICE incompleto tras 10s; enviando answer con lo que haya")
	}
	local := t.pc.LocalDescription()
	if local == nil {
		return answer.SDP, nil
	}
	if !strings.Contains(local.SDP, "a=candidate") {
		slog.Warn("answer sin candidatos ICE; la conexión puede fallar")
	}
	return local.SDP, nil
}

// WaitDataChannelOpen espera a que el DC abra (con timeout).
func (t *WebRtcTunnel) WaitDataChannelOpen(timeout time.Duration) bool {
	t.dcMu.Lock()
	if t.dc != nil {
		rs := t.dc.ReadyState()
		if rs == webrtc.DataChannelStateOpen {
			t.dcMu.Unlock()
			return true
		}
	}
	t.dcMu.Unlock()
	select {
	case <-t.openCh:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (t *WebRtcTunnel) markClosed() {
	t.closedMu.Lock()
	t.closed = true
	t.closedMu.Unlock()
}

// writerLoop consume sendCh y envía frames al DataChannel.
func (t *WebRtcTunnel) writerLoop() {
	for frame := range t.sendCh {
		t.dcMu.Lock()
		dc := t.dc
		t.dcMu.Unlock()
		if dc == nil {
			continue
		}
		if err := dc.Send(frame); err != nil {
			slog.Warn("DC send error", "err", err)
			t.markClosed()
			return
		}
	}
}

func (t *WebRtcTunnel) sendFrame(frame []byte) {
	select {
	case t.sendCh <- frame:
	default:
		slog.Warn("sendCh lleno, descartando frame")
	}
}

// handleIncoming procesa frames entrantes del DataChannel.
func (t *WebRtcTunnel) handleIncoming(data []byte) {
	off := 0
	for off < len(data) {
		f := decodeFrame(data[off:])
		if f == nil {
			break
		}
		off += f.Consumed
		switch f.Type {
		case FrameOpen:
			target := string(f.Payload)
			slog.Info("OPEN", "sid", f.StreamID, "target", target)
			go t.handleOpen(f.StreamID, target)
		case FrameData:
			if v, ok := t.streams.Load(f.StreamID); ok {
				ss := v.(*serverStream)
				if _, err := ss.writer.Write(f.Payload); err != nil {
					slog.Warn("DATA write", "sid", f.StreamID, "err", err)
				}
			}
		case FrameClose:
			slog.Info("CLOSE", "sid", f.StreamID)
			if v, ok := t.streams.LoadAndDelete(f.StreamID); ok {
				ss := v.(*serverStream)
				ss.conn.Close()
			}
		case FramePing:
			t.sendFrame(encodePong())
		}
	}
}

// handleOpen conecta TCP al target y arranca pipe bidireccional.
func (t *WebRtcTunnel) handleOpen(sid uint32, target string) {
	parts := splitHostPort(target)
	if parts == nil {
		slog.Warn("OPEN target inválido", "sid", sid, "target", target)
		t.sendFrame(encodeErr(sid, ErrGeneral, "target inválido"))
		return
	}
	host, port := parts[0], parts[1]
	dialer := net.Dialer{Timeout: 15 * time.Second}
	conn, err := dialer.Dial("tcp", net.JoinHostPort(host, port))
	if err != nil {
		slog.Warn("OPEN connect", "sid", sid, "target", target, "err", err)
		t.sendFrame(encodeErr(sid, ErrConnRefused, err.Error()))
		return
	}
	t.streams.Store(sid, &serverStream{writer: conn, conn: conn})
	t.sendFrame(encodeOpened(sid))
	slog.Info("OPENED", "sid", sid, "host", host, "port", port)

	// Pipe: TCP reader → DATA frames
	go func() {
		buf := make([]byte, MaxPayload)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				for _, frame := range chunked(sid, buf[:n]) {
					t.sendFrame(frame)
				}
			}
			if err != nil {
				break
			}
		}
		t.sendFrame(encodeClose(sid))
		if v, ok := t.streams.LoadAndDelete(sid); ok {
			ss := v.(*serverStream)
			ss.conn.Close()
		}
	}()
}

// splitHostPort divide "host:port" devolviendo [host, portStr].
func splitHostPort(target string) []string {
	i := lastIndexByte(target, ':')
	if i < 0 {
		return nil
	}
	host := target[:i]
	port := target[i+1:]
	if host == "" || port == "" {
		return nil
	}
	return []string{host, port}
}

func lastIndexByte(s string, c byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// Close cierra el PeerConnection y libera recursos.
func (t *WebRtcTunnel) Close() {
	t.markClosed()
	close(t.sendCh)
	if t.pc != nil {
		t.pc.Close()
	}
}
