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
	pc        *webrtc.PeerConnection
	dc        *webrtc.DataChannel
	dcMu      sync.Mutex
	streams   sync.Map // streamID (uint32) → *serverStream
	sendCh    chan []byte
	openCh    chan struct{}
	closed    bool
	closedMu  sync.Mutex
	deadCh    chan struct{}
	deadOnce  sync.Once
	pendingMu sync.Mutex
	pending   map[uint32][][]byte // DATA llegados antes del stream (race OPEN/DATA)
}

type serverStream struct {
	writer io.Writer
	conn   net.Conn
	epoch  uint64 // detecta reuso de sid: el pipe viejo no mata al stream nuevo
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
		pc:      pc,
		sendCh:  make(chan []byte, 1024),
		openCh:  make(chan struct{}, 1),
		deadCh:  make(chan struct{}),
		pending: make(map[uint32][][]byte),
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

// kill cierra el túnel una sola vez: deadCh desbloquea a sendFrame/writerLoop.
func (t *WebRtcTunnel) kill() {
	t.deadOnce.Do(func() {
		t.markClosed()
		close(t.deadCh)
	})
}

func (t *WebRtcTunnel) isDead() bool {
	select {
	case <-t.deadCh:
		return true
	default:
		return false
	}
}

// writerLoop consume sendCh y envía frames al DataChannel.
func (t *WebRtcTunnel) writerLoop() {
	for {
		select {
		case <-t.deadCh:
			return
		case frame := <-t.sendCh:
			t.dcMu.Lock()
			dc := t.dc
			t.dcMu.Unlock()
			if dc == nil {
				continue
			}
			if err := dc.Send(frame); err != nil {
				slog.Warn("DC send error", "err", err)
				t.kill()
				return
			}
		}
	}
}

// sendFrame encola un frame al DataChannel con BACKPRESSURE, no con descarte.
// FIX crítico: antes hacía `select/default` — si sendCh (1024) se llenaba
// (el peer deja de leer: SCTP saturado, juego lento), el frame se DESCARTABA
// EN SILENCIO. Perder N bytes de un stream TCP corrompe todos los paquetes
// posteriores de Minecraft: síntoma observado = DecoderException en
// clientbound/login_finished (el framing se re-alinea por azar y el contenido
// llega corrupto). Ahora bloquea hasta 60 s; si no drena, el túnel está muerto
// y se cierra de forma explícita (el cliente reconecta y reabre streams).
func (t *WebRtcTunnel) sendFrame(frame []byte) {
	if t.isDead() {
		return
	}
	select {
	case t.sendCh <- frame:
	case <-time.After(60 * time.Second):
		slog.Error("sendCh saturado 60s — el peer no lee; cerrando túnel",
			"queued", len(t.sendCh))
		t.kill()
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
			} else {
				// FIX race OPEN/DATA: handleOpen es asíncrono (dial de 15 s);
				// si el cliente no espera el OPENED, sus primeros frames DATA
				// llegaban ANTES del dial y se descartaban sin log — el
				// handshake de Minecraft se perdía entero. Se bufferean y se
				// vuelcan al socket cuando el stream quede listo.
				t.bufferPending(f.StreamID, f.Payload)
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

// bufferPending guarda payloads DATA de un sid cuyo stream aún no existe.
// Tope: 1024 payloads (16 MB) por sid — si se pasa, el cliente está roto y
// se ignora lo restante en vez de comer memoria.
func (t *WebRtcTunnel) bufferPending(sid uint32, payload []byte) {
	t.pendingMu.Lock()
	defer t.pendingMu.Unlock()
	if len(t.pending[sid]) >= 1024 {
		slog.Error("pending overflow para sid — descartando stream", "sid", sid)
		return
	}
	p := make([]byte, len(payload))
	copy(p, payload)
	t.pending[sid] = append(t.pending[sid], p)
}

// flushPending escribe (y limpia) los payloads pendientes de un sid al
// socket TCP del stream recién creado — es data C→S, va al target, NO al
// DataChannel.
func (t *WebRtcTunnel) flushPending(sid uint32, w io.Writer) {
	t.pendingMu.Lock()
	payloads := t.pending[sid]
	delete(t.pending, sid)
	t.pendingMu.Unlock()
	for _, p := range payloads {
		if _, err := w.Write(p); err != nil {
			slog.Warn("flush pending write", "sid", sid, "err", err)
			return
		}
	}
}

// dropPending descarta los pendientes de un sid (dial fallido).
func (t *WebRtcTunnel) dropPending(sid uint32) {
	t.pendingMu.Lock()
	delete(t.pending, sid)
	t.pendingMu.Unlock()
}

// handleOpen conecta TCP al target y arranca pipe bidireccional.
func (t *WebRtcTunnel) handleOpen(sid uint32, target string) {
	parts := splitHostPort(target)
	if parts == nil {
		slog.Warn("OPEN target inválido", "sid", sid, "target", target)
		t.sendFrame(encodeErr(sid, ErrGeneral, "target inválido"))
		t.dropPending(sid)
		return
	}
	host, port := parts[0], parts[1]
	dialer := net.Dialer{Timeout: 15 * time.Second}
	conn, err := dialer.Dial("tcp", net.JoinHostPort(host, port))
	if err != nil {
		slog.Warn("OPEN connect", "sid", sid, "target", target, "err", err)
		t.sendFrame(encodeErr(sid, ErrConnRefused, err.Error()))
		t.dropPending(sid)
		return
	}

	// FIX reuso de sid: si el cliente reabre el mismo sid (conexión local
	// nueva del forward fijo) sin haber mandado CLOSE, el pipe VIEJO aún
	// vive y su Close/EOF mataría al stream NUEVO. Se increments el epoch:
	// el pipe viejo ve que ya no es el dueño del sid y muere en silencio.
	var epoch uint64
	if v, loaded := t.streams.Load(sid); loaded {
		old := v.(*serverStream)
		old.conn.Close()
		epoch = old.epoch + 1
	}

	ss := &serverStream{writer: conn, conn: conn, epoch: epoch}
	t.streams.Store(sid, ss)
	t.sendFrame(encodeOpened(sid))
	t.flushPending(sid, conn)
	slog.Info("OPENED", "sid", sid, "host", host, "port", port)

	// Pipe: TCP reader → DATA frames
	go func() {
		buf := make([]byte, MaxPayload)
		for {
			n, rerr := conn.Read(buf)
			if n > 0 {
				for _, frame := range chunked(sid, buf[:n]) {
					t.sendFrame(frame)
				}
			}
			if rerr != nil {
				break
			}
		}
		// Solo el dueño actual del sid anuncia el CLOSE — atómicamente:
		// CompareAndDelete elimina la entrada SOLO si sigue siendo MI
		// stream (mismo puntero). El Load+Delete anterior era una carrera
		// TOCTOU: el pipe viejo podía borrar la entrada NUEVA si el Store
		// del reuso caía entre su Load y su Delete.
		if t.streams.CompareAndDelete(sid, ss) {
			t.sendFrame(encodeClose(sid))
		}
		conn.Close()
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
	t.kill()
	if t.pc != nil {
		t.pc.Close()
	}
}
