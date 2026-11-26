// tunnel_test.go — tests de integridad del framing y del túnel TCP.
// Reproducen la secuencia del login de Minecraft para verificar que el
// path servidor es lossless (regresión del DecoderException
// clientbound/login_finished reportado a través del túnel).
package main

import (
	"bytes"
	"crypto/rand"
	"net"
	"sync"
	"testing"
	"time"
)

// ─── Framing: round-trip byte-exacto ─────────────────────────────────────────

func TestFrameRoundTrip(t *testing.T) {
	sizes := []int{0, 1, 2, 100, 16383, 16384, 16385, 32768, 65535, 65536, 131072}
	for _, size := range sizes {
		data := make([]byte, size)
		rand.Read(data)
		var wire []byte
		for _, fr := range chunked(7, data) {
			wire = append(wire, fr...)
		}
		// Decodificar todos los frames concatenados (como haría el cliente).
		var got []byte
		off := 0
		for off < len(wire) {
			f := decodeFrame(wire[off:])
			if f == nil {
				t.Fatalf("size=%d: frame indecisible en off=%d", size, off)
			}
			if f.StreamID != 7 || f.Type != FrameData {
				t.Fatalf("size=%d: frame inesperado type=%d sid=%d", size, f.Type, f.StreamID)
			}
			got = append(got, f.Payload...)
			off += f.Consumed
		}
		if !bytes.Equal(data, got) {
			t.Fatalf("size=%d: corrupción — enviados %d bytes, recibidos %d",
				size, len(data), len(got))
		}
	}
}

// Simula el S→C del login de MC: set_compression + login_finished llegando
// en ráfagas TCP de tamaño variable — el server trocea por Reads y el
// reensamblado del cliente debe ser byte-exacto.
func TestMinecraftLoginBurstIntegridad(t *testing.T) {
	// Paquetes sintéticos estilo MC: [len varint][id varint][payload]
	mcPacket := func(id byte, payload []byte) []byte {
		body := append([]byte{id}, payload...)
		// varint de longitud (asumimos < 128 para el test)
		return append([]byte{byte(len(body))}, body...)
	}
	setCompression := mcPacket(0x03, []byte{0x80, 0x02}) // threshold 256 varint
	loginFinished := mcPacket(0x02, bytes.Repeat([]byte{0x41, 0x00}, 64))
	handshake := mcPacket(0x00, []byte("localhost\x00\xff\xfd"))
	loginStart := mcPacket(0x00, []byte("Nyxthor"))

	stream := bytes.Join([][]byte{handshake, loginStart, setCompression, loginFinished}, nil)

	for _, readSize := range []int{1, 7, 64, 511, 4096, 16384, 65536} {
		var wire []byte
		for off := 0; off < len(stream); off += readSize {
			end := off + readSize
			if end > len(stream) {
				end = len(stream)
			}
			for _, fr := range chunked(1, stream[off:end]) {
				wire = append(wire, fr...)
			}
		}
		var got []byte
		off := 0
		for off < len(wire) {
			f := decodeFrame(wire[off:])
			if f == nil {
				t.Fatalf("readSize=%d: frame indecisible", readSize)
			}
			got = append(got, f.Payload...)
			off += f.Consumed
		}
		if !bytes.Equal(stream, got) {
			t.Fatalf("readSize=%d: stream corrupto (%d vs %d bytes)",
				readSize, len(stream), len(got))
		}
	}
}

// ─── Túnel TCP real (handleOpen + handleIncoming) ────────────────────────────

func newTestTunnel() *WebRtcTunnel {
	return &WebRtcTunnel{
		sendCh:  make(chan []byte, 1024),
		openCh:  make(chan struct{}, 1),
		deadCh:  make(chan struct{}),
		pending: make(map[uint32][][]byte),
	}
}

// drainFrames vacía sendCh acumulando payloads DATA del sid dado.
func drainFrames(t *WebRtcTunnel, sid uint32) []byte {
	var out []byte
	for {
		select {
		case fr := <-t.sendCh:
			f := decodeFrame(fr)
			if f != nil && f.Type == FrameData && f.StreamID == sid {
				out = append(out, f.Payload...)
			}
		default:
			return out
		}
	}
}

// Regresión race OPEN/DATA: el cliente manda OPEN y los bytes del handshake
// de MC inmediatamente, SIN esperar el OPENED — nada debe perderse.
func TestOpenDataRaceSinPerdida(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	target := ln.Addr().String()
	received := make(chan []byte, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		c.SetReadDeadline(time.Now().Add(10 * time.Second))
		buf := make([]byte, 4096)
		n, _ := c.Read(buf)
		received <- buf[:n]
	}()

	tun := newTestTunnel()
	handshake := []byte{0x10, 0x00, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't', 0x00, 0xff, 0xfd}

	// OPEN y DATA en el MISMO mensaje SCTP (peor caso de la carrera).
	var msg []byte
	msg = append(msg, encodeOpen(1, target)...)
	msg = append(msg, encodeData(1, handshake)...)
	tun.handleIncoming(msg)

	select {
	case got := <-received:
		if !bytes.Equal(got, handshake) {
			t.Fatalf("handshake corrupto: %x vs %x", got, handshake)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("el dial+datos no llegó al target — race OPEN/DATA perdió bytes")
	}
}

// Regresión reuso de sid: reabrir el mismo sid no debe dejar que el pipe
// viejo mate al stream nuevo (bytes del nuevo flujo intactos).
func TestReusoDeSidNoCorrompe(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	conns := make(chan net.Conn, 2)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			conns <- c
		}
	}()

	tun := newTestTunnel()
	target := ln.Addr().String()

	// Stream 1 (p. ej. el status ping del curl): escribir ANTES del reuso
	// para que los bytes ya estén en vuelo hacia sendCh.
	tun.handleIncoming(encodeOpen(1, target))
	var c1 net.Conn
	select {
	case c1 = <-conns:
	case <-time.After(5 * time.Second):
		t.Fatal("accept 1 falló")
	}
	payload1 := []byte("status-response-viejo")
	c1.Write(payload1)
	time.Sleep(300 * time.Millisecond) // el pipe viejo lee y encola los bytes

	// Reuso del sid 1 sin CLOSE (conexión nueva del juego): el stream viejo
	// queda superseded (epoch+1, conn vieja cerrada, sin CLOSE espurio).
	tun.handleIncoming(encodeOpen(1, target))
	var c2 net.Conn
	select {
	case c2 = <-conns:
	case <-time.After(5 * time.Second):
		t.Fatal("accept 2 falló")
	}

	// El server responde por la conn nueva: el stream nuevo sigue vivo.
	payload2 := []byte("login_finished-nuevo")
	c2.Write(payload2)

	deadline := time.Now().Add(5 * time.Second)
	var gotAll []byte
	for time.Now().Before(deadline) {
		gotAll = append(gotAll, drainFrames(tun, 1)...)
		if bytes.Contains(gotAll, payload1) && bytes.Contains(gotAll, payload2) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !bytes.Contains(gotAll, payload1) || !bytes.Contains(gotAll, payload2) {
		t.Fatalf("bytes perdidos con reuso de sid: old=%v new=%v",
			bytes.Contains(gotAll, payload1), bytes.Contains(gotAll, payload2))
	}
	// El stream NUEVO debe seguir registrado (el pipe viejo no lo borró).
	if _, ok := tun.streams.Load(uint32(1)); !ok {
		t.Fatal("el pipe viejo borró el stream nuevo (bug de reuso de sid)")
	}
}

// Backpressure: sendFrame ya no descarta — con sendCh lleno bloquea (aquí
// verificamos solo el caso normal + muerte limpia del túnel).
func TestSendFrameMuerteLimpia(t *testing.T) {
	tun := newTestTunnel()
	tun.kill()
	// Tras kill: no panic, no envío.
	done := make(chan struct{})
	go func() {
		tun.sendFrame(encodeData(1, []byte("x")))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sendFrame bloqueó tras kill")
	}
}

// Integridad concurrente: dos streams simultáneos, datos aleatorios,
// reensamblado byte-exacto por sid (simula SOCKS5 + forward a la vez).
func TestDosStreamsConcurrentesIntegros(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	type result struct {
		data []byte
		err  error
	}
	results := make(chan result, 2)
	go func() {
		for i := 0; i < 2; i++ {
			c, err := ln.Accept()
			if err != nil {
				results <- result{err: err}
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				c.SetReadDeadline(time.Now().Add(10 * time.Second))
				// Longitud desconocida a priori: leer hasta deadline. Con
				// timeout y n>0 el dato está completo (el túnel no cierra TCP).
				buf := make([]byte, 70000)
				var n int
				var err error
				for n < 70000 {
					var m int
					m, err = c.Read(buf[n:])
					n += m
					if err != nil {
						break
					}
				}
				results <- result{data: buf[:n], err: err}
			}(c)
		}
	}()

	tun := newTestTunnel()
	target := ln.Addr().String()
	msg1 := make([]byte, 50000)
	msg2 := make([]byte, 70000)
	rand.Read(msg1)
	rand.Read(msg2)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); tun.handleIncoming(encodeOpen(10, target)) }()
	go func() { defer wg.Done(); tun.handleIncoming(encodeOpen(20, target)) }()
	wg.Wait()
	time.Sleep(200 * time.Millisecond)

	// Enviar ambos streams intercalados en un solo mensaje SCTP.
	var wire []byte
	for i := 0; i < len(msg1); i += 997 {
		end := i + 997
		if end > len(msg1) {
			end = len(msg1)
		}
		wire = append(wire, encodeData(10, msg1[i:end])...)
	}
	for i := 0; i < len(msg2); i += 1301 {
		end := i + 1301
		if end > len(msg2) {
			end = len(msg2)
		}
		wire = append(wire, encodeData(20, msg2[i:end])...)
	}
	tun.handleIncoming(wire)

	r1 := <-results
	r2 := <-results
	// EOF/timeout son esperados: el túnel mantiene el TCP abierto y el
	// reader acaba por deadline; lo que importa es n>0 e integridad.
	for _, r := range []result{r1, r2} {
		if len(r.data) == 0 {
			t.Fatalf("stream vacío en el target (err=%v)", r.err)
		}
	}
	got := map[string][]byte{}
	for _, r := range [][]byte{r1.data, r2.data} {
		if len(r) == len(msg1) {
			got["s1"] = r
		} else if len(r) == len(msg2) {
			got["s2"] = r
		}
	}
	if !bytes.Equal(got["s1"], msg1) {
		t.Fatalf("stream 1 corrupto (%d bytes)", len(got["s1"]))
	}
	if !bytes.Equal(got["s2"], msg2) {
		t.Fatalf("stream 2 corrupto (%d bytes)", len(got["s2"]))
	}
}
