// todus.go — Auth toDus, cliente XMPP, fetch TURN creds y signaling WebRTC.
// Reproduce el protocolo del SDK Python todus-sdk.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	authURL       = "https://auth.todus.cu/v2/auth/token"
	phoneSecret   = "fake1234567890abcdef12345678" // UUID hardcodeado (sin guiones, 32 chars)
	xmppHost      = "im.todus.cu"
	xmppPort      = 1756
	turnJID       = "tturn@im.todus.cu"
)

// TurnCreds — credenciales TURN devueltas por el server toDus.
type TurnCreds struct {
	URIs     []string `json:"uris"`
	Username string   `json:"username"`
	Password string   `json:"password"`
	TTL      int64    `json:"ttl"`
}

type turnURI struct {
	Proto string `json:"proto"`
	URI   string `json:"uri"`
}

type turnCredsRaw struct {
	URIs     []turnURI `json:"uris"`
	Username string    `json:"username"`
	Password string    `json:"password"`
	TTL      int64     `json:"ttl"`
}

// normalizePhone normaliza un teléfono cubano (10 dígitos nacionales).
func normalizePhone(phone string) string {
	var b strings.Builder
	for _, c := range phone {
		if c >= '0' && c <= '9' {
			b.WriteRune(c)
		}
	}
	cleaned := b.String()
	switch {
	case len(cleaned) == 10 && strings.HasPrefix(cleaned, "53"):
		return cleaned
	case len(cleaned) == 12 && strings.HasPrefix(cleaned, "53"):
		return cleaned[2:]
	default:
		return cleaned
	}
}

func buildJID(phone string) string {
	return normalizePhone(phone) + "@im.todus.cu"
}

// sf codifica un campo protobuf wire-type 2 (length-delimited).
func sf(fieldNumber int, value string) []byte {
	data := []byte(value)
	tag := byte((fieldNumber << 3) | 2)
	return append(append([]byte{tag}, varint(uint64(len(data)))...), data...)
}

func varint(v uint64) []byte {
	var out []byte
	for v > 0x7f {
		out = append(out, byte((v&0x7f)|0x80))
		v >>= 7
	}
	return append(out, byte(v&0x7f))
}

// loginWithPhoneOnly hace login solo con número. Devuelve el JWT.
func loginWithPhoneOnly(phone string) (string, error) {
	p := normalizePhone(phone)
	body := append(sf(1, p), sf(2, phoneSecret)...)

	req, err := http.NewRequest("POST", authURL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("content-type", "application/octet-stream")
	req.Header.Set("user-agent", "ToDus 2.1.1")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("login HTTP: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("login HTTP %d: %s", resp.StatusCode, string(raw))
	}
	re := regexp.MustCompile(`eyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+`)
	m := re.Find(raw)
	if m == nil {
		return "", fmt.Errorf("JWT no encontrado en respuesta: %s", string(raw))
	}
	slog.Info("auth OK", "phone", p, "token_len", len(m))
	return string(m), nil
}

// jwtExp decodifica el exp del JWT (sin verificar firma). Unix timestamp.
func jwtExp(token string) int64 {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return 0
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		// intentar con padding
		s := parts[1]
		if pad := 4 - len(s)%4; pad < 4 {
			s += strings.Repeat("=", pad)
		}
		payload, err = base64.URLEncoding.DecodeString(s)
		if err != nil {
			return 0
		}
	}
	var j map[string]interface{}
	if err := json.Unmarshal(payload, &j); err != nil {
		return 0
	}
	if v, ok := j["exp"].(float64); ok {
		return int64(v)
	}
	return 0
}

// md5Hex devuelve el hash MD5 en hex (lo usa el SDK para el resource XMPP).
func md5Hex(s string) string {
	h := md5.Sum([]byte(s))
	return fmt.Sprintf("%x", h)
}

func randomID(n int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	rand.Read(b)
	for i := range b {
		b[i] = chars[int(b[i])%len(chars)]
	}
	return string(b)
}

func escapeXML(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "'", "&apos;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}

func unescapeXML(s string) string {
	s = strings.ReplaceAll(s, "&amp;", "&")
	s = strings.ReplaceAll(s, "&lt;", "<")
	s = strings.ReplaceAll(s, "&gt;", ">")
	s = strings.ReplaceAll(s, "&apos;", "'")
	s = strings.ReplaceAll(s, "&quot;", "\"")
	return s
}

// ---------- XMPP Client ----------

// XmppClient — sesión XMPP viva sobre TLS con loop de lectura.
type XmppClient struct {
	phone string
	jid   string
	token string
	conn  *tls.Conn
	writeMu sync.Mutex
	msgCh chan string // mensajes entrantes (body de <m>)
	iqCh  chan string // IQ responses (XML completo)
	done  chan struct{}
}

// ConnectXmpp conecta, hace handshake (SASL PLAIN + bind + presence) y arranca el reader.
func ConnectXmpp(phone, token string) (*XmppClient, error) {
	p := normalizePhone(phone)
	jid := buildJID(p)
	slog.Info("XMPP conectando", "host", xmppHost, "port", xmppPort, "jid", jid)

	// TCP connect
	rawConn, err := net.DialTimeout("tcp", net.JoinHostPort(xmppHost, strconv.Itoa(xmppPort)), 15*time.Second)
	if err != nil {
		return nil, fmt.Errorf("XMPP TCP: %w", err)
	}

	// TLS con verificación desactivada (cert LE, algunas ROMs no lo tienen)
	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         xmppHost,
		MinVersion:         tls.VersionTLS12,
	}
	conn := tls.Client(rawConn, tlsConf)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := conn.HandshakeContext(ctx); err != nil {
		rawConn.Close()
		return nil, fmt.Errorf("XMPP TLS: %w", err)
	}
	slog.Debug("XMPP TLS OK", "version", conn.ConnectionState().Version)

	x := &XmppClient{
		phone: p,
		jid:   jid,
		token: token,
		conn:  conn,
		msgCh: make(chan string, 256),
		iqCh:  make(chan string, 64),
		done:  make(chan struct{}),
	}

	// Handshake
	if err := x.handshake(); err != nil {
		conn.Close()
		return nil, err
	}

	// Reader loop en goroutine
	go x.readLoop()

	slog.Info("XMPP conectado y listo", "jid", jid)
	return x, nil
}

func (x *XmppClient) handshake() error {
	// 1. Stream open
	x.writeRaw("<stream:stream xmlns='jc' o='im.todus.cu' xmlns:stream='x1' v='1.0'>")
	if _, err := x.readUntil("<stream:features>"); err != nil {
		return fmt.Errorf("stream open: %w", err)
	}

	// 2. SASL PLAIN
	authstr := append([]byte{0}, []byte(x.phone)...)
	authstr = append(authstr, 0)
	authstr = append(authstr, []byte(x.token)...)
	b64 := base64.StdEncoding.EncodeToString(authstr)
	x.writeRaw("<ah xmlns='ah:ns' e='PLAIN'>" + b64 + "</ah>")

	resp, err := x.readUntilAny("<ok", "<not-authorized", "<failure")
	if err != nil {
		return fmt.Errorf("SASL read: %w", err)
	}
	if strings.Contains(resp, "<not-authorized") || strings.Contains(resp, "<failure") {
		return fmt.Errorf("XMPP auth rechazada (token inválido/expirado)")
	}
	slog.Debug("XMPP SASL OK")

	// 3. Stream restart
	x.writeRaw("<stream:stream xmlns='jc' o='im.todus.cu' xmlns:stream='x1' v='1.0'>")
	if _, err := x.readUntil("<stream:features>"); err != nil {
		return fmt.Errorf("stream restart: %w", err)
	}

	// 4. Bind
	bindID := randomID(12)
	resource := md5Hex(x.phone) + "_Android"
	x.writeRaw("<iq i='" + bindID + "' t='set'><b1 xmlns='x4'><re>" + resource + "</re></b1></iq>")
	bindResp, err := x.readUntil("</iq>")
	if err != nil {
		return fmt.Errorf("bind: %w", err)
	}
	slog.Debug("XMPP bind OK", "resp_prefix", truncate(bindResp, 160))

	// 5. Presence
	x.writeRaw("<p/>")
	return nil
}

func (x *XmppClient) writeRaw(s string) error {
	x.writeMu.Lock()
	defer x.writeMu.Unlock()
	x.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := x.conn.Write([]byte(s))
	return err
}

// readUntil lee hasta encontrar terminator (o timeout 15s).
func (x *XmppClient) readUntil(terminator string) (string, error) {
	br := bufio.NewReader(x.conn)
	var sb strings.Builder
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		x.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		b, err := br.ReadByte()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return "", err
		}
		sb.WriteByte(b)
		if strings.Contains(sb.String(), terminator) {
			return sb.String(), nil
		}
	}
	return "", fmt.Errorf("timeout esperando %s", terminator)
}

// readUntilAny lee hasta encontrar cualquiera de los terminators.
func (x *XmppClient) readUntilAny(terminators ...string) (string, error) {
	br := bufio.NewReader(x.conn)
	var sb strings.Builder
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		x.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		b, err := br.ReadByte()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return "", err
		}
		sb.WriteByte(b)
		s := sb.String()
		for _, t := range terminators {
			if strings.Contains(s, t) {
				return s, nil
			}
		}
	}
	return "", fmt.Errorf("timeout esperando uno de %v", terminators)
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// readLoop — procesa stanzas entrantes y las dispatcha a msgCh/iqCh.
func (x *XmppClient) readLoop() {
	br := bufio.NewReader(x.conn)
	var accum strings.Builder
	for {
		select {
		case <-x.done:
			return
		default:
		}
		x.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		b, err := br.ReadByte()
		if err != nil {
			slog.Warn("XMPP reader fin", "jid", x.jid, "err", err)
			close(x.msgCh)
			close(x.iqCh)
			return
		}
		accum.WriteByte(b)
		for {
			stanza, rest := extractStanza(accum.String())
			if stanza == "" {
				break
			}
			accum.Reset()
			accum.WriteString(rest)
			x.dispatchStanza(stanza)
		}
	}
}

func (x *XmppClient) dispatchStanza(stanza string) {
	if strings.HasPrefix(stanza, "<m ") || strings.HasPrefix(stanza, "<m>") {
		bodyStart := strings.Index(stanza, ">")
		bodyEnd := strings.LastIndex(stanza, "</m>")
		if bodyStart >= 0 && bodyEnd > bodyStart {
			body := unescapeXML(stanza[bodyStart+1 : bodyEnd])
			select {
			case x.msgCh <- body:
			default:
				slog.Warn("XMPP msgCh lleno, descartando")
			}
		}
	} else if strings.HasPrefix(stanza, "<iq ") || strings.HasPrefix(stanza, "<iq>") {
		select {
		case x.iqCh <- stanza:
		default:
			slog.Warn("XMPP iqCh lleno, descartando")
		}
	}
}

// extractStanza extrae la primera stanza XML completa de s. Devuelve (stanza, rest).
func extractStanza(s string) (string, string) {
	start := strings.Index(s, "<")
	if start < 0 {
		return "", s
	}
	after := s[start:]
	// Self-closing
	if sc := strings.Index(after, "/>"); sc >= 0 {
		// verificar que no haya '>' antes del '/>'
		if gt := strings.Index(after, ">"); gt < 0 || sc < gt {
			total := sc + 2
			return after[:total], s[:start] + after[total:]
		}
	}
	closeOpen := strings.Index(after, ">")
	if closeOpen < 0 {
		return "", s
	}
	tagName := after[1:closeOpen]
	if sp := strings.IndexAny(tagName, " \t"); sp >= 0 {
		tagName = tagName[:sp]
	}
	tagName = strings.TrimSuffix(tagName, "/")
	closeTag := "</" + tagName + ">"
	end := strings.Index(after, closeTag)
	if end < 0 {
		return "", s
	}
	total := end + len(closeTag)
	return after[:total], s[:start] + after[total:]
}

// SendMessage envía un mensaje XMPP <m> al peer_jid.
func (x *XmppClient) SendMessage(toJID, body string) error {
	id := randomID(16)
	stanza := "<m o='" + toJID + "' t='c' i='" + id + "' xmlns='jc'>" + escapeXML(body) + "</m>"
	return x.writeRaw(stanza)
}

// SendIQ envía un IQ y espera la respuesta.
func (x *XmppClient) SendIQ(to, payload string) (string, error) {
	id := randomID(12)
	iq := "<iq i='" + id + "' t='get' o='" + to + "'>" + payload + "</iq>"
	x.writeRaw(iq)
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-deadline.C:
			return "", fmt.Errorf("IQ timeout")
		case resp, ok := <-x.iqCh:
			if !ok {
				return "", fmt.Errorf("IQ channel cerrado")
			}
			if strings.Contains(resp, id) {
				return resp, nil
			}
		}
	}
}

// FetchTurnCreds solicita credenciales TURN al server toDus.
func (x *XmppClient) FetchTurnCreds() (*TurnCreds, error) {
	payload := "<query xmlns='todus:turn:cred' userId='" + x.jid + "'/>"
	resp, err := x.SendIQ(turnJID, payload)
	if err != nil {
		return nil, fmt.Errorf("TURN IQ: %w", err)
	}
	re := regexp.MustCompile(`(?s)<query[^>]*xmlns=['"]todus:turn:cred['"][^>]*>(.*?)</query>`)
	m := re.FindStringSubmatch(resp)
	if m == nil {
		return nil, fmt.Errorf("TURN IQ sin <query>: %s", truncate(resp, 200))
	}
	b64content := strings.TrimSpace(m[1])
	decoded, err := base64.StdEncoding.DecodeString(b64content)
	if err != nil {
		return nil, fmt.Errorf("TURN base64 decode: %w", err)
	}
	var raw turnCredsRaw
	if err := json.Unmarshal(decoded, &raw); err != nil {
		return nil, fmt.Errorf("TURN JSON: %w", err)
	}
	creds := &TurnCreds{
		Username: raw.Username,
		Password: raw.Password,
		TTL:      raw.TTL,
	}
	for _, u := range raw.URIs {
		uri := u.URI
		if u.Proto == "tcp" && !strings.Contains(uri, "transport=tcp") {
			uri += "?transport=tcp"
		}
		creds.URIs = append(creds.URIs, uri)
	}
	slog.Info("TURN creds OK", "uris", creds.URIs, "user", creds.Username, "ttl", creds.TTL)
	return creds, nil
}

// RecvMessage espera el siguiente mensaje entrante con timeout.
func (x *XmppClient) RecvMessage(timeout time.Duration) (string, bool) {
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case msg, ok := <-x.msgCh:
		return msg, ok
	case <-t.C:
		return "", true
	case <-x.done:
		return "", false
	}
}

func (x *XmppClient) JID() string  { return x.jid }
func (x *XmppClient) Phone() string { return x.phone }
func (x *XmppClient) Token() string { return x.token }

// IsClosed indica si la sesión XMPP está cerrada.
func (x *XmppClient) IsClosed() bool {
	select {
	case <-x.done:
		return true
	default:
		return false
	}
}

func (x *XmppClient) Close() {
	select {
	case <-x.done:
	default:
		close(x.done)
	}
	x.conn.Close()
}
