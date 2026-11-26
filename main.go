// main.go — RTCVPN Server (Go). Servidor de túnel WebRTC sobre toDus.
//
// Arquitectura (v2 — señalización 100% XMPP):
//   - Pool de 4 teléfonos toDus (JWT + XMPP + TURN creds).
//   - La APK NO usa HTTP contra el servidor: manda "CALL:<apk_phone>" por
//     XMPP al número del servidor, el pool asigna un teléfono libre y
//     responde "ACCEPT:{server_phone, server_jid, turn}" (o "BUSY:{position}"
//     si todos ocupados → luego "CHANNEL_FREE:<phone>").
//   - Listener XMPP por teléfono: procesa "CALL:..." y "OFFER:..." del APK y
//     responde "ACCEPT:..."/"ANSWER:...".
//   - TURN siempre forzado (iceTransportPolicy=Relay): solo se necesitan
//     conexiones SALIENTES (im.todus.cu + TURN), ninguna IP pública del
//     servidor ni puertos de entrada.
//   - HTTP API local (127.0.0.1) solo para diagnóstico: /status, /health.
//     POST /connect y /disconnect se mantienen por compatibilidad.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	pool    *PhonePool
	tunnels sync.Map // server_phone → *WebRtcTunnel
)

type connectReq struct {
	ApkPhone string `json:"apk_phone"`
}

type connectResp struct {
	ServerPhone string     `json:"server_phone"`
	ServerJID   string     `json:"server_jid"`
	Turn        *TurnCreds `json:"turn"`
}

type errorResp struct {
	Error            string `json:"error"`
	Message          string `json:"message"`
	WaitlistPosition int    `json:"waitlist_position,omitempty"`
}

type disconnectReq struct {
	ServerPhone string `json:"server_phone"`
}

// Respuestas XMPP del protocolo CALL (v2 — señalización 100% XMPP).
type xmppAcceptResp struct {
	ServerPhone string     `json:"server_phone"`
	ServerJID   string     `json:"server_jid"`
	Turn        *TurnCreds `json:"turn"`
}

type xmppBusyResp struct {
	Position int    `json:"position"`
	Message  string `json:"message"`
}

func main() {
	phonesFlag := flag.String("phones", "5350256349,5350155246,5351417372,5351430352", "Teléfonos del pool (separados por coma)")
	port := flag.Int("port", 8080, "Puerto HTTP API")
	jwtInterval := flag.Int("jwt-check-interval", 300, "Intervalo de revalidación JWT (segundos)")
	flag.Parse()

	// Logger
	lvl := slog.LevelInfo
	if os.Getenv("RUST_LOG") == "debug" || os.Getenv("LOG_LEVEL") == "debug" {
		lvl = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})))

	phones := strings.Split(*phonesFlag, ",")
	for i := range phones {
		phones[i] = strings.TrimSpace(phones[i])
	}
	slog.Info("RTCVPN server arrancando", "phones", phones, "count", len(phones))

	pool = NewPhonePool(phones)
	pool.InitAll()
	free := 0
	for _, s := range pool.slots {
		s.mu.RLock()
		if s.state == StateFree {
			free++
		}
		s.mu.RUnlock()
	}
	slog.Info("teléfonos listos", "free", free, "total", len(pool.slots))

	// Listener XMPP por teléfono
	for _, s := range pool.slots {
		go xmppListenerLoop(s.Phone)
	}

	// Revalidación JWT periódica
	go func() {
		t := time.NewTicker(time.Duration(*jwtInterval) * time.Second)
		defer t.Stop()
		for range t.C {
			for _, s := range pool.slots {
				pool.MaybeRevalidate(s)
			}
		}
	}()

	// Detección de sesiones zombis: un kick de toDus (doble login con otra
	// instancia del server, p.ej. el runner de Actions auto-relanzado sin
	// morir el anterior) deja la sesión TCP viva pero el servidor ya no
	// entrega stanzas — sin error ni mensajes: el CALL del APK llega al
	// vacío y nunca hay respuesta. Un IQ last-seen a sí mismo cada 90 s
	// lo detecta: sin respuesta en 15 s → Kill → el listener reconecta.
	go func() {
		t := time.NewTicker(90 * time.Second)
		defer t.Stop()
		for range t.C {
			for _, s := range pool.slots {
				xmpp := pool.GetXmpp(s.Phone)
				if xmpp == nil || xmpp.IsClosed() {
					continue
				}
				if _, err := xmpp.SendIQ(xmpp.JID(), "<query xmlns='todus:last:2'/>"); err != nil {
					slog.Warn("sesión zombi detectada — matando para reconectar",
						"phone", s.Phone, "err", err)
					xmpp.Kill()
				}
			}
		}
	}()

	// HTTP API
	mux := http.NewServeMux()
	mux.HandleFunc("/connect", handleConnect)
	mux.HandleFunc("/disconnect", handleDisconnect)
	mux.HandleFunc("/status", handleStatus)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	addr := fmt.Sprintf("0.0.0.0:%d", *port)
	slog.Info("HTTP API escuchando", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		slog.Error("HTTP server falló", "err", err)
		os.Exit(1)
	}
}

func handleConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req connectReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid json"})
		return
	}
	slog.Info("POST /connect", "apk_phone", req.ApkPhone)

	serverPhone, turn := pool.Acquire(req.ApkPhone)
	if serverPhone != "" {
		writeJSON(w, 200, connectResp{
			ServerPhone: serverPhone,
			ServerJID:   buildJID(serverPhone),
			Turn:        turn,
		})
		return
	}
	// Todos ocupados
	pos := 0
	wl := pool.Waitlist()
	for i, w := range wl {
		if w == req.ApkPhone {
			pos = i + 1
			break
		}
	}
	writeJSON(w, 200, errorResp{
		Error:            "all_busy",
		Message:          "Todos los canales están ocupados por el momento, se te avisará cuando uno esté disponible.",
		WaitlistPosition: pos,
	})
}

func handleDisconnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req disconnectReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid json"})
		return
	}
	slog.Info("POST /disconnect", "server_phone", req.ServerPhone)
	// Cerrar túnel si existe
	if v, ok := tunnels.LoadAndDelete(req.ServerPhone); ok {
		t := v.(*WebRtcTunnel)
		t.Close()
	}
	// Liberar y notificar waitlist
	if next := pool.Release(req.ServerPhone); next != "" {
		notifyChannelFree(req.ServerPhone, next)
	}
	writeJSON(w, 200, map[string]bool{"ok": true})
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	count := 0
	tunnels.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	writeJSON(w, 200, map[string]interface{}{
		"phones":         pool.Status(),
		"waitlist":       pool.Waitlist(),
		"tunnels_active": count,
	})
}

// xmppSendToJID envía un mensaje a un JID usando la sesión XMPP del teléfono
// dado. Es variable para poder inyectar un stub en los tests unitarios.
var xmppSendToJID = func(phone, apkJID, body string) error {
	xmpp := pool.GetXmpp(phone)
	if xmpp == nil {
		return fmt.Errorf("sin sesión XMPP para %s", phone)
	}
	return xmpp.SendMessage(apkJID, body)
}

// sendCallResponse entrega la respuesta del CALL (ACCEPT/BUSY) al APK con
// failover entre sesiones: primero la sesión que recibió el CALL; si está
// muerta o falla el envío, cualquier otra sesión viva del pool.
// FIX: antes la respuesta salía SOLO por la sesión receptora — si esa sesión
// estaba zombi (kickeada por doble login), el ACCEPT se perdía en silencio
// y el APK se quedaba esperando para siempre.
func sendCallResponse(receiverPhone, apkPhone, body string) error {
	apkJID := buildJID(apkPhone)
	phones := []string{receiverPhone}
	for _, s := range pool.slots {
		if s.Phone != receiverPhone {
			phones = append(phones, s.Phone)
		}
	}
	var lastErr error
	for _, ph := range phones {
		if err := xmppSendToJID(ph, apkJID, body); err == nil {
			if ph != receiverPhone {
				slog.Warn("respuesta entregada con failover", "receiver", receiverPhone, "via", ph)
			}
			return nil
		} else {
			lastErr = err
		}
	}
	return fmt.Errorf("ninguna sesión XMPP viva pudo enviar la respuesta: %v", lastErr)
}

// handleCall procesa "CALL:<apk_phone>" recibido por XMPP: asigna un teléfono
// libre del pool y responde ACCEPT (o BUSY + waitlist) a la APK.
func handleCall(apkPhone, receiverPhone string) error {
	if apkPhone == "" {
		return fmt.Errorf("CALL sin teléfono")

	}
	apkJID := buildJID(apkPhone)
	serverPhone, turn := pool.Acquire(apkPhone)
	if serverPhone == "" {
		// Todos ocupados: encolar y avisar por BUSY.
		pos := pool.WaitlistPosition(apkPhone)
		resp, _ := json.Marshal(xmppBusyResp{
			Position: pos,
			Message:  "Todos los canales están ocupados; se te avisará en cuanto quede uno libre.",
		})
		if err := sendCallResponse(receiverPhone, apkPhone, "BUSY:"+string(resp)); err != nil {
			return err
		}
		slog.Info("CALL encolado", "apk", apkJID, "posicion", pos)
		return nil
	}

	resp, err := json.Marshal(xmppAcceptResp{
		ServerPhone: serverPhone,
		ServerJID:   buildJID(serverPhone),
		Turn:        turn,
	})
	if err != nil {
		pool.Release(serverPhone)
		return err
	}
	if err := sendCallResponse(receiverPhone, apkPhone, "ACCEPT:"+string(resp)); err != nil {
		pool.Release(serverPhone)
		return err
	}
	slog.Info("CALL aceptado", "apk", apkJID, "receiver", receiverPhone,
		"assigned", serverPhone, "turn_user", turnUser(turn))
	return nil
}

// turnUser devuelve el username TURN para logging sin pánico si turn es nil.
func turnUser(turn *TurnCreds) string {
	if turn == nil {
		return ""
	}
	return turn.Username
}

// notifyChannelFree envía un mensaje XMPP al APK phone: "CHANNEL_FREE:<server_phone>"
func notifyChannelFree(serverPhone, apkPhone string) {
	xmpp := pool.GetXmpp(serverPhone)
	if xmpp == nil {
		slog.Warn("notifyChannelFree: sin XMPP", "server_phone", serverPhone)
		return
	}
	apkJID := buildJID(apkPhone)
	msg := "CHANNEL_FREE:" + serverPhone
	if err := xmpp.SendMessage(apkJID, msg); err != nil {
		slog.Warn("notifyChannelFree send", "err", err)
	} else {
		slog.Info("notificación CHANNEL_FREE enviada", "to", apkJID, "server_phone", serverPhone)
	}
}

// xmppListenerLoop — loop principal del listener por teléfono.
// Si la sesión muere (kick de otra sesión, corte de red), se reconecta
// automáticamente conservando el estado del slot (ocupación).
func xmppListenerLoop(phone string) {
	slog.Info("XMPP listener arrancado", "phone", phone)
	for {
		xmpp := pool.GetXmpp(phone)
		if xmpp == nil || xmpp.IsClosed() {
			slog.Warn("XMPP no disponible, reconectando", "phone", phone)
			time.Sleep(5 * time.Second)
			if s := pool.find(phone); s != nil {
				if err := pool.ReconnectSession(s); err != nil {
					slog.Error("reconexión XMPP falló", "phone", phone, "err", err)
					time.Sleep(15 * time.Second)
				}
			}
			continue
		}
		msg, ok := xmpp.RecvMessage(60 * time.Second)
		if !ok {
			// msgCh cerrado: sesión muerta; el próximo giro la detecta.
			continue
		}
		if msg == "" {
			// timeout de RecvMessage: sin mensajes nuevos, seguir sin ruido.
			continue
		}
		if err := processSignalingMessage(msg, phone); err != nil {
			slog.Error("signaling msg err", "phone", phone, "err", err)
		}
	}
}

// processSignalingMessage procesa mensajes CALL/OFFER/CLOSED del APK.
func processSignalingMessage(msg, serverPhone string) error {
	if strings.HasPrefix(msg, "CALL:") {
		apkPhone := normalizePhone(strings.TrimSpace(msg[5:]))
		return handleCall(apkPhone, serverPhone)
	}
	if strings.HasPrefix(msg, "OFFER:") {
		sdp := msg[6:]
		slog.Info("OFFER recibido", "server_phone", serverPhone, "sdp_len", len(sdp))
		s := pool.find(serverPhone)
		if s == nil {
			return fmt.Errorf("phone %s no encontrado", serverPhone)
		}
		// FIX TURN expirada: las creds del slot pueden llevar horas cacheadas
		// (TTL real 1 h) — refrescar si está por expirar ANTES de crear el
		// answerer, o el ANSWER sale sin candidatos ICE y el DataChannel del
		// cliente nunca abre.
		turn := pool.MaybeRefreshTurn(s)
		apkPhone := s.apkPhone
		if turn == nil {
			return fmt.Errorf("sin TURN creds para %s", serverPhone)
		}
		// Re-OFFER (rellamada de la APK): cerrar el túnel anterior del slot.
		if v, loaded := tunnels.Load(serverPhone); loaded && v != nil {
			slog.Info("cerrando túnel anterior para re-OFFER", "server_phone", serverPhone)
			v.(*WebRtcTunnel).Close()
			tunnels.Delete(serverPhone)
		}
		// Crear tunnel answerer
		tunnel, err := NewAnswerer(turn)
		if err != nil {
			return err
		}
		// Procesar offer → answer
		answer, err := tunnel.ProcessOffer(sdp)
		if err != nil {
			tunnel.Close()
			return err
		}
		// Enviar ANSWER al APK via XMPP
		xmpp := pool.GetXmpp(serverPhone)
		if xmpp == nil {
			tunnel.Close()
			return fmt.Errorf("XMPP no disponible")
		}
		if apkPhone == "" {
			apkPhone = serverPhone // fallback
		}
		apkJID := buildJID(apkPhone)
		if err := xmpp.SendMessage(apkJID, "ANSWER:"+answer); err != nil {
			tunnel.Close()
			return err
		}
		tunnels.Store(serverPhone, tunnel)
		pool.MarkBusy(serverPhone, apkPhone)
		slog.Info("ANSWER enviado", "to", apkJID, "server_phone", serverPhone)
		// Esperar DataChannel open
		if !tunnel.WaitDataChannelOpen(30 * time.Second) {
			slog.Warn("DataChannel no abrió", "server_phone", serverPhone)
			tunnel.Close()
			tunnels.Delete(serverPhone)
			pool.Release(serverPhone)
		}
		return nil
	}
	if msg == "CLOSED" {
		slog.Info("CLOSED recibido", "server_phone", serverPhone)
		if v, ok := tunnels.LoadAndDelete(serverPhone); ok {
			v.(*WebRtcTunnel).Close()
		}
		if next := pool.Release(serverPhone); next != "" {
			notifyChannelFree(serverPhone, next)
		}
		return nil
	}
	if msg == "READY" {
		slog.Info("READY recibido", "server_phone", serverPhone)
		return nil
	}
	if strings.HasPrefix(msg, "CHANNEL_FREE:") {
		// Ignorar: esto lo enviamos nosotros
		return nil
	}
	slog.Info("mensaje no reconocido", "server_phone", serverPhone, "msg", truncate(msg, 100))
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
