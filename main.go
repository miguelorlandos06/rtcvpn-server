// main.go — RTCVPN Server (Go). Servidor de túnel WebRTC sobre toDus.
//
// Arquitectura:
// - Pool de 4 teléfonos toDus (JWT + XMPP + TURN creds).
// - HTTP API: POST /connect asigna teléfono libre, POST /disconnect libera,
//   GET /status estado del pool.
// - Listener XMPP por teléfono: procesa mensajes "OFFER:..." del APK y
//   responde "ANSWER:..." con el SDP.
// - TURN siempre forzado (iceTransportPolicy=Relay).
// - Si los 4 están ocupados, el APK se encola y recibe notificación XMPP
//   "CHANNEL_FREE:<phone>" cuando uno se libera.
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
	pool     *PhonePool
	tunnels  sync.Map // server_phone → *WebRtcTunnel
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
func xmppListenerLoop(phone string) {
	slog.Info("XMPP listener arrancado", "phone", phone)
	for {
		xmpp := pool.GetXmpp(phone)
		if xmpp == nil || xmpp.IsClosed() {
			slog.Warn("XMPP no disponible, reintentando", "phone", phone)
			time.Sleep(5 * time.Second)
			if s := pool.find(phone); s != nil {
				pool.MaybeRevalidate(s)
			}
			continue
		}
		msg, ok := xmpp.RecvMessage(60 * time.Second)
		if !ok {
			slog.Debug("XMPP sin mensajes (60s)", "phone", phone)
			continue
		}
		if err := processSignalingMessage(msg, phone); err != nil {
			slog.Error("signaling msg err", "phone", phone, "err", err)
		}
	}
}

// processSignalingMessage procesa mensajes OFFER/CLOSED del APK.
func processSignalingMessage(msg, serverPhone string) error {
	if strings.HasPrefix(msg, "OFFER:") {
		sdp := msg[6:]
		slog.Info("OFFER recibido", "server_phone", serverPhone, "sdp_len", len(sdp))
		s := pool.find(serverPhone)
		if s == nil {
			return fmt.Errorf("phone %s no encontrado", serverPhone)
		}
		s.mu.RLock()
		turn := s.turnCreds
		apkPhone := s.apkPhone
		s.mu.RUnlock()
		if turn == nil {
			return fmt.Errorf("sin TURN creds para %s", serverPhone)
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
