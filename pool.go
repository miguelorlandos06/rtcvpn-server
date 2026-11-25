// pool.go — Pool de teléfonos toDus: gestiona los 4 números, JWT (con
// revalidación), sesiones XMPP y estado de ocupación.
package main

import (
	"log/slog"
	"sync"
	"time"
)

type PhoneState string

const (
	StateFree       PhoneState = "Free"
	StateBusy       PhoneState = "Busy"
	StateConnecting PhoneState = "Connecting"
	StateError      PhoneState = "Error"
)

type PhoneSlot struct {
	Phone     string
	mu        sync.RWMutex
	jwt       string
	jwtExp    int64
	xmpp      *XmppClient
	state     PhoneState
	turnCreds *TurnCreds
	apkPhone  string // cuando está busy/connecting, el APK user asignado
	since     time.Time
}

type PhoneStatus struct {
	Phone          string     `json:"phone"`
	State          PhoneState `json:"state"`
	ApkPhone       string     `json:"apk_phone,omitempty"`
	Since          time.Time  `json:"since,omitempty"`
	TokenExpiresAt int64      `json:"token_expires_at"`
}

type PhonePool struct {
	slots    []*PhoneSlot
	waitlist []string
	wlMu     sync.Mutex
}

func NewPhonePool(phones []string) *PhonePool {
	p := &PhonePool{}
	for _, ph := range phones {
		p.slots = append(p.slots, &PhoneSlot{
			Phone: ph,
			state: StateError,
		})
	}
	return p
}

// InitAll inicializa todos los teléfonos (login + XMPP + TURN).
func (p *PhonePool) InitAll() {
	var wg sync.WaitGroup
	for _, s := range p.slots {
		wg.Add(1)
		go func(s *PhoneSlot) {
			defer wg.Done()
			if err := p.InitPhone(s); err != nil {
				slog.Error("init phone falló", "phone", s.Phone, "err", err)
			}
		}(s)
	}
	wg.Wait()
}

// InitPhone inicializa un teléfono: login + XMPP + TURN creds.
func (p *PhonePool) InitPhone(s *PhoneSlot) error {
	slog.Info("inicializando teléfono", "phone", s.Phone)
	// 1. Login
	jwt, err := loginWithPhoneOnly(s.Phone)
	if err != nil {
		s.mu.Lock()
		s.state = StateError
		s.mu.Unlock()
		return err
	}
	exp := jwtExp(jwt)
	// 2. XMPP connect
	xmpp, err := ConnectXmpp(s.Phone, jwt)
	if err != nil {
		s.mu.Lock()
		s.state = StateError
		s.mu.Unlock()
		return err
	}
	// 3. TURN creds
	turn, err := xmpp.FetchTurnCreds()
	if err != nil {
		xmpp.Close()
		s.mu.Lock()
		s.state = StateError
		s.mu.Unlock()
		return err
	}
	s.mu.Lock()
	s.jwt = jwt
	s.jwtExp = exp
	s.xmpp = xmpp
	s.turnCreds = turn
	s.state = StateFree
	s.mu.Unlock()
	slog.Info("teléfono listo", "phone", s.Phone, "jwt_exp", exp)
	return nil
}

// Acquire asigna un teléfono libre al APK user. Devuelve (server_phone, turn_creds).
// Si todos ocupados, encola al APK y devuelve "".
// Si el APK ya tenía un slot asignado (rellamada tras CHANNEL_FREE, reintento
// o reconexión), devuelve el MISMO slot para no desperdiciar canales.
func (p *PhonePool) Acquire(apkPhone string) (string, *TurnCreds) {
	// Slot ya asignado a este APK (Connecting o Busy): reutilizar.
	for _, s := range p.slots {
		s.mu.RLock()
		if s.apkPhone == apkPhone && s.state != StateError {
			phone := s.Phone
			turn := s.turnCreds
			s.mu.RUnlock()
			slog.Info("reutilizando slot del APK", "server_phone", phone, "apk", apkPhone, "state", p.slotState(phone))
			return phone, turn
		}
		s.mu.RUnlock()
	}
	for _, s := range p.slots {
		s.mu.RLock()
		if s.state == StateFree {
			s.mu.RUnlock()
			s.mu.Lock()
			if s.state != StateFree {
				s.mu.Unlock()
				continue
			}
			s.state = StateConnecting
			s.apkPhone = apkPhone
			turn := s.turnCreds
			s.mu.Unlock()
			slog.Info("asignado teléfono", "server_phone", s.Phone, "apk", apkPhone)
			return s.Phone, turn
		}
		s.mu.RUnlock()
	}
	// Ninguno libre → encolar
	p.wlMu.Lock()
	for _, w := range p.waitlist {
		if w == apkPhone {
			p.wlMu.Unlock()
			return "", nil
		}
	}
	p.waitlist = append(p.waitlist, apkPhone)
	pos := len(p.waitlist)
	p.wlMu.Unlock()
	slog.Warn("waitlist encolado", "apk", apkPhone, "posicion", pos)
	return "", nil
}

// MarkBusy marca un teléfono como Busy (tunnel activo).
func (p *PhonePool) MarkBusy(serverPhone, apkPhone string) {
	if s := p.find(serverPhone); s != nil {
		s.mu.Lock()
		s.state = StateBusy
		s.apkPhone = apkPhone
		s.since = time.Now()
		s.mu.Unlock()
	}
}

// Release libera un teléfono. Devuelve el siguiente APK en waitlist (para notificar).
func (p *PhonePool) Release(serverPhone string) string {
	s := p.find(serverPhone)
	if s == nil {
		return ""
	}
	s.mu.Lock()
	s.state = StateFree
	s.apkPhone = ""
	s.mu.Unlock()
	slog.Info("teléfono liberado", "phone", serverPhone)

	p.wlMu.Lock()
	if len(p.waitlist) > 0 {
		next := p.waitlist[0]
		p.waitlist = p.waitlist[1:]
		p.wlMu.Unlock()
		slog.Info("notificando a APK siguiente en waitlist", "apk", next, "server_phone", serverPhone)
		return next
	}
	p.wlMu.Unlock()
	return ""
}

func (p *PhonePool) find(phone string) *PhoneSlot {
	for _, s := range p.slots {
		if s.Phone == phone {
			return s
		}
	}
	return nil
}

// GetXmpp devuelve la sesión XMPP de un teléfono.
func (p *PhonePool) GetXmpp(phone string) *XmppClient {
	s := p.find(phone)
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.xmpp
}

// Status devuelve el estado de todos los teléfonos.
func (p *PhonePool) Status() []PhoneStatus {
	var out []PhoneStatus
	for _, s := range p.slots {
		s.mu.RLock()
		st := PhoneStatus{
			Phone:          s.Phone,
			State:          s.state,
			ApkPhone:       s.apkPhone,
			Since:          s.since,
			TokenExpiresAt: s.jwtExp,
		}
		s.mu.RUnlock()
		out = append(out, st)
	}
	return out
}

// Waitlist devuelve la lista de APKs esperando.
func (p *PhonePool) Waitlist() []string {
	p.wlMu.Lock()
	defer p.wlMu.Unlock()
	cp := make([]string, len(p.waitlist))
	copy(cp, p.waitlist)
	return cp
}

// WaitlistPosition devuelve la posición (1-based) del APK en la lista de
// espera, o 0 si no está encolado.
func (p *PhonePool) WaitlistPosition(apkPhone string) int {
	p.wlMu.Lock()
	defer p.wlMu.Unlock()
	for i, w := range p.waitlist {
		if w == apkPhone {
			return i + 1
		}
	}
	return 0
}

// slotState devuelve el estado de un teléfono (para logging).
func (p *PhonePool) slotState(phone string) PhoneState {
	s := p.find(phone)
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

// ReconnectSession reconecta (login + XMPP + TURN) conservando el estado
// del slot (ocupación y APK asignado). La usa el listener cuando la sesión
// XMPP muere (kick, corte de red, cierre del servidor).
func (p *PhonePool) ReconnectSession(s *PhoneSlot) error {
	slog.Info("reconectando sesión XMPP", "phone", s.Phone)
	jwt, err := loginWithPhoneOnly(s.Phone)
	if err != nil {
		return err
	}
	exp := jwtExp(jwt)
	xmpp, err := ConnectXmpp(s.Phone, jwt)
	if err != nil {
		return err
	}
	turn, err := xmpp.FetchTurnCreds()
	if err != nil {
		slog.Warn("reconexión: TURN fetch falló (conservando anteriores)", "phone", s.Phone, "err", err)
		turn = nil
	}
	s.mu.Lock()
	old := s.xmpp
	s.jwt = jwt
	s.jwtExp = exp
	s.xmpp = xmpp
	if turn != nil {
		s.turnCreds = turn
	}
	s.mu.Unlock()
	if old != nil {
		old.Close()
	}
	slog.Info("sesión XMPP restaurada", "phone", s.Phone, "jwt_exp", exp)
	return nil
}

// MaybeRevalidate revalida el JWT si está cerca de expirar (<10 min).
func (p *PhonePool) MaybeRevalidate(s *PhoneSlot) bool {
	s.mu.RLock()
	exp := s.jwtExp
	s.mu.RUnlock()
	if exp == 0 || time.Now().Add(10*time.Minute).Unix() < exp {
		return false
	}
	slog.Info("revalidando JWT", "phone", s.Phone)
	jwt, err := loginWithPhoneOnly(s.Phone)
	if err != nil {
		slog.Error("revalidación login falló", "phone", s.Phone, "err", err)
		return false
	}
	newExp := jwtExp(jwt)
	xmpp, err := ConnectXmpp(s.Phone, jwt)
	if err != nil {
		slog.Error("revalidación XMPP falló", "phone", s.Phone, "err", err)
		return false
	}
	turn, err := xmpp.FetchTurnCreds()
	if err != nil {
		slog.Warn("revalidación TURN fetch falló (usando anteriores)", "phone", s.Phone)
		turn = nil
	}
	s.mu.Lock()
	oldXmpp := s.xmpp
	s.jwt = jwt
	s.jwtExp = newExp
	s.xmpp = xmpp
	if turn != nil {
		s.turnCreds = turn
	}
	s.mu.Unlock()
	if oldXmpp != nil {
		oldXmpp.Close()
	}
	slog.Info("XMPP reconectado", "phone", s.Phone, "jwt_exp", newExp)
	return true
}
