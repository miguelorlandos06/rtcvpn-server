// main_test.go — tests del failover de respuestas CALL (regresión del bug
// "CALL enviado y sin respuesta" cuando la sesión del teléfono receptor
// estaba zombi). Sin red: el envío XMPP se inyecta con un stub.
package main

import (
	"strings"
	"sync"
	"testing"
)

type stubSendRecord struct {
	phone string
	jid   string
	body  string
}

// withStubSender reemplaza xmppSendToJID durante el test y devuelve los
// envíos efectuados + un control de qué teléfonos "fallan" (sesión muerta).
func withStubSender(t *testing.T, dead map[string]bool) (*[]stubSendRecord, *sync.Mutex) {
	t.Helper()
	var mu sync.Mutex
	var sent []stubSendRecord
	orig := xmppSendToJID
	xmppSendToJID = func(phone, jid, body string) error {
		mu.Lock()
		defer mu.Unlock()
		if dead[phone] {
			return errStubDead
		}
		sent = append(sent, stubSendRecord{phone, jid, body})
		return nil
	}
	t.Cleanup(func() { xmppSendToJID = orig })
	return &sent, &mu
}

var errStubDead = &stubError{}

type stubError struct{}

func (*stubError) Error() string { return "stub: sesión muerta" }

func setupPool(t *testing.T, phones ...string) {
	t.Helper()
	orig := pool
	pool = newTestPool(phones...)
	t.Cleanup(func() { pool = orig })
}

// Regresión principal: el CALL llega por un teléfono cuya sesión está
// muerta — la respuesta debe salir por OTRA sesión viva del pool.
func TestHandleCallFailoverConSesionReceptoraMuerta(t *testing.T) {
	setupPool(t, "5351430352", "5350256349", "5350155246")
	sent, mu := withStubSender(t, map[string]bool{"5351430352": true})

	err := handleCall("5353185427", "5351430352")
	if err != nil {
		t.Fatalf("handleCall falló: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	sentSlice := *sent
	if len(sentSlice) == 0 {
		t.Fatal("ningún envío efectuado — el ACCEPT se perdió (bug original)")
	}
	if sentSlice[0].phone == "5351430352" {
		t.Fatalf("envió por la sesión muerta: %+v", sentSlice[0])
	}
	if !strings.HasPrefix(sentSlice[0].body, "ACCEPT:") {
		t.Fatalf("body inesperado: %q", sentSlice[0].body)
	}
	if !strings.Contains(sentSlice[0].body, `"server_phone"`) {
		t.Fatalf("ACCEPT sin server_phone: %q", sentSlice[0].body)
	}
	if !strings.Contains(sentSlice[0].jid, "5353185427@im.todus.cu") {
		t.Fatalf("jid destino incorrecto: %q", sentSlice[0].jid)
	}
}

// Todos los teléfonos muertos → error explícito y liberación del slot.
func TestHandleCallTodasLasSesionesMuertas(t *testing.T) {
	setupPool(t, "5351430352", "5350256349")
	_, mu := withStubSender(t, map[string]bool{"5351430352": true, "5350256349": true})

	err := handleCall("5353185427", "5351430352")
	if err == nil {
		t.Fatal("esperaba error con todas las sesiones muertas")
	}
	mu.Lock()
	defer mu.Unlock()
	// El slot asignado debe liberarse para no fugar canales.
	if ph, _ := pool.Acquire("5353185427"); ph == "" {
		t.Fatal("el slot asignado no se liberó tras el fallo")
	}
}

// Pool lleno → BUSY por failover aunque el receptor esté muerto.
func TestHandleCallBusyConFailover(t *testing.T) {
	setupPool(t, "5351430352", "5350256349")
	ph, _ := pool.Acquire("5359999999") // ocupa el único slot libre
	_ = ph
	// El segundo teléfono queda ocupado por el mismo mecanismo:
	if _, _ = pool.Acquire("5358888888"); true {
		// ambos slots ocupados ahora
	}
	sent, mu := withStubSender(t, map[string]bool{"5351430352": true})

	err := handleCall("5353185427", "5351430352")
	if err != nil {
		t.Fatalf("handleCall falló: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(*sent) == 0 {
		t.Fatal("BUSY no enviado — también necesita failover")
	}
	if !strings.HasPrefix((*sent)[0].body, "BUSY:") {
		t.Fatalf("esperaba BUSY:, got %q", (*sent)[0].body)
	}
	if !strings.Contains((*sent)[0].body, `"position"`) {
		t.Fatalf("BUSY sin posición: %q", (*sent)[0].body)
	}
}

// Reintento del mismo APK: el slot se reutiliza y el ACCEPT vuelve a salir.
func TestHandleCallReintentoReutilizaSlot(t *testing.T) {
	setupPool(t, "5350256349", "5350155246")
	sent, mu := withStubSender(t, nil)

	if err := handleCall("5353185427", "5350256349"); err != nil {
		t.Fatalf("primer CALL: %v", err)
	}
	if err := handleCall("5353185427", "5350256349"); err != nil {
		t.Fatalf("segundo CALL: %v", err)
	}
	mu.Lock()
	sentSlice := *sent
	mu.Unlock()
	if len(sentSlice) != 2 {
		t.Fatalf("esperaba 2 ACCEPT, got %d", len(sentSlice))
	}
	var phones []string
	for _, s := range sentSlice {
		phones = append(phones, s.phone)
	}
	if phones[0] != phones[1] {
		t.Fatalf("el reintento no reutilizó el slot: %v", phones)
	}
}

// sendCallResponse ordena: receptor primero, resto del pool después.
func TestSendCallResponseOrden(t *testing.T) {
	setupPool(t, "5350256349", "5350155246", "5351417372")
	sent, mu := withStubSender(t, map[string]bool{"5350256349": true})

	if err := sendCallResponse("5350256349", "5353185427", "PING:x"); err != nil {
		t.Fatalf("err: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(*sent) != 1 || (*sent)[0].phone == "5350256349" {
		t.Fatalf("failover incorrecto: %+v", *sent)
	}
}

// turnUser con nil no debe pánico (escenario de tests sin TURN creds).
func TestTurnUserNil(t *testing.T) {
	if turnUser(nil) != "" {
		t.Fatal("turnUser(nil) != ''")
	}
}
