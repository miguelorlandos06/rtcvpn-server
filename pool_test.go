// pool_test.go — tests unitarios del pool de teléfonos (sin red).
package main

import "testing"

func newTestPool(phones ...string) *PhonePool {
	p := NewPhonePool(phones)
	for _, s := range p.slots {
		s.mu.Lock()
		s.state = StateFree
		s.mu.Unlock()
	}
	return p
}

func TestAcquireAsignaLibre(t *testing.T) {
	p := newTestPool("5350256349", "5350155246")
	ph, _ := p.Acquire("5350000001")
	if ph == "" {
		t.Fatal("esperaba teléfono libre")
	}
	if st := p.find(ph); st == nil || st.state != StateConnecting {
		t.Fatalf("slot %q no quedó Connecting", ph)
	}
}

func TestAcquireReusaSlotMismoAPK(t *testing.T) {
	p := newTestPool("5350256349", "5350155246")
	ph1, _ := p.Acquire("5350000001")
	p.MarkBusy(ph1, "5350000001")
	// El mismo APK reintenta: debe reutilizar SU slot aunque siga busy.
	ph2, _ := p.Acquire("5350000001")
	if ph1 != ph2 {
		t.Fatalf("reintento asignó slot distinto: %q vs %q", ph1, ph2)
	}
	// Otro APK no debe robar el slot.
	ph3, _ := p.Acquire("5350000002")
	if ph3 == ph1 {
		t.Fatal("APK distinto recibió el slot ocupado")
	}
}

func TestAcquireTodosOcupadosEncola(t *testing.T) {
	p := newTestPool("5350256349")
	p.Acquire("5350000001")
	ph, _ := p.Acquire("5350000002")
	if ph != "" {
		t.Fatal("esperaba string vacío con pool lleno")
	}
	if pos := p.WaitlistPosition("5350000002"); pos != 1 {
		t.Fatalf("posición waitlist=%d, want 1", pos)
	}
	// Dedup: no encolar dos veces el mismo APK.
	p.Acquire("5350000002")
	if pos := p.WaitlistPosition("5350000002"); pos != 1 {
		t.Fatalf("dedup waitlist falló: posición=%d", pos)
	}
}

func TestReleaseDevuelveSiguienteEnEspera(t *testing.T) {
	p := newTestPool("5350256349")
	ph, _ := p.Acquire("5350000001")
	p.Acquire("5350000002") // encolado
	next := p.Release(ph)
	if next != "5350000002" {
		t.Fatalf("next=%q, want 5350000002", next)
	}
	if p.WaitlistPosition("5350000002") != 0 {
		t.Fatal("waitlist no se vació")
	}
}

func TestSlotEnErrorSeSalta(t *testing.T) {
	p := newTestPool("5350256349", "5350155246")
	p.slots[0].mu.Lock()
	p.slots[0].state = StateError // sesión muerta (zombi ya detectada)
	p.slots[0].mu.Unlock()
	ph, _ := p.Acquire("5350000001")
	if ph != "5350155246" {
		t.Fatalf("asignó %q, want el slot sano 5350155246", ph)
	}
}
