// todus_test.go — tests unitarios del protocolo XMPP (sin red).
package main

import (
	"strings"
	"testing"
)

// ─── extractMessageBody ──────────────────────────────────────────────────────

func TestExtractMessageBodySimple(t *testing.T) {
	st := "<m f='a@im.todus.cu' o='b@im.todus.cu' t='c' i='x1'><k xmlns='x8'/><b>CALL:5353185427</b></m>"
	got := extractMessageBody(st)
	if got != "CALL:5353185427" {
		t.Fatalf("got %q", got)
	}
}

func TestExtractMessageBodyNested(t *testing.T) {
	st := "<m o='b@im.todus.cu' t='c' i='x2'><b>ACCEPT:{&quot;server_phone&quot;:&quot;<b>inner</b>&quot;}</b></m>"
	got := extractMessageBody(st)
	if got != "ACCEPT:{\"server_phone\":\"<b>inner</b>\"}" {
		t.Fatalf("got %q", got)
	}
}

func TestExtractMessageBodyEmpty(t *testing.T) {
	if got := extractMessageBody("<m o='b' t='c' i='x3'><k xmlns='x8'/></m>"); got != "" {
		t.Fatalf("esperaba vacío, got %q", got)
	}
	if got := extractMessageBody("<m o='b' t='c' i='x4'><b/></m>"); got != "" {
		t.Fatalf("esperaba vacío con <b/>, got %q", got)
	}
}

func TestExtractMessageBodyEscapes(t *testing.T) {
	st := "<m o='b' t='c' i='x5'><b>OFFER:&lt;sdp&gt;a&amp;b</b></m>"
	got := extractMessageBody(st)
	if got != "OFFER:<sdp>a&b" {
		t.Fatalf("got %q", got)
	}
}

// ─── plainTextBody (fallback sin <b>) ────────────────────────────────────────

func TestPlainTextBodyCall(t *testing.T) {
	st := "<m f='apk@im.todus.cu' o='srv@im.todus.cu' t='c' i='x6'>CALL:5353185427</m>"
	if got := plainTextBody(st); got != "CALL:5353185427" {
		t.Fatalf("got %q", got)
	}
}

func TestPlainTextBodyIgnoresReceipts(t *testing.T) {
	st := "<m o='b' t='c' i='x7'><dd xmlns='y'/></m>"
	if got := plainTextBody(st); got != "" {
		t.Fatalf("esperaba vacío, got %q", got)
	}
}

func TestPlainTextBodyWithChildrenTags(t *testing.T) {
	st := "<m o='b' t='c' i='x8'><k xmlns='x8'/>CALL:5353185427</m>"
	if got := plainTextBody(st); got != "CALL:5353185427" {
		t.Fatalf("got %q", got)
	}
}

// ─── dispatchStanza: fallback conectado ──────────────────────────────────────

func TestDispatchStanzaFallsBackToPlainText(t *testing.T) {
	x := &XmppClient{msgCh: make(chan string, 4), iqCh: make(chan string, 4)}
	x.dispatchStanza("<m o='b' t='c' i='x9'>CALL:5353185427</m>")
	select {
	case got := <-x.msgCh:
		if got != "CALL:5353185427" {
			t.Fatalf("got %q", got)
		}
	default:
		t.Fatal("mensaje de texto plano no llegó a msgCh (fallback roto)")
	}
}

// ─── extractStanza ───────────────────────────────────────────────────────────

func TestExtractStanzaComplete(t *testing.T) {
	s := "<m o='b' t='c' i='a'><b>hola</b></m>"
	st, rest := extractStanza(s)
	if st != s || rest != "" {
		t.Fatalf("st=%q rest=%q", st, rest)
	}
}

func TestExtractStanzaSelfClosing(t *testing.T) {
	s := "  <p/>  <m o='b' t='c' i='a'><b>x</b></m>"
	st, rest := extractStanza(s)
	if st != "<p/>" || strings.TrimSpace(rest) != "<m o='b' t='c' i='a'><b>x</b></m>" {
		t.Fatalf("st=%q rest=%q", st, rest)
	}
}

func TestExtractStanzaIncomplete(t *testing.T) {
	s := "<m o='b' t='c' i='a'><b>sin cerrar"
	st, rest := extractStanza(s)
	if st != "" || rest != s {
		t.Fatalf("st=%q rest=%q", st, rest)
	}
}

func TestExtractStanzaTwoInRow(t *testing.T) {
	s := "<m o='b' t='c' i='a'><b>1</b></m><m o='b' t='c' i='b'><b>2</b></m>"
	st1, rest := extractStanza(s)
	if st1 != "<m o='b' t='c' i='a'><b>1</b></m>" {
		t.Fatalf("st1=%q", st1)
	}
	st2, rest2 := extractStanza(rest)
	if st2 != "<m o='b' t='c' i='b'><b>2</b></m>" || rest2 != "" {
		t.Fatalf("st2=%q rest2=%q", st2, rest2)
	}
}

// ─── buildMessageStanza ──────────────────────────────────────────────────────

func TestBuildMessageStanzaFormatoOficial(t *testing.T) {
	st := buildMessageStanza("5353185427@im.todus.cu", "CALL:5353185427")
	for _, want := range []string{
		"<m o='5353185427@im.todus.cu' t='c' i='",
		"' xmlns='jc'>",
		"<k xmlns='x8'/>",
		"<b>CALL:5353185427</b>",
		"</m>",
	} {
		if !strings.Contains(st, want) {
			t.Fatalf("stanza %q no contiene %q", st, want)
		}
	}
	// Round-trip: lo que construimos debe parsearse de vuelta con el mismo cuerpo.
	if got := extractMessageBody(st); got != "CALL:5353185427" {
		t.Fatalf("round-trip body=%q", got)
	}
}

func TestBuildMessageStanzaEscapa(t *testing.T) {
	st := buildMessageStanza("x@im.todus.cu", "a<b>&'\"")
	if !strings.Contains(st, "&lt;b&gt;") || !strings.Contains(st, "&amp;") {
		t.Fatalf("escape faltante: %q", st)
	}
}

// ─── normalizePhone / buildJID ───────────────────────────────────────────────

func TestNormalizePhone(t *testing.T) {
	cases := map[string]string{
		"5351430352":             "5351430352",
		"+53 5143-0352":          "5351430352",
		"5351430352@im.todus.cu": "5351430352",
		"  5350256349  ":         "5350256349",
	}
	for in, want := range cases {
		if got := normalizePhone(in); got != want {
			t.Errorf("normalizePhone(%q)=%q, want %q", in, got, want)
		}
	}
}

func TestBuildJID(t *testing.T) {
	if got := buildJID("+5351430352"); got != "5351430352@im.todus.cu" {
		t.Fatalf("got %q", got)
	}
}

// ─── escape/unescape round-trip ──────────────────────────────────────────────

func TestEscapeUnescapeRoundTrip(t *testing.T) {
	orig := `CALL:<json>{"a":"b&c<d>"}</json>`
	if got := unescapeXML(escapeXML(orig)); got != orig {
		t.Fatalf("round-trip: %q", got)
	}
}
