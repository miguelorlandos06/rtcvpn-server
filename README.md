# RTCVPN Server (Go)

Servidor de túnel WebRTC sobre toDus XMPP + TURN forzado.

## Protocolo v2 — señalización 100% XMPP

La APK **no usa HTTP contra el servidor** (ni descubre ninguna IP). Flujo
completo desde el botón de encender de la app:

1. La APK conecta su propia sesión XMPP a `im.todus.cu:1756` (JWT toDus).
2. La APK envía `CALL:<apk_phone>` por XMPP al **número del servidor**.
3. El pool asigna un teléfono libre y responde
   `ACCEPT:{"server_phone":"...","server_jid":"...","turn":{...}}`.
   Si los 4 canales están ocupados responde `BUSY:{"position":N}` y luego
   notifica `CHANNEL_FREE:<phone>` cuando queda uno libre.
4. La APK crea la PeerConnection (TURN forzado, `iceTransportPolicy=RELAY`)
   y envía `OFFER:<sdp>` al JID asignado; el servidor responde
   `ANSWER:<sdp>`.
5. TURN + DataChannel abren el túnel: frames
   `[tipo:1][stream_id:4 BE][len:2 BE][payload]` (OPEN/OPENED/DATA/CLOSE/
   ERR/PING/PONG) — ver `tunnel_protocol.go`.

Como el TURN siempre está forzado, el servidor solo necesita conexiones
**salientes** (im.todus.cu + TURN de toDus): puede correr en cualquier
runner/VM sin IP estable ni puertos de entrada.

## Compilar
```bash
go build -o rtcvpn-server .
```

## Ejecutar
```bash
./rtcvpn-server --phones 5350256349,5350155246,5351417372,5351430352 --port 8080
```

## HTTP API (solo diagnóstico local; la APK no la usa)
- POST /connect {"apk_phone":"53XXX"} → asigna teléfono libre + TURN creds
- POST /disconnect {"server_phone":"..."} → libera teléfono
- GET /status → estado del pool
- GET /health → ok

## GitHub Actions
El workflow se auto-relanza cada 5 horas usando GH_TOKEN como secret. La APK
ya no lee la rama `server-ip` (quedó obsoleta con el protocolo v2).
