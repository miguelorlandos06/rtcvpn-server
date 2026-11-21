# RTCVPN Server (Go)

Servidor de túnel WebRTC sobre toDus XMPP + TURN forzado.

## Compilar
```bash
go build -o rtcvpn-server .
```

## Ejecutar
```bash
./rtcvpn-server --phones 5350256349,5350155246,5351417372,5351430352 --port 8080
```

## HTTP API
- POST /connect {"apk_phone":"53XXX"} → asigna teléfono libre + TURN creds
- POST /disconnect {"server_phone":"..."} → libera teléfono
- GET /status → estado del pool
- GET /health → ok

## GitHub Actions
El workflow se auto-relanza cada 5 horas usando GH_TOKEN como secret.
