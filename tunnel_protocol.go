// tunnel_protocol.go — Protocolo de framing binario sobre el DataChannel.
// Frame: [type:1][stream_id:4 BE][len:2 BE][payload:N]
package main

import "encoding/binary"

const (
	FrameOpen    byte = 0
	FrameData    byte = 1
	FrameClose   byte = 2
	FrameErr     byte = 3
	FrameOpened  byte = 4
	FramePing    byte = 5
	FramePong    byte = 6
	CtrlStreamID      = 0
	MaxPayload        = 16384
	headerLen         = 7

	ErrGeneral         byte = 1
	ErrHostUnreachable byte = 2
	ErrConnRefused     byte = 3
	ErrConnTimeout     byte = 4
)

func encodeFrame(ftype byte, streamID uint32, payload []byte) []byte {
	n := len(payload)
	if n > 0xFFFF {
		n = 0xFFFF
	}
	out := make([]byte, headerLen+n)
	out[0] = ftype
	binary.BigEndian.PutUint32(out[1:5], streamID)
	binary.BigEndian.PutUint16(out[5:7], uint16(n))
	copy(out[headerLen:], payload[:n])
	return out
}

type decodedFrame struct {
	Type     byte
	StreamID uint32
	Payload  []byte
	Consumed int
}

func decodeFrame(buf []byte) *decodedFrame {
	if len(buf) < headerLen {
		return nil
	}
	plen := int(binary.BigEndian.Uint16(buf[5:7]))
	if len(buf) < headerLen+plen {
		return nil
	}
	return &decodedFrame{
		Type:     buf[0],
		StreamID: binary.BigEndian.Uint32(buf[1:5]),
		Payload:  buf[headerLen : headerLen+plen],
		Consumed: headerLen + plen,
	}
}

func encodeOpen(sid uint32, hostPort string) []byte {
	return encodeFrame(FrameOpen, sid, []byte(hostPort))
}
func encodeData(sid uint32, data []byte) []byte {
	return encodeFrame(FrameData, sid, data)
}
func encodeClose(sid uint32) []byte {
	return encodeFrame(FrameClose, sid, nil)
}
func encodeOpened(sid uint32) []byte {
	return encodeFrame(FrameOpened, sid, nil)
}
func encodeErr(sid uint32, code byte, msg string) []byte {
	p := make([]byte, 1+len(msg))
	p[0] = code
	copy(p[1:], msg)
	return encodeFrame(FrameErr, sid, p)
}
func encodePing() []byte { return encodeFrame(FramePing, CtrlStreamID, nil) }
func encodePong() []byte { return encodeFrame(FramePong, CtrlStreamID, nil) }

// chunked divide un payload grande en frames DATA <= MaxPayload.
func chunked(sid uint32, data []byte) [][]byte {
	if len(data) == 0 {
		return [][]byte{encodeData(sid, nil)}
	}
	var frames [][]byte
	for start := 0; start < len(data); start += MaxPayload {
		end := start + MaxPayload
		if end > len(data) {
			end = len(data)
		}
		frames = append(frames, encodeData(sid, data[start:end]))
	}
	return frames
}
