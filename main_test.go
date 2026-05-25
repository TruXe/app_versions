package main

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// buildInfoResponse crafts a valid A2S_INFO ('I') reply.
func buildInfoResponse() []byte {
	b := bytes.Buffer{}
	b.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}) // header
	b.WriteByte(0x49)                       // 'I'
	b.WriteByte(0x11)                       // protocol
	b.WriteString("Test DayZ Server")
	b.WriteByte(0x00)
	b.WriteString("chernarusplus")
	b.WriteByte(0x00)
	b.WriteString("dayz") // folder
	b.WriteByte(0x00)
	b.WriteString("DayZ") // game
	b.WriteByte(0x00)
	binary.Write(&b, binary.LittleEndian, uint16(24492)) // app id (low 16 bits)
	b.WriteByte(42)                                      // players
	b.WriteByte(60)                                      // max players
	b.WriteByte(3)                                       // bots
	b.WriteByte('d')                                     // type
	b.WriteByte('l')                                     // environment
	b.WriteByte(1)                                       // visibility (private)
	b.WriteByte(1)                                       // vac
	b.WriteString("1.26.158020")
	b.WriteByte(0x00)
	return b.Bytes()
}

func TestParseA2SInfo(t *testing.T) {
	s, err := ParseA2SInfo(buildInfoResponse())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if s.Name != "Test DayZ Server" {
		t.Errorf("Name = %q, want %q", s.Name, "Test DayZ Server")
	}
	if s.Map != "chernarusplus" {
		t.Errorf("Map = %q, want %q", s.Map, "chernarusplus")
	}
	if s.Players != 42 {
		t.Errorf("Players = %d, want 42", s.Players)
	}
	if s.MaxPlayers != 60 {
		t.Errorf("MaxPlayers = %d, want 60", s.MaxPlayers)
	}
	if s.Bots != 3 {
		t.Errorf("Bots = %d, want 3", s.Bots)
	}
	if !s.Password {
		t.Errorf("Password = false, want true")
	}
	if s.Version != "1.26.158020" {
		t.Errorf("Version = %q, want %q", s.Version, "1.26.158020")
	}
}

func TestParseA2SInfoRejectsGarbage(t *testing.T) {
	if _, err := ParseA2SInfo([]byte{0x01, 0x02}); err == nil {
		t.Error("expected error for short packet")
	}
	if _, err := ParseA2SInfo([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x41, 0x00}); err == nil {
		t.Error("expected error for non-'I' header")
	}
}

func TestBuildA2SInfoPacket(t *testing.T) {
	p := BuildA2SInfoPacket(nil)
	want := append([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x54}, []byte("Source Engine Query")...)
	want = append(want, 0x00)
	if !bytes.Equal(p, want) {
		t.Errorf("packet = %v, want %v", p, want)
	}

	ch := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	p = BuildA2SInfoPacket(ch)
	if !bytes.Equal(p[len(p)-4:], ch) {
		t.Errorf("challenge not appended, got tail %v", p[len(p)-4:])
	}
}

// TestQueryServerWithChallenge spins up a fake Source server on loopback that
// first answers with a challenge and only returns info on the second request.
func TestQueryServerWithChallenge(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer pc.Close()

	challenge := []byte{0x11, 0x22, 0x33, 0x44}

	go func() {
		buf := make([]byte, 2048)

		// First request -> challenge reply.
		n, addr, err := pc.ReadFrom(buf)
		if err != nil {
			return
		}
		reply := append([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x41}, challenge...)
		pc.WriteTo(reply, addr)

		// Second request must carry the challenge -> info reply.
		n, addr, err = pc.ReadFrom(buf)
		if err != nil {
			return
		}
		if !bytes.Contains(buf[:n], challenge) {
			t.Errorf("second request missing challenge")
			return
		}
		pc.WriteTo(buildInfoResponse(), addr)
	}()

	// Give the listener a moment to be ready.
	time.Sleep(50 * time.Millisecond)

	s, err := QueryServer(pc.LocalAddr().String())
	if err != nil {
		t.Fatalf("QueryServer: %v", err)
	}
	if s.Name != "Test DayZ Server" {
		t.Errorf("Name = %q, want %q", s.Name, "Test DayZ Server")
	}
	if s.Players != 42 {
		t.Errorf("Players = %d, want 42", s.Players)
	}
	if s.Port == 0 {
		t.Errorf("Port not parsed")
	}
}

func TestUnique(t *testing.T) {
	in := []string{"a:1", "b:2", "a:1", "c:3", "b:2"}
	out := unique(in)
	if len(out) != 3 {
		t.Fatalf("len = %d, want 3 (%v)", len(out), out)
	}
}
