package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestParseServerList(t *testing.T) {
	body := []byte(`{
	  "response": {
	    "servers": [
	      {"addr":"1.2.3.4:27016","gameport":27016,"name":"DayZ One","map":"chernarusplus","players":40,"max_players":60,"bots":0,"version":"1.26.158020"},
	      {"addr":"5.6.7.8:2302","gameport":2302,"name":"DayZ Two","map":"enoch","players":300,"max_players":127,"bots":2,"version":"1.26"},
	      {"addr":"bogus-no-port","name":"skip me"}
	    ]
	  }
	}`)

	servers, err := parseServerList(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(servers) != 2 {
		t.Fatalf("len = %d, want 2 (bad addr should be skipped)", len(servers))
	}

	if servers[0].IP != "1.2.3.4" || servers[0].Port != 27016 {
		t.Errorf("server0 addr = %s:%d, want 1.2.3.4:27016", servers[0].IP, servers[0].Port)
	}
	if servers[0].Name != "DayZ One" || servers[0].Map != "chernarusplus" {
		t.Errorf("server0 name/map = %q/%q", servers[0].Name, servers[0].Map)
	}
	if servers[0].Players != 40 || servers[0].MaxPlayers != 60 {
		t.Errorf("server0 players = %d/%d, want 40/60", servers[0].Players, servers[0].MaxPlayers)
	}

	// players=300 must clamp into uint8 without wrapping.
	if servers[1].Players != 255 {
		t.Errorf("server1 players = %d, want clamped 255", servers[1].Players)
	}
}

func TestFetchViaWebAPI(t *testing.T) {
	var gotQuery url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"response":{"servers":[
			{"addr":"9.9.9.9:27016","name":"Live","map":"chernarusplus","players":10,"max_players":60,"version":"1.26"}
		]}}`)
	}))
	defer srv.Close()

	old := WEB_API_URL
	WEB_API_URL = srv.URL
	defer func() { WEB_API_URL = old }()

	servers, err := FetchViaWebAPI("dummy-key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(servers) != 1 || servers[0].IP != "9.9.9.9" || servers[0].Port != 27016 {
		t.Fatalf("unexpected servers: %+v", servers)
	}
	if gotQuery.Get("key") != "dummy-key" {
		t.Errorf("key param = %q", gotQuery.Get("key"))
	}
	if gotQuery.Get("filter") != "\\appid\\221100" {
		t.Errorf("filter param = %q", gotQuery.Get("filter"))
	}
}

func TestFetchViaWebAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, "Access is denied")
	}))
	defer srv.Close()

	old := WEB_API_URL
	WEB_API_URL = srv.URL
	defer func() { WEB_API_URL = old }()

	if _, err := FetchViaWebAPI("bad-key"); err == nil {
		t.Fatal("expected error on non-200 response")
	}
}

func TestClampU8(t *testing.T) {
	cases := map[int]uint8{-5: 0, 0: 0, 100: 100, 255: 255, 300: 255}
	for in, want := range cases {
		if got := clampU8(in); got != want {
			t.Errorf("clampU8(%d) = %d, want %d", in, got, want)
		}
	}
}

func TestUnique(t *testing.T) {
	in := []string{"a:1", "b:2", "a:1", "c:3", "b:2"}
	out := unique(in)
	if len(out) != 3 {
		t.Fatalf("len = %d, want 3 (%v)", len(out), out)
	}
}
