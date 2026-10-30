package kafka

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rakunlabs/kafka-console/internal/config"
)

// Ported from github.com/worldline-go/wkafka (oauth, mTLS and GCP tests).

func must(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}

func authenticate(t *testing.T, s config.OAuth) (string, error) {
	t.Helper()

	m, err := oauthMechanism(s)
	must(t, err)

	_, initial, err := m.Authenticate(t.Context(), "broker:9092")

	return string(initial), err
}

const gcpTestSA = "sa@proj.iam.gserviceaccount.com"

func genCert(t *testing.T) (string, string, *x509.Certificate) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must(t, err)

	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	must(t, err)

	kb, err := x509.MarshalECPrivateKey(key)
	must(t, err)

	cert, err := x509.ParseCertificate(der)
	must(t, err)

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})), cert
}

func caPEM(s *httptest.Server) string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw}))
}

// ─── Client credentials ──────────────────────────────────

func TestOAuthClientCredentials(t *testing.T) {
	for _, expires := range []int{3600, 1} {
		t.Run(fmt.Sprint(expires), func(t *testing.T) {
			var calls atomic.Int32

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				user, pass, ok := r.BasicAuth()
				if !ok || user != "client" || pass != "secret" || r.FormValue("grant_type") != "client_credentials" || r.FormValue("scope") != "kafka read" {
					t.Error("unexpected token request")
				}

				n := calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"access_token":"token-%d","token_type":"Bearer","expires_in":%d}`, n, expires) //nolint:errcheck // test response writer
			}))
			defer server.Close()

			m, err := oauthMechanism(config.OAuth{TokenURL: server.URL, ClientID: "client", ClientSecret: "secret", Scopes: []string{"kafka", "read"}})
			must(t, err)

			// Canceling the first session must not poison later refreshes.
			ctx, cancel := context.WithCancel(t.Context())
			_, initial, err := m.Authenticate(ctx, "broker:9092")
			cancel()
			must(t, err)

			if !strings.Contains(string(initial), "auth=Bearer token-1") {
				t.Fatalf("initial = %q", initial)
			}

			var wg sync.WaitGroup
			for range 8 {
				wg.Go(func() {
					if _, _, err := m.Authenticate(t.Context(), "broker:9092"); err != nil {
						t.Error(err)
					}
				})
			}
			wg.Wait()

			// oauth2 refreshes within ten seconds of expiry.
			want := int32(1)
			if expires == 1 {
				want = 9
			}

			if got := calls.Load(); got != want {
				t.Fatalf("token requests = %d, want %d", got, want)
			}
		})
	}
}

func TestOAuthStatic(t *testing.T) {
	initial, err := authenticate(t, config.OAuth{Token: "static", Zid: "user", Extensions: map[string]string{"cluster": "one"}})
	must(t, err)

	if want := "n,a=user,\x01auth=Bearer static\x01cluster=one\x01\x01"; initial != want {
		t.Fatalf("initial = %q, want %q", initial, want)
	}
}

func TestOAuthClientCredentialsProxy(t *testing.T) {
	var proxied atomic.Int32

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !r.URL.IsAbs() || r.URL.Host != "token.invalid" {
			t.Errorf("unexpected proxy request %s", r.URL)
		}

		proxied.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"access_token":"via-proxy","token_type":"Bearer","expires_in":3600}`) //nolint:errcheck // test response writer
	}))
	defer proxy.Close()

	initial, err := authenticate(t, config.OAuth{TokenURL: "http://token.invalid/token", ClientID: "client", ClientSecret: "secret", Proxy: proxy.URL})
	must(t, err)

	if !strings.Contains(initial, "auth=Bearer via-proxy") || proxied.Load() != 1 {
		t.Fatalf("initial = %q, proxied = %d", initial, proxied.Load())
	}
}

func TestOAuthValidation(t *testing.T) {
	for i, c := range []config.OAuth{
		{},
		{Token: "token", TokenURL: "https://example.com"},
		{ClientID: "client"},
		{TokenURL: "/relative", ClientID: "client", ClientSecret: "secret"},
		{TokenURL: "https://example.com", ClientID: "client", ClientSecret: "secret", Proxy: "nope"},
		{Token: "token", Timeout: -time.Second},
	} {
		if _, err := oauthMechanism(c); err == nil {
			t.Errorf("case %d: want error", i)
		}
	}
}

func TestOAuthTokenTimeout(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)

	_, err := authenticate(t, config.OAuth{TokenURL: server.URL, ClientID: "client", ClientSecret: "secret", Timeout: 20 * time.Millisecond})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
}

func TestOAuthErrorHidesBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"invalid_client","detail":"secret=hunter2"}`, http.StatusUnauthorized)
	}))
	defer server.Close()

	_, err := authenticate(t, config.OAuth{TokenURL: server.URL, ClientID: "client", ClientSecret: "secret"})
	if err == nil || strings.Contains(err.Error(), "hunter2") || !strings.Contains(err.Error(), "401") {
		t.Fatalf("err = %v", err)
	}
}

// ─── Client credentials over mTLS ────────────────────────

type mtlsTokenServer struct {
	srv   *httptest.Server
	calls atomic.Int32
}

func newMTLSTokenServer(t *testing.T, client *x509.Certificate, wantSecret string) *mtlsTokenServer {
	t.Helper()

	s := &mtlsTokenServer{}
	pool := x509.NewCertPool()
	pool.AddCert(client)

	s.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, basic := r.BasicAuth()

		ok := r.FormValue("grant_type") == "client_credentials"
		if wantSecret == "" {
			ok = ok && !basic && r.FormValue("client_id") == "worker" && r.FormValue("client_secret") == ""
		} else {
			ok = ok && basic && user == "worker" && pass == wantSecret
		}

		if !ok {
			http.Error(w, `{"error":"invalid_client"}`, http.StatusUnauthorized)

			return
		}

		n := s.calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"access_token":"mtls-%d","token_type":"Bearer","expires_in":3600}`, n) //nolint:errcheck // test response writer
	}))
	s.srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	s.srv.StartTLS()
	t.Cleanup(s.srv.Close)

	return s
}

func TestOAuthClientCredentialsMTLS(t *testing.T) {
	for _, secret := range []string{"", "secret"} {
		t.Run("secret="+secret, func(t *testing.T) {
			certPEM, keyPEM, cert := genCert(t)
			s := newMTLSTokenServer(t, cert, secret)

			m, err := oauthMechanism(config.OAuth{
				TokenURL: s.srv.URL, ClientID: "worker", ClientSecret: secret,
				TLS: config.OAuthTLS{Cert: certPEM, Key: keyPEM, CA: caPEM(s.srv)},
			})
			must(t, err)

			for range 3 {
				_, initial, err := m.Authenticate(t.Context(), "broker:9092")
				must(t, err)

				if !strings.Contains(string(initial), "auth=Bearer mtls-1") {
					t.Fatalf("initial = %q", initial)
				}
			}

			if s.calls.Load() != 1 {
				t.Fatalf("token requests = %d", s.calls.Load())
			}
		})
	}
}

func TestOAuthClientCredentialsMTLSRequiresCert(t *testing.T) {
	_, _, cert := genCert(t)
	s := newMTLSTokenServer(t, cert, "secret")

	// CA only: the handshake fails without a client certificate.
	_, err := authenticate(t, config.OAuth{
		TokenURL: s.srv.URL, ClientID: "worker", ClientSecret: "secret",
		TLS: config.OAuthTLS{CA: caPEM(s.srv)},
	})
	if err == nil || s.calls.Load() != 0 {
		t.Fatalf("err = %v, calls = %d", err, s.calls.Load())
	}
}

func TestOAuthTLSInsecureSkipVerify(t *testing.T) {
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"access_token":"test-token","token_type":"Bearer","expires_in":3600}`) //nolint:errcheck // test response writer
	}))
	t.Cleanup(s.Close)

	for _, insecure := range []bool{false, true} {
		initial, err := authenticate(t, config.OAuth{
			TokenURL: s.URL, ClientID: "worker", ClientSecret: "secret",
			TLS: config.OAuthTLS{InsecureSkipVerify: insecure},
		})

		if insecure && (err != nil || !strings.Contains(initial, "auth=Bearer test-token")) {
			t.Fatalf("insecure: err = %v, initial = %q", err, initial)
		}

		if !insecure && err == nil {
			t.Fatal("verified: want error for an untrusted server")
		}
	}
}

func TestMTLSClientFileRotation(t *testing.T) {
	certPEM, keyPEM, _ := genCert(t)
	dir := t.TempDir()
	write := func(name, content string) string {
		p := filepath.Join(dir, name)
		must(t, os.WriteFile(p, []byte(content), 0o600))

		return p
	}

	cfg := config.OAuthTLS{CertFile: write("cert.pem", certPEM), KeyFile: write("key.pem", keyPEM)}

	m, err := newMTLSClient(cfg, "")
	must(t, err)

	c1, err := m.Client()
	must(t, err)

	c2, err := m.Client()
	must(t, err)

	if c1 != c2 {
		t.Fatal("unchanged files must reuse the client")
	}

	newCert, newKey, _ := genCert(t)
	must(t, os.WriteFile(cfg.CertFile, []byte(newCert), 0o600))
	must(t, os.WriteFile(cfg.KeyFile, []byte(newKey), 0o600))

	c3, err := m.Client()
	must(t, err)

	if c1 == c3 || m.CertPEM() != newCert {
		t.Fatal("rotated files must rebuild the client")
	}
}

func TestOAuthClientCredentialsMTLSValidation(t *testing.T) {
	certPEM, keyPEM, _ := genCert(t)

	for name, c := range map[string]config.OAuth{
		"no secret no cert": {TokenURL: "https://x", ClientID: "c", TLS: config.OAuthTLS{CA: "x"}},
		"cert without key":  {TokenURL: "https://x", ClientID: "c", TLS: config.OAuthTLS{Cert: certPEM}},
		"cert both":         {TokenURL: "https://x", ClientID: "c", TLS: config.OAuthTLS{Cert: certPEM, CertFile: "/x", Key: keyPEM}},
		"missing file":      {TokenURL: "https://x", ClientID: "c", TLS: config.OAuthTLS{CertFile: "/does/not/exist", Key: keyPEM}},
		"bad pair":          {TokenURL: "https://x", ClientID: "c", TLS: config.OAuthTLS{Cert: certPEM, Key: "nope"}},
		"tls with token":    {Token: "t", TLS: config.OAuthTLS{Cert: certPEM, Key: keyPEM}},
		"no client id":      {TokenURL: "https://x", TLS: config.OAuthTLS{Cert: certPEM, Key: keyPEM}},
	} {
		if _, err := oauthMechanism(c); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

// ─── GCP workload identity federation ────────────────────

type gcpFake struct {
	srv      *httptest.Server
	sts, imp atomic.Int32
	fail     atomic.Bool
	expires  time.Time
}

func newGCPFake(t *testing.T, client *x509.Certificate) *gcpFake {
	t.Helper()

	f := &gcpFake{expires: time.Now().Add(time.Hour)}
	pool := x509.NewCertPool()
	pool.AddCert(client)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/token", func(w http.ResponseWriter, r *http.Request) {
		f.sts.Add(1)

		if f.fail.Load() {
			http.Error(w, "boom", http.StatusInternalServerError)

			return
		}

		var certs []string
		if r.FormValue("subject_token_type") != gcpTokenTypeMTLS ||
			r.FormValue("grant_type") != gcpGrantTypeTokenExchange ||
			r.FormValue("audience") != "//iam.googleapis.com/test" ||
			json.Unmarshal([]byte(r.FormValue("subject_token")), &certs) != nil || len(certs) != 1 {
			http.Error(w, "bad request", http.StatusBadRequest)

			return
		}

		der, _ := base64.StdEncoding.DecodeString(certs[0])
		if string(r.TLS.PeerCertificates[0].Raw) != string(der) {
			http.Error(w, "cert mismatch", http.StatusUnauthorized)

			return
		}

		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "sts-token", "expires_in": 3600})
	})
	mux.HandleFunc("/v1/projects/-/serviceAccounts/"+gcpTestSA+":generateAccessToken", func(w http.ResponseWriter, r *http.Request) {
		f.imp.Add(1)

		var body struct {
			Lifetime string `json:"lifetime"`
		}

		_ = json.NewDecoder(r.Body).Decode(&body)

		if r.Header.Get("Authorization") != "Bearer sts-token" || body.Lifetime != "3600s" {
			http.Error(w, "bad impersonation", http.StatusBadRequest)

			return
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"accessToken": "ya29.final",
			"expireTime":  f.expires.UTC().Format(time.RFC3339),
		})
	})

	f.srv = httptest.NewUnstartedServer(mux)
	f.srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	f.srv.StartTLS()
	t.Cleanup(f.srv.Close)

	return f
}

func gcpTestConfig(t *testing.T) (config.GCP, *gcpFake) {
	t.Helper()

	certPEM, keyPEM, cert := genCert(t)
	f := newGCPFake(t, cert)

	creds, err := json.Marshal(map[string]any{
		"type":                              "external_account",
		"audience":                          "//iam.googleapis.com/test",
		"subject_token_type":                gcpTokenTypeMTLS,
		"token_url":                         f.srv.URL + "/v1/token",
		"service_account_impersonation_url": f.srv.URL + "/v1/projects/-/serviceAccounts/" + gcpTestSA + ":generateAccessToken",
	})
	must(t, err)

	return config.GCP{
		Enabled:     true,
		Credentials: string(creds),
		Cert:        certPEM,
		Key:         keyPEM,
		CA:          caPEM(f.srv),
	}, f
}

func TestOAuthGCPMechanismCached(t *testing.T) {
	cfg, f := gcpTestConfig(t)

	m, err := oauthMechanism(config.OAuth{GCP: cfg})
	must(t, err)

	if m.Name() != "OAUTHBEARER" {
		t.Fatalf("name = %s", m.Name())
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			_, initial, err := m.Authenticate(t.Context(), "broker:9092")
			if err != nil {
				t.Error(err)

				return
			}

			if !strings.Contains(string(initial), "auth=Bearer ya29.final") {
				t.Errorf("unexpected initial %q", initial)
			}
		})
	}
	wg.Wait()

	if f.sts.Load() != 1 || f.imp.Load() != 1 {
		t.Fatalf("sts = %d, impersonation = %d; want one each", f.sts.Load(), f.imp.Load())
	}
}

func TestOAuthGCPInsecureSkipVerify(t *testing.T) {
	for _, insecure := range []bool{false, true} {
		cfg, _ := gcpTestConfig(t)
		cfg.CA = ""
		cfg.InsecureSkipVerify = insecure

		s, err := newGCPTokenSource(cfg, time.Second)
		must(t, err)

		token, err := s.Token(t.Context())

		if insecure && (err != nil || token != "ya29.final") {
			t.Fatalf("insecure: token = %q, err = %v", token, err)
		}

		if !insecure && err == nil {
			t.Fatal("verified: want error for an untrusted server")
		}
	}
}

func TestOAuthGCPManagedKafkaFormat(t *testing.T) {
	cfg, _ := gcpTestConfig(t)
	cfg.Format = gcpFormatManagedKafka

	s, err := newGCPTokenSource(cfg, time.Second)
	must(t, err)

	token, err := s.Token(t.Context())
	must(t, err)

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d parts", len(parts))
	}

	payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
	sig, _ := base64.RawURLEncoding.DecodeString(parts[2])

	if !strings.Contains(string(payload), `"sub":"`+gcpTestSA+`"`) || string(sig) != "ya29.final" {
		t.Fatalf("payload = %s, sig = %s", payload, sig)
	}
}

func TestOAuthGCPStaleOnFailure(t *testing.T) {
	cfg, f := gcpTestConfig(t)

	s, err := newGCPTokenSource(cfg, time.Second)
	must(t, err)

	_, err = s.Token(t.Context())
	must(t, err)

	// Refresh fails but the cached token is still valid: keep using it.
	f.fail.Store(true)
	s.now = func() time.Time { return f.expires.Add(-time.Minute) }

	token, err := s.Token(t.Context())
	must(t, err)

	if token != "ya29.final" {
		t.Fatalf("token = %q", token)
	}

	// Expired and refresh fails: report the error.
	s.now = func() time.Time { return f.expires.Add(time.Minute) }
	if _, err := s.Token(t.Context()); err == nil || !strings.Contains(err.Error(), "sts exchange") {
		t.Fatalf("err = %v", err)
	}
}

func TestOAuthGCPFilesAndRotation(t *testing.T) {
	cfg, f := gcpTestConfig(t)
	dir := t.TempDir()
	write := func(name, content string) string {
		p := filepath.Join(dir, name)
		must(t, os.WriteFile(p, []byte(content), 0o600))

		return p
	}

	cfg.CredentialsFile = write("creds.json", cfg.Credentials)
	cfg.CertFile = write("cert.pem", cfg.Cert)
	cfg.KeyFile = write("key.pem", cfg.Key)
	cfg.CAFile = write("ca.pem", cfg.CA)
	cfg.Credentials, cfg.Cert, cfg.Key, cfg.CA = "", "", "", ""

	s, err := newGCPTokenSource(cfg, time.Second)
	must(t, err)

	_, err = s.Token(t.Context())
	must(t, err)

	// Rotate to an untrusted cert: the next refresh must pick it up and fail the handshake.
	certPEM, keyPEM, _ := genCert(t)
	must(t, os.WriteFile(cfg.CertFile, []byte(certPEM), 0o600))
	must(t, os.WriteFile(cfg.KeyFile, []byte(keyPEM), 0o600))

	s.cached = nil
	before := f.sts.Load()

	if _, err := s.Token(t.Context()); err == nil || f.sts.Load() != before {
		t.Fatalf("err = %v, sts before/after = %d/%d", err, before, f.sts.Load())
	}
}

func TestOAuthGCPValidation(t *testing.T) {
	for name, c := range map[string]config.OAuth{
		"gcp and token":   {Token: "x", GCP: config.GCP{Enabled: true}},
		"no audience":     {GCP: config.GCP{Enabled: true, Cert: "x", Key: "y"}},
		"cert both":       {GCP: config.GCP{Enabled: true, Audience: "a", Cert: "x", CertFile: "/x", Key: "y"}},
		"missing file":    {GCP: config.GCP{Enabled: true, Audience: "a", CertFile: "/does/not/exist", Key: "y"}},
		"bad type":        {GCP: config.GCP{Enabled: true, Credentials: `{"type":"service_account"}`}},
		"bad subject":     {GCP: config.GCP{Enabled: true, Credentials: `{"audience":"a","subject_token_type":"urn:ietf:params:oauth:token-type:jwt"}`}},
		"bad format":      {GCP: config.GCP{Enabled: true, Audience: "a", Format: "nope"}},
		"kafka no sa url": {GCP: config.GCP{Enabled: true, Audience: "a", Format: gcpFormatManagedKafka}},
		"no cert":         {GCP: config.GCP{Enabled: true, Audience: "a"}},
		"bad proxy":       {GCP: config.GCP{Enabled: true, Audience: "a", Cert: "x", Key: "y", Proxy: "ftp://p:1"}},
	} {
		if _, err := oauthMechanism(c); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

func TestOAuthGCPProxy(t *testing.T) {
	cfg, f := gcpTestConfig(t)

	var connects atomic.Int32

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "connect only", http.StatusMethodNotAllowed)

			return
		}

		connects.Add(1)

		upstream, err := net.Dial("tcp", r.Host)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)

			return
		}

		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			upstream.Close() //nolint:errcheck // best-effort cleanup on hijack failure

			return
		}

		_, _ = conn.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n"))

		go func() {
			_, _ = io.Copy(upstream, conn)
			upstream.Close() //nolint:errcheck // best-effort cleanup
		}()

		_, _ = io.Copy(conn, upstream)
		conn.Close() //nolint:errcheck // best-effort cleanup
	}))
	t.Cleanup(proxy.Close)

	// The GCP proxy falls back to oauth.proxy.
	m, err := oauthMechanism(config.OAuth{Proxy: proxy.URL, Timeout: 5 * time.Second, GCP: cfg})
	must(t, err)

	_, initial, err := m.Authenticate(t.Context(), "broker:9092")
	must(t, err)

	if !strings.Contains(string(initial), "auth=Bearer ya29.final") || connects.Load() == 0 || f.sts.Load() != 1 {
		t.Fatalf("initial = %q, connects = %d, sts = %d", initial, connects.Load(), f.sts.Load())
	}
}

type gcpMetadataFake struct {
	srv          *httptest.Server
	token, email atomic.Int32
	scopes       atomic.Value
}

func newGCPMetadataFake(t *testing.T) *gcpMetadataFake {
	t.Helper()

	f := &gcpMetadataFake{}

	const base = "/computeMetadata/v1/instance/service-accounts/default/"

	mux := http.NewServeMux()
	mux.HandleFunc(base+"token", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			http.Error(w, "missing flavor", http.StatusForbidden)
			return
		}

		f.token.Add(1)
		f.scopes.Store(r.URL.Query().Get("scopes"))
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "ya29.metadata", "expires_in": 3600, "token_type": "Bearer"})
	})
	mux.HandleFunc(base+"email", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			http.Error(w, "missing flavor", http.StatusForbidden)
			return
		}

		f.email.Add(1)
		_, _ = io.WriteString(w, "gke-sa@proj.iam.gserviceaccount.com")
	})

	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)

	return f
}

func TestOAuthGCPMetadata(t *testing.T) {
	f := newGCPMetadataFake(t)

	m, err := oauthMechanism(config.OAuth{GCP: config.GCP{
		Enabled: true, Source: gcpSourceMetadata, MetadataURL: f.srv.URL + "/", Format: gcpFormatManagedKafka,
	}})
	must(t, err)

	for range 3 {
		_, initial, err := m.Authenticate(t.Context(), "broker:9092")
		must(t, err)

		token := strings.TrimSuffix(strings.SplitN(string(initial), "auth=Bearer ", 2)[1], "\x01\x01")

		parts := strings.Split(token, ".")
		if len(parts) != 3 {
			t.Fatalf("token has %d parts", len(parts))
		}

		payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
		sig, _ := base64.RawURLEncoding.DecodeString(parts[2])

		if !strings.Contains(string(payload), `"sub":"gke-sa@proj.iam.gserviceaccount.com"`) || string(sig) != "ya29.metadata" {
			t.Fatalf("payload = %s, sig = %s", payload, sig)
		}
	}

	if f.token.Load() != 1 || f.email.Load() != 1 {
		t.Fatalf("token calls = %d, email calls = %d", f.token.Load(), f.email.Load())
	}

	if got := f.scopes.Load(); got != gcpScopeCloudPlatform {
		t.Fatalf("scopes = %v", got)
	}
}

func TestOAuthGCPMetadataEnvHostAndNoProxy(t *testing.T) {
	f := newGCPMetadataFake(t)
	t.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(f.srv.URL, "http://"))
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")

	s, err := newGCPTokenSource(config.GCP{Enabled: true, Source: gcpSourceMetadata}, time.Second)
	must(t, err)

	token, err := s.Token(t.Context())
	must(t, err)

	if token != "ya29.metadata" || f.email.Load() != 0 {
		t.Fatalf("token = %q, email calls = %d", token, f.email.Load())
	}
}

func TestOAuthGCPMetadataValidation(t *testing.T) {
	for name, c := range map[string]config.GCP{
		"bad source":      {Enabled: true, Source: "nope"},
		"with cert":       {Enabled: true, Source: gcpSourceMetadata, Cert: "x", Key: "y"},
		"with creds":      {Enabled: true, Source: gcpSourceMetadata, Credentials: "{}"},
		"with audience":   {Enabled: true, Source: gcpSourceMetadata, Audience: "a"},
		"bad url":         {Enabled: true, Source: gcpSourceMetadata, MetadataURL: "metadata"},
		"metadata on wif": {Enabled: true, Audience: "a", Cert: "x", Key: "y", MetadataURL: "http://m"},
	} {
		if _, err := oauthMechanism(config.OAuth{GCP: c}); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}
