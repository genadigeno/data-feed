package security

import (
	"crypto/x509"
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

func TestSelfSignedFingerprintSurvivesReload(t *testing.T) {
	dir := t.TempDir()

	_, first, err := SelfSignedServerTLS(dir, now)
	if err != nil {
		t.Fatal(err)
	}
	_, second, err := SelfSignedServerTLS(dir, now)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("fingerprint changed across reload: %s vs %s", first, second)
	}
	if !strings.HasPrefix(first, fingerprintPrefix) {
		t.Fatalf("fingerprint %q lacks prefix", first)
	}
}

func TestPinAcceptsOnlyTheMatchingCertificate(t *testing.T) {
	cfg, fp, err := SelfSignedServerTLS(t.TempDir(), now)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(cfg.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	chain := []*x509.Certificate{cert}

	if err := checkPin(chain, fp); err != nil {
		t.Errorf("matching pin rejected: %v", err)
	}
	if err := checkPin(chain, fingerprintPrefix+strings.Repeat("0", 64)); err == nil {
		t.Error("wrong pin accepted")
	}
	if err := checkPin(nil, fp); err == nil {
		t.Error("empty chain accepted")
	}
}

func TestClientTLSRequiresUsableCA(t *testing.T) {
	if _, err := ClientTLS(ClientTLSOptions{CAFile: t.TempDir() + "/missing.pem"}); err == nil {
		t.Fatal("missing CA file accepted")
	}
}
