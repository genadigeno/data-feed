package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const fingerprintPrefix = "sha256:"

// Fingerprint returns the pin of a DER certificate.
func Fingerprint(der []byte) string {
	sum := sha256.Sum256(der)
	return fingerprintPrefix + hex.EncodeToString(sum[:])
}

// ServerTLS loads certFile and keyFile.
func ServerTLS(certFile, keyFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load TLS key pair: %w", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}, nil
}

// SelfSignedServerTLS loads the certificate stored in dir, creating it on first
// use, and returns the config with the fingerprint that Workers pin. The stored
// key means the fingerprint survives Coordinator restarts.
func SelfSignedServerTLS(dir string, now time.Time) (*tls.Config, string, error) {
	certPath := filepath.Join(dir, "worker-listener.crt")
	keyPath := filepath.Join(dir, "worker-listener.key")

	if _, err := os.Stat(certPath); errors.Is(err, os.ErrNotExist) {
		if err := writeSelfSigned(dir, certPath, keyPath, now); err != nil {
			return nil, "", err
		}
	} else if err != nil {
		return nil, "", fmt.Errorf("check TLS certificate: %w", err)
	}

	cfg, err := ServerTLS(certPath, keyPath)
	if err != nil {
		return nil, "", err
	}
	return cfg, Fingerprint(cfg.Certificates[0].Certificate[0]), nil
}

func writeSelfSigned(dir, certPath, keyPath string, now time.Time) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate TLS key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("generate certificate serial: %w", err)
	}
	host, _ := os.Hostname()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "datafeed coordinator"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.AddDate(10, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		DNSNames:     []string{"localhost"},
	}
	if host != "" {
		tmpl.DNSNames = append(tmpl.DNSNames, host)
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return fmt.Errorf("create certificate: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return fmt.Errorf("encode TLS key: %w", err)
	}

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create TLS directory: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		return fmt.Errorf("write TLS key: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certPath, certPEM, 0o644); err != nil {
		return fmt.Errorf("write TLS certificate: %w", err)
	}
	return nil
}

// RequireClientCerts turns on mutual TLS using the CA certificates in caFile.
func RequireClientCerts(cfg *tls.Config, caFile string) error {
	pemBytes, err := os.ReadFile(caFile)
	if err != nil {
		return fmt.Errorf("read client CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return errors.New("client CA file holds no certificates")
	}
	cfg.ClientCAs = pool
	cfg.ClientAuth = tls.RequireAndVerifyClientCert
	return nil
}

// ClientTLSOptions selects how a Worker verifies the Coordinator.
type ClientTLSOptions struct {
	CAFile      string
	Fingerprint string
	SkipVerify  bool // development only; rejected elsewhere by config validation
}

// ClientTLS builds the Worker's TLS configuration.
func ClientTLS(o ClientTLSOptions) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS13}
	if o.SkipVerify {
		cfg.InsecureSkipVerify = true // Dev-only escape hatch, gated by config validation.
		return cfg, nil
	}
	if o.CAFile != "" {
		pemBytes, err := os.ReadFile(o.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read coordinator CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, errors.New("coordinator CA file holds no certificates")
		}
		cfg.RootCAs = pool
	}
	if o.Fingerprint != "" {
		// The pin replaces chain and hostname verification: it identifies the exact
		// certificate, which is what a self-signed Coordinator needs.
		cfg.InsecureSkipVerify = true
		want := strings.ToLower(o.Fingerprint)
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			return checkPin(cs.PeerCertificates, want)
		}
	}
	return cfg, nil
}

func checkPin(chain []*x509.Certificate, want string) error {
	if len(chain) == 0 {
		return errors.New("coordinator presented no certificate")
	}
	got := Fingerprint(chain[0].Raw)
	if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
		return fmt.Errorf("coordinator certificate fingerprint %s does not match the configured pin", got)
	}
	return nil
}
