package kafka

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/rakunlabs/kafka-console/internal/config"
)

const defaultTokenTimeout = 10 * time.Second

// oauthMechanism builds OAUTHBEARER from exactly one of a static token, the
// client credentials flow (secret and/or client certificate), or Google
// Cloud workload identity federation.
func oauthMechanism(s config.OAuth) (sasl.Mechanism, error) {
	credentials := s.TokenURL != "" || s.ClientID != "" || s.ClientSecret != "" || len(s.Scopes) > 0 || s.TLS.IsSet()

	modes := 0

	for _, set := range []bool{s.Token != "", credentials, s.GCP.Enabled} {
		if set {
			modes++
		}
	}

	if modes != 1 {
		return nil, errors.New("oauth: configure exactly one of token, client credentials, or gcp")
	}

	if s.Timeout < 0 {
		return nil, errors.New("oauth: timeout must not be negative")
	}

	timeout := s.Timeout
	if timeout == 0 {
		timeout = defaultTokenTimeout
	}

	extensions := maps.Clone(s.Extensions)

	if s.GCP.Enabled {
		gcp := s.GCP
		if gcp.Proxy == "" {
			gcp.Proxy = s.Proxy
		}

		source, err := newGCPTokenSource(gcp, timeout)
		if err != nil {
			return nil, fmt.Errorf("oauth: %w", err)
		}

		return oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
			token, err := source.Token(ctx)
			if err != nil {
				return oauth.Auth{}, fmt.Errorf("oauth: %w", err)
			}

			return oauth.Auth{Token: token, Zid: s.Zid, Extensions: extensions}, nil
		}), nil
	}

	if s.Token != "" {
		return oauth.Auth{Token: s.Token, Zid: s.Zid, Extensions: extensions}.AsMechanism(), nil
	}

	var mtls *mtlsClient

	if s.TLS.IsSet() || s.Proxy != "" {
		var err error
		if mtls, err = newMTLSClient(s.TLS, s.Proxy); err != nil {
			return nil, fmt.Errorf("oauth: tls: %w", err)
		}
	}

	certAuth := mtls != nil && mtls.CertPEM() != ""
	if s.TokenURL == "" || s.ClientID == "" || (s.ClientSecret == "" && !certAuth) {
		return nil, errors.New("oauth: token_url, client_id and client_secret (or a tls client certificate) are required for client credentials")
	}

	if u, err := url.Parse(s.TokenURL); err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, errors.New("oauth: token_url must be an absolute HTTP or HTTPS URL")
	}

	cc := clientcredentials.Config{
		ClientID:     s.ClientID,
		ClientSecret: s.ClientSecret,
		TokenURL:     s.TokenURL,
		Scopes:       append([]string(nil), s.Scopes...),
	}

	if s.ClientSecret == "" {
		// RFC 8705: authenticate with the certificate, send client_id in the body.
		cc.AuthStyle = oauth2.AuthStyleInParams
	}

	var (
		mu     sync.Mutex
		cached *oauth2.Token
	)

	return oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
		mu.Lock()
		defer mu.Unlock()

		if err := ctx.Err(); err != nil {
			return oauth.Auth{}, err
		}

		if !cached.Valid() {
			fetchCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			if mtls != nil {
				client, err := mtls.Client()
				if err != nil {
					return oauth.Auth{}, fmt.Errorf("oauth: tls: %w", err)
				}

				fetchCtx = context.WithValue(fetchCtx, oauth2.HTTPClient, client)
			}

			token, err := cc.Token(fetchCtx)
			if err != nil {
				// Token endpoint bodies can echo secrets; report only the status.
				var response *oauth2.RetrieveError
				if errors.As(err, &response) && response.Response != nil {
					return oauth.Auth{}, fmt.Errorf("oauth: token endpoint returned HTTP %d", response.Response.StatusCode)
				}

				return oauth.Auth{}, fmt.Errorf("oauth: token request failed: %w", unwrapURLError(err))
			}

			cached = token
		}

		return oauth.Auth{Token: cached.AccessToken, Zid: s.Zid, Extensions: extensions}, nil
	}), nil
}

// unwrapURLError drops the request URL, which can carry query secrets, and
// keeps the transport reason (TLS, DNS, proxy).
func unwrapURLError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}

	return err
}
