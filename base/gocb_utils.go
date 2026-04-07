// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbaselabs/gocbconnstr/v2"
)

// GocbcorexAuthenticator creates a gocbcorex.Authenticator from credentials or certificate paths.
func GocbcorexAuthenticator(username, password, certPath, keyPath string) (gocbcorex.Authenticator, error) {
	if certPath != "" && keyPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, err
		}
		return &CertAuthenticator{
			ClientCertificate: &cert,
			Username:          username,
			Password:          password,
		}, nil
	}

	return &gocbcorex.PasswordAuthenticator{
		Username: username,
		Password: password,
	}, nil
}

// CertAuthenticator implements gocbcorex.Authenticator for client certificate authentication.
type CertAuthenticator struct {
	ClientCertificate *tls.Certificate
	Username          string
	Password          string
}

func (a *CertAuthenticator) GetClientCertificate(service gocbcorex.ServiceType, hostPort string) (*tls.Certificate, error) {
	return a.ClientCertificate, nil
}

func (a *CertAuthenticator) GetCredentials(service gocbcorex.ServiceType, hostPort string) (string, string, error) {
	return a.Username, a.Password, nil
}

// GocbcorexTLSConfig builds a *tls.Config from a CA cert path and TLS skip verify flag.
func GocbcorexTLSConfig(ctx context.Context, tlsSkipVerify *bool, caCertPath string) (*tls.Config, error) {
	if tlsSkipVerify != nil && *tlsSkipVerify {
		return &tls.Config{
			InsecureSkipVerify: true,
		}, nil
	}

	certPool, err := getRootCAs(ctx, caCertPath)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		RootCAs: certPool,
	}, nil
}

// getRootCAs gets generates a cert pool from the certs at caCertPath. If caCertPath is empty, the systems cert pool is used.
// If an error happens when retrieving the system cert pool, it is logged (not returned) and an empty (not nil) cert pool is returned.
func getRootCAs(ctx context.Context, caCertPath string) (*x509.CertPool, error) {
	if caCertPath != "" {
		rootCAs := x509.NewCertPool()

		caCert, err := os.ReadFile(caCertPath)
		if err != nil {
			return nil, err
		}

		ok := rootCAs.AppendCertsFromPEM(caCert)
		if !ok {
			return nil, ErrInvalidCACert
		}

		return rootCAs, nil
	}

	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		rootCAs = x509.NewCertPool()
		WarnfCtx(ctx, "Could not retrieve root CAs: %v", err)
	}
	return rootCAs, nil
}

// ErrInvalidCACert is returned when the CA cert cannot be parsed.
var ErrInvalidCACert = &sgError{"invalid CA cert"}

// MgmtRequest makes a request to the http couchbase management api. This function will read the entire contents of
// the response and return the output bytes, the status code, and an error.
func MgmtRequest(client *http.Client, mgmtEp, method, uri, contentType, username, password string, body io.Reader) ([]byte, int, error) {
	req, err := http.NewRequest(method, mgmtEp+uri, body)
	if err != nil {
		return nil, 0, err
	}

	if contentType != "" {
		req.Header.Add("Content-Type", contentType)
	}

	if username != "" {
		req.SetBasicAuth(username, password)
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = response.Body.Close() }()

	respBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, 0, err
	}
	return respBytes, response.StatusCode, nil
}

// CouchbaseClusterWaitUntilReadyOptions provides options for waiting until a cluster agent is ready.
type CouchbaseClusterWaitUntilReadyOptions struct {
	Timeout time.Duration
}

// NewClusterAgent creates a gocbcorex.Agent for management/test operations against a Couchbase cluster.
func NewClusterAgent(ctx context.Context, clusterSpec CouchbaseClusterSpec, opts CouchbaseClusterWaitUntilReadyOptions) (*gocbcorex.Agent, error) {
	auth, err := GocbcorexAuthenticator(clusterSpec.Username, clusterSpec.Password, clusterSpec.X509Certpath, clusterSpec.X509Keypath)
	if err != nil {
		return nil, fmt.Errorf("unable to create authenticator: %w", err)
	}

	connSpec, err := gocbconnstr.Parse(clusterSpec.Server)
	if err != nil {
		return nil, fmt.Errorf("unable to parse connection string: %w", err)
	}

	seedConfig, err := buildSeedConfig(connSpec)
	if err != nil {
		return nil, fmt.Errorf("unable to build seed config: %w", err)
	}

	var tlsConfig *tls.Config
	if ServerIsTLS(clusterSpec.Server) {
		tlsConfig, err = GocbcorexTLSConfig(ctx, &clusterSpec.TLSSkipVerify, clusterSpec.CACertpath)
		if err != nil {
			return nil, fmt.Errorf("unable to create TLS config: %w", err)
		}
	}

	agent, err := gocbcorex.CreateAgent(ctx, gocbcorex.AgentOptions{
		Authenticator: auth,
		TLSConfig:     tlsConfig,
		SeedConfig:    seedConfig,
		BucketName:    "", // cluster-level agent, no bucket
		Logger:        GocbcorexLogger(),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create cluster agent: %w", err)
	}

	return agent, nil
}
