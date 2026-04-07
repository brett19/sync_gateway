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

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcorex"
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

// GoCBCoreTLSRootCAProvider returns a TLS root CA provider function for gocbcore DCP clients.
// TODO: Remove when DCP is fully migrated to gocbcorex.
func GoCBCoreTLSRootCAProvider(ctx context.Context, tlsSkipVerify *bool, caCertPath string) (func() *x509.CertPool, error) {
	certPool, err := getRootCAs(ctx, caCertPath)
	if err != nil {
		return nil, err
	}
	return func() *x509.CertPool {
		return certPool
	}, nil
}

// CouchbaseClusterWaitUntilReadyOptions provides options for waiting until a gocbcore agent is ready.
type CouchbaseClusterWaitUntilReadyOptions struct {
	Timeout       time.Duration
	RetryStrategy gocbcore.RetryStrategy
}

// NewClusterAgent creates a gocbcore.Agent for management/test operations against a Couchbase cluster.
// TODO: Migrate to gocbcorex-based management when test infrastructure is fully migrated.
func NewClusterAgent(ctx context.Context, clusterSpec CouchbaseClusterSpec, opts CouchbaseClusterWaitUntilReadyOptions) (*gocbcore.Agent, error) {
	connStr := clusterSpec.Server

	agentConfig := gocbcore.AgentConfig{
		DefaultRetryStrategy: gocbcore.NewBestEffortRetryStrategy(nil),
	}
	connStrError := agentConfig.FromConnStr(connStr)
	if connStrError != nil {
		return nil, fmt.Errorf("unable to create cluster agent - error building conn str: %v", connStrError)
	}

	username := clusterSpec.Username
	password := clusterSpec.Password
	auth := &gocbcore.PasswordAuthProvider{
		Username: username,
		Password: password,
	}

	tlsRootCAProvider, err := GoCBCoreTLSRootCAProvider(ctx, &clusterSpec.TLSSkipVerify, clusterSpec.CACertpath)
	if err != nil {
		return nil, err
	}

	agentConfig.SecurityConfig.Auth = auth
	agentConfig.SecurityConfig.TLSRootCAProvider = tlsRootCAProvider
	agentConfig.UserAgent = "SyncGatewayTest"

	agent, err := gocbcore.CreateAgent(&agentConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create cluster agent: %w", err)
	}

	// Wait for agent to be ready
	agentReadyErr := make(chan error, 1)
	_, err = agent.WaitUntilReady(
		time.Now().Add(opts.Timeout),
		gocbcore.WaitUntilReadyOptions{},
		func(_ *gocbcore.WaitUntilReadyResult, err error) {
			agentReadyErr <- err
		})
	if err != nil {
		_ = agent.Close()
		return nil, fmt.Errorf("WaitUntilReady for cluster agent returned error: %w", err)
	}
	err = <-agentReadyErr
	if err != nil {
		_ = agent.Close()
		return nil, fmt.Errorf("WaitUntilReady error channel for cluster agent returned error: %w", err)
	}

	return agent, nil
}
