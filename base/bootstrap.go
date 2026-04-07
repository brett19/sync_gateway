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
	"errors"
	"fmt"
	"reflect"
	"sync"

	"dario.cat/mergo"
	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/memdx"
	"github.com/couchbaselabs/gocbconnstr/v2"
)

// BootstrapConnection is the interface that can be used to bootstrap Sync Gateway against a Couchbase Server cluster.
// Manages retrieval of set of buckets, and generic interaction with bootstrap metadata documents from those buckets.
type BootstrapConnection interface {
	// GetConfigBuckets returns a list of bucket names where a bootstrap metadata documents could reside.
	GetConfigBuckets(context.Context) ([]string, error)
	// GetMetadataDocument fetches a bootstrap metadata document for a given bucket and key, along with the CAS of the config document.
	GetMetadataDocument(ctx context.Context, bucket, key string, valuePtr any) (cas uint64, err error)
	// InsertMetadataDocument saves a new bootstrap metadata document for a given bucket and key.
	InsertMetadataDocument(ctx context.Context, bucket, key string, value any) (newCAS uint64, err error)
	// DeleteMetadataDocument deletes an existing bootstrap metadata document for a given bucket and key.
	DeleteMetadataDocument(ctx context.Context, bucket, key string, cas uint64) (err error)
	// UpdateMetadataDocument updates an existing bootstrap metadata document for a given bucket and key. updateCallback can return nil to remove the config.  Retries on CAS failure.
	UpdateMetadataDocument(ctx context.Context, bucket, key string, updateCallback func(rawBucketConfig []byte, rawBucketConfigCas uint64) (updatedConfig []byte, err error)) (newCAS uint64, err error)
	// WriteMetadataDocument writes a bootstrap metadata document for a given bucket and key.  Does not retry on CAS failure.
	WriteMetadataDocument(ctx context.Context, bucket, key string, cas uint64, valuePtr any) (casOut uint64, err error)
	// TouchMetadataDocument sets the specified property in a bootstrap metadata document for a given bucket and key.  Used to
	// trigger CAS update on the document, to block any racing updates. Does not retry on CAS failure.
	TouchMetadataDocument(ctx context.Context, bucket, key string, property string, value string, cas uint64) (casOut uint64, err error)
	// KeyExists checks whether the specified key exists in the bucket's default collection
	KeyExists(ctx context.Context, bucket, key string) (exists bool, err error)
	// GetDocument retrieves the document with the specified key from the bucket's default collection.
	// Returns exists=false if key is not found, returns error for any other error.
	GetDocument(ctx context.Context, bucket, docID string, rv any) (exists bool, err error)
	// Close releases any long-lived connections
	Close()
}

// CouchbaseClusterSpec define how to make a connection to Couchbase Server
type CouchbaseClusterSpec struct {
	Server        string // connection string to connect to the Couchbase cluster
	Username      string // RBAC username to authenticate with the cluster
	Password      string // RBAC password to authenticate with the cluster
	X509Certpath  string // X.509 cert path to authenticate with the cluster
	X509Keypath   string // X.509 key path to authenticate with the cluster
	CACertpath    string // CA cert path to use for TLS connections
	TLSSkipVerify bool   // If true, do not validate TLS certificate
}

// CouchbaseCluster is a gocbcorex implementation of BootstrapConnection
type CouchbaseCluster struct {
	server               string
	auth                 gocbcorex.Authenticator
	tlsConfig            *tls.Config
	forcePerBucketAuth   bool                            // Forces perBucketAuth authenticators to be used to connect to the bucket
	perBucketAuth        map[string]gocbcorex.Authenticator
	bucketConnectionMode BucketConnectionMode            // Whether to cache cluster connections
	cachedAgents         cachedAgentConnections           // Per-bucket cached agent connections
	cachedConnectionLock sync.Mutex                       // mutex for access to cached connections
	configPersistence    ConfigPersistence                // ConfigPersistence mode
}

type BucketConnectionMode int

const (
	// CachedClusterConnections mode reuses a cached cluster connection.  Should be used for recurring operations
	CachedClusterConnections BucketConnectionMode = iota
	// PerUseClusterConnections mode establishes a new cluster connection per cluster operation.  Should be used for adhoc operations
	PerUseClusterConnections
)

type cachedAgent struct {
	agent     *gocbcorex.Agent // underlying agent
	closeFn   func()           // teardown function which will close the agent connection
	refcount  int              // count of how many functions are using this cachedAgent
	shouldClose bool           // mark this cachedAgent as needing to be closed with ref
}

// cachedAgentConnections is a lockable map of cached agents containing refcounts
type cachedAgentConnections struct {
	agents map[string]*cachedAgent
	lock   sync.Mutex
}

// removeOutdatedBuckets marks any active agents for closure and removes the cached connections.
func (c *cachedAgentConnections) removeOutdatedBuckets(activeBuckets Set) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for bucketName, agent := range c.agents {
		_, exists := activeBuckets[bucketName]
		if exists {
			continue
		}
		agent.shouldClose = true
		c._teardown(bucketName)
	}
}

// closeAll removes all cached agents
func (c *cachedAgentConnections) closeAll() {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, agent := range c.agents {
		agent.shouldClose = true
		agent.closeFn()
	}
}

// teardown closes the cached agent connection while locked
func (c *cachedAgentConnections) teardown(bucketName string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.agents[bucketName].refcount--
	c._teardown(bucketName)
}

// _teardown expects the lock to be acquired before calling this function and the reference count to be up to date.
func (c *cachedAgentConnections) _teardown(bucketName string) {
	if !c.agents[bucketName].shouldClose || c.agents[bucketName].refcount > 0 {
		return
	}
	c.agents[bucketName].closeFn()
	delete(c.agents, bucketName)
}

// _get returns a cachedAgent for a given bucketName, or nil if it doesn't exist
func (c *cachedAgentConnections) _get(bucketName string) *cachedAgent {
	agent, ok := c.agents[bucketName]
	if !ok {
		return nil
	}
	c.agents[bucketName].refcount++
	return agent
}

// _set adds a cachedAgent for a given bucketName
func (c *cachedAgentConnections) _set(bucketName string, agent *cachedAgent) {
	c.agents[bucketName] = agent
}

var _ BootstrapConnection = &CouchbaseCluster{}

// NewCouchbaseCluster creates and opens a Couchbase Server cluster connection.
func NewCouchbaseCluster(ctx context.Context, clusterSpec CouchbaseClusterSpec,
	forcePerBucketAuth bool, perBucketCreds PerBucketCredentialsConfig,
	useXattrConfig bool, bucketMode BucketConnectionMode) (*CouchbaseCluster, error) {

	tlsConfig, err := GocbcorexTLSConfig(ctx, Ptr(clusterSpec.TLSSkipVerify), clusterSpec.CACertpath)
	if err != nil {
		return nil, err
	}

	auth, err := GocbcorexAuthenticator(
		clusterSpec.Username, clusterSpec.Password,
		clusterSpec.X509Certpath, clusterSpec.X509Keypath,
	)
	if err != nil {
		return nil, err
	}

	// Populate individual bucket credentials
	perBucketAuthMap := make(map[string]gocbcorex.Authenticator, len(perBucketCreds))
	for bucket, credentials := range perBucketCreds {
		bucketAuth, err := GocbcorexAuthenticator(
			credentials.Username, credentials.Password,
			credentials.X509CertPath, credentials.X509KeyPath,
		)
		if err != nil {
			return nil, err
		}
		perBucketAuthMap[bucket] = bucketAuth
	}

	cbCluster := &CouchbaseCluster{
		server:               clusterSpec.Server,
		auth:                 auth,
		tlsConfig:            tlsConfig,
		forcePerBucketAuth:   forcePerBucketAuth,
		perBucketAuth:        perBucketAuthMap,
		bucketConnectionMode: bucketMode,
	}

	if bucketMode == CachedClusterConnections {
		cbCluster.cachedAgents = cachedAgentConnections{agents: make(map[string]*cachedAgent)}
	}

	cbCluster.configPersistence = &DocumentBootstrapPersistence{}
	if useXattrConfig {
		cbCluster.configPersistence = &XattrBootstrapPersistence{}
	}

	return cbCluster, nil
}

// createAgent creates a gocbcorex.Agent for the specified bucket.
func (cc *CouchbaseCluster) createAgent(ctx context.Context, bucketName string) (*gocbcorex.Agent, func(), error) {

	auth := cc.auth
	if bucketAuth, set := cc.perBucketAuth[bucketName]; set {
		auth = bucketAuth
	} else if cc.forcePerBucketAuth {
		return nil, nil, fmt.Errorf("unable to get bucket %q since credentials are not defined in bucket_credentials", MD(bucketName).Redact())
	}

	connSpec, err := gocbconnstr.Parse(cc.server)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to parse connection string for agent creation: %w", err)
	}
	seedConfig, err := buildSeedConfig(connSpec)
	if err != nil {
		return nil, nil, err
	}

	agent, err := gocbcorex.CreateAgent(ctx, gocbcorex.AgentOptions{
		Logger:        GocbcorexLogger(),
		Authenticator: auth,
		TLSConfig:     cc.tlsConfig,
		SeedConfig:    seedConfig,
		BucketName:    bucketName,
	})
	if err != nil {
		return nil, nil, err
	}

	teardownFn := func() {
		if closeErr := agent.Close(); closeErr != nil {
			WarnfCtx(ctx, "Failed to close agent for bucket %s: %v", MD(bucketName), closeErr)
		}
	}

	return agent, teardownFn, nil
}

// defaultCollection returns a *Collection wrapper for the default collection of the given agent.
func (cc *CouchbaseCluster) defaultCollection(agent *gocbcorex.Agent, bucketName string) *Collection {
	return &Collection{
		Bucket: &GocbV2Bucket{
			agent: agent,
			Spec: BucketSpec{
				BucketName: bucketName,
			},
		},
		scopeName:      DefaultScope,
		collectionName: DefaultCollection,
	}
}

func (cc *CouchbaseCluster) GetConfigBuckets(ctx context.Context) ([]string, error) {
	if cc == nil {
		return nil, errors.New("nil CouchbaseCluster")
	}

	// TODO: Implement bucket listing via gocbcorex management API
	// This requires creating an agent without a bucket name and using the management HTTP API.
	// For now, this is a stub that needs to be completed.
	return nil, fmt.Errorf("GetConfigBuckets not yet implemented with gocbcorex")
}

func (cc *CouchbaseCluster) GetMetadataDocument(ctx context.Context, location, docID string, valuePtr any) (cas uint64, err error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	agent, teardown, err := cc.getAgent(ctx, location)
	if err != nil {
		return 0, err
	}
	defer teardown()

	coll := cc.defaultCollection(agent, location)
	cas, err = cc.configPersistence.loadConfig(ctx, coll, docID, valuePtr)
	SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleKvOps.Add(1)
	return cas, err
}

func (cc *CouchbaseCluster) InsertMetadataDocument(ctx context.Context, location, key string, value any) (newCAS uint64, err error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	agent, teardown, err := cc.getAgent(ctx, location)
	if err != nil {
		return 0, err
	}
	defer teardown()

	coll := cc.defaultCollection(agent, location)
	return cc.configPersistence.insertConfig(coll, key, value)
}

// WriteMetadataDocument writes a metadata document, and fails on CAS mismatch
func (cc *CouchbaseCluster) WriteMetadataDocument(ctx context.Context, location, docID string, cas uint64, value any) (newCAS uint64, err error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}
	if cas == 0 {
		return 0, RedactErrorf("CAS for %q in bucket %q must be non-zero to call WriteMetadataDocument, to add a new document use InsertMetadataDocument", MD(docID), MD(location))
	}

	agent, teardown, err := cc.getAgent(ctx, location)
	if err != nil {
		return 0, err
	}
	defer teardown()

	rawDocument, err := JSONMarshal(value)
	if err != nil {
		return 0, err
	}

	coll := cc.defaultCollection(agent, location)
	return cc.configPersistence.replaceRawConfig(coll, docID, rawDocument, cas)
}

func (cc *CouchbaseCluster) TouchMetadataDocument(ctx context.Context, location, docID string, property, value string, cas uint64) (newCAS uint64, err error) {

	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	agent, teardown, err := cc.getAgent(ctx, location)
	if err != nil {
		return 0, err
	}
	defer teardown()

	coll := cc.defaultCollection(agent, location)
	return cc.configPersistence.touchConfigRollback(coll, docID, property, value, cas)
}

func (cc *CouchbaseCluster) DeleteMetadataDocument(ctx context.Context, location, key string, cas uint64) (err error) {
	if cc == nil {
		return errors.New("nil CouchbaseCluster")
	}

	agent, teardown, err := cc.getAgent(ctx, location)
	if err != nil {
		return err
	}
	defer teardown()

	coll := cc.defaultCollection(agent, location)
	_, removeErr := cc.configPersistence.removeRawConfig(coll, key, cas)
	return removeErr
}

// UpdateMetadataDocument retries on CAS mismatch
func (cc *CouchbaseCluster) UpdateMetadataDocument(ctx context.Context, location, docID string, updateCallback func(bucketConfig []byte, rawBucketConfigCas uint64) (newConfig []byte, err error)) (newCAS uint64, err error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	agent, teardown, err := cc.getAgent(ctx, location)
	if err != nil {
		return 0, err
	}
	defer teardown()

	coll := cc.defaultCollection(agent, location)

	for {
		bucketValue, cas, err := cc.configPersistence.loadRawConfig(ctx, coll, docID)
		if err != nil {
			return 0, err
		}
		newConfig, err := updateCallback(bucketValue, cas)
		if err != nil {
			return 0, err
		}

		// handle delete when updateCallback returns nil
		if newConfig == nil {
			removeCasOut, err := cc.configPersistence.removeRawConfig(coll, docID, cas)
			if err != nil {
				// retry on cas failure
				if errors.Is(err, memdx.ErrCasMismatch) {
					continue
				}
				return 0, err
			}
			return removeCasOut, nil
		}

		replaceCfgCasOut, err := cc.configPersistence.replaceRawConfig(coll, docID, newConfig, cas)
		if err != nil {
			if errors.Is(err, memdx.ErrCasMismatch) {
				// retry on cas failure
				continue
			}
			return 0, err
		}

		return replaceCfgCasOut, nil
	}

}

// KeyExists checks whether a key exists in the default collection for the specified bucket
func (cc *CouchbaseCluster) KeyExists(ctx context.Context, location, docID string) (exists bool, err error) {
	if cc == nil {
		return false, errors.New("nil CouchbaseCluster")
	}

	agent, teardown, err := cc.getAgent(ctx, location)
	if err != nil {
		return false, err
	}
	defer teardown()

	coll := cc.defaultCollection(agent, location)
	return cc.configPersistence.keyExists(coll, docID)
}

// GetDocument fetches a document from the default collection.  Does not use configPersistence - callers
// requiring configPersistence handling should use GetMetadataDocument.
func (cc *CouchbaseCluster) GetDocument(ctx context.Context, bucketName, docID string, rv any) (exists bool, err error) {
	if cc == nil {
		return false, errors.New("nil CouchbaseCluster")
	}

	agent, teardown, err := cc.getAgent(ctx, bucketName)
	if err != nil {
		return false, err
	}
	defer teardown()

	coll := cc.defaultCollection(agent, bucketName)
	_, getErr := coll.Get(docID, rv)
	if getErr != nil {
		if errors.Is(getErr, memdx.ErrDocNotFound) {
			return false, nil
		}
		return false, getErr
	}
	return true, nil
}

// Close calls teardown for any cached agents and removes from cachedAgents
func (cc *CouchbaseCluster) Close() {

	cc.cachedAgents.closeAll()
}

func (cc *CouchbaseCluster) getAgent(ctx context.Context, bucketName string) (agent *gocbcorex.Agent, teardownFn func(), err error) {

	if cc.bucketConnectionMode != CachedClusterConnections {
		return cc.createAgent(ctx, bucketName)
	}

	teardownFn = func() {
		cc.cachedAgents.teardown(bucketName)
	}
	cc.cachedAgents.lock.Lock()
	defer cc.cachedAgents.lock.Unlock()
	cached := cc.cachedAgents._get(bucketName)
	if cached != nil {
		return cached.agent, teardownFn, nil
	}

	// cached agent not found, connect and add
	newAgent, closeFn, err := cc.createAgent(ctx, bucketName)
	if err != nil {
		return nil, nil, err
	}
	cc.cachedAgents._set(bucketName, &cachedAgent{
		agent:    newAgent,
		closeFn:  closeFn,
		refcount: 1,
	})

	return newAgent, teardownFn, nil
}

// GetAgentForBucket returns a gocbcorex.Agent for the specified bucket.
func (cc *CouchbaseCluster) GetAgentForBucket(ctx context.Context, bucketName string) (agent *gocbcorex.Agent, teardownFn func(), err error) {
	return cc.createAgent(ctx, bucketName)
}

type PerBucketCredentialsConfig map[string]*CredentialsConfig

type CredentialsConfig struct {
	Username string `json:"username,omitempty"       help:"Username for authenticating to the bucket"`
	Password string `json:"password,omitempty"       help:"Password for authenticating to the bucket"`
	CredentialsConfigX509
}

type CredentialsConfigX509 struct {
	X509CertPath string `json:"x509_cert_path,omitempty" help:"Cert path (public key) for X.509 bucket auth"`
	X509KeyPath  string `json:"x509_key_path,omitempty"  help:"Key path (private key) for X.509 bucket auth"`
}

// ConfigMerge applies non-empty fields from b onto non-empty fields on a
func ConfigMerge(a, b any) error {
	return mergo.Merge(a, b, mergo.WithTransformers(&mergoNilTransformer{}), mergo.WithOverride)
}

// mergoNilTransformer is a mergo.Transformers implementation that treats non-nil zero values as non-empty when merging.
type mergoNilTransformer struct{}

var _ mergo.Transformers = &mergoNilTransformer{}

func (t *mergoNilTransformer) Transformer(typ reflect.Type) func(dst, src reflect.Value) error {
	if typ.Kind() == reflect.Ptr {
		if typ.Elem().Kind() == reflect.Struct {
			// skip nilTransformer for structs, to allow recursion
			return nil
		}
		return func(dst, src reflect.Value) error {
			if dst.CanSet() && !src.IsNil() {
				dst.Set(src)
			}
			return nil
		}
	}
	return nil
}
