/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"crypto/tls"
	"errors"
	"expvar"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbmgmtx"
	"github.com/couchbase/gocbcorex/contrib/cbconfig"
	"github.com/couchbase/gocbcorex/memdx"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/gocbconnstr/v2"
	pkgerrors "github.com/pkg/errors"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// GetGoCBv2Bucket opens a connection to the Couchbase cluster and returns a *GocbV2Bucket for the specified BucketSpec.
func GetGoCBv2Bucket(ctx context.Context, spec BucketSpec) (*GocbV2Bucket, error) {

	connStr, err := spec.GetGoCBConnString()
	if err != nil {
		WarnfCtx(ctx, "Unable to parse server value: %s error: %v", SD(spec.Server), err)
		return nil, err
	}

	connSpec, err := gocbconnstr.Parse(connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to parse connection string: %w", err)
	}

	authenticator, err := spec.GocbcorexAuth()
	if err != nil {
		return nil, err
	}

	var tlsConfig *tls.Config
	if spec.IsTLS() {
		tlsConfig, err = GocbcorexTLSConfig(ctx, &spec.TLSSkipVerify, spec.CACertPath)
		if err != nil {
			return nil, err
		}
	}

	seedConfig, err := buildSeedConfig(connSpec)
	if err != nil {
		return nil, err
	}

	agentOpts := gocbcorex.AgentOptions{
		Logger:        GocbcorexLogger(),
		Authenticator: authenticator,
		TLSConfig:     tlsConfig,
		BucketName:    spec.BucketName,
		SeedConfig:    seedConfig,
	}

	agent, err := gocbcorex.CreateAgent(ctx, agentOpts)
	if err != nil {
		if errors.Is(err, memdx.ErrAuthError) {
			return nil, ErrAuthError
		}
		InfofCtx(ctx, KeyAuth, "Unable to connect to cluster: %v", err)
		return nil, err
	}

	// TODO: Fetch cluster compat version via management API instead of gocb.Cluster.Internal().GetNodesMetadata
	clusterCompatMajor, clusterCompatMinor := 7, 6

	gocbv2Bucket := &GocbV2Bucket{
		agent:                     agent,
		Spec:                      spec,
		clusterCompatMajorVersion: uint64(clusterCompatMajor),
		clusterCompatMinorVersion: uint64(clusterCompatMinor),
	}

	// Set limits for concurrent query and kv ops
	maxConcurrentQueryOps := MaxConcurrentQueryOps
	if spec.MaxConcurrentQueryOps != nil {
		maxConcurrentQueryOps = *spec.MaxConcurrentQueryOps
	}

	queryNodeCount, err := gocbv2Bucket.QueryEpsCount()
	if err != nil || queryNodeCount == 0 {
		queryNodeCount = 1
	}

	if maxConcurrentQueryOps > DefaultHttpMaxIdleConnsPerHost*queryNodeCount {
		maxConcurrentQueryOps = DefaultHttpMaxIdleConnsPerHost * queryNodeCount
		InfofCtx(ctx, KeyAll, "Setting max_concurrent_query_ops to %d based on query node count (%d)", maxConcurrentQueryOps, queryNodeCount)
	}

	gocbv2Bucket.queryOps = make(chan struct{}, maxConcurrentQueryOps)

	// TODO: kv_pool_size handling for gocbcorex - review concurrent single ops limit
	nodeCount := 1
	mgmtEps, mgmtEpsErr := gocbv2Bucket.MgmtEps()
	if mgmtEpsErr == nil && len(mgmtEps) > 0 {
		nodeCount = len(mgmtEps)
	}
	gocbv2Bucket.kvOps = make(chan struct{}, MaxConcurrentSingleOps*nodeCount)

	return gocbv2Bucket, nil
}

// buildSeedConfig converts a parsed connection spec into a gocbcorex SeedConfig.
func buildSeedConfig(connSpec gocbconnstr.ConnSpec) (gocbcorex.SeedConfig, error) {
	resolved, err := gocbconnstr.Resolve(connSpec)
	if err != nil {
		return gocbcorex.SeedConfig{}, fmt.Errorf("unable to resolve connection string: %w", err)
	}

	var httpAddrs []string
	for _, host := range resolved.HttpHosts {
		httpAddrs = append(httpAddrs, fmt.Sprintf("%s:%d", host.Host, host.Port))
	}

	var memdAddrs []string
	for _, host := range resolved.MemdHosts {
		memdAddrs = append(memdAddrs, fmt.Sprintf("%s:%d", host.Host, host.Port))
	}

	return gocbcorex.SeedConfig{
		HTTPAddrs: httpAddrs,
		MemdAddrs: memdAddrs,
	}, nil
}

type GocbV2Bucket struct {
	agent                                                *gocbcorex.Agent
	Spec                                                 BucketSpec    // Spec is a copy of the BucketSpec for DCP usage
	queryOps                                             chan struct{} // Manages max concurrent query ops
	kvOps                                                chan struct{} // Manages max concurrent kv ops
	clusterCompatMajorVersion, clusterCompatMinorVersion uint64        // E.g: 6 and 0 for 6.0.3
	supportsHLV                                          bool          // Flag to indicate with bucket supports mobile XDCR
}

var (
	_ sgbucket.BucketStore            = &GocbV2Bucket{}
	_ CouchbaseBucketStore            = &GocbV2Bucket{}
	_ sgbucket.DynamicDataStoreBucket = &GocbV2Bucket{}
)

// AsGocbV2Bucket returns a bucket as a GocbV2Bucket, or an error if it is not one.
func AsGocbV2Bucket(bucket Bucket) (*GocbV2Bucket, error) {
	baseBucket := GetBaseBucket(bucket)
	if gocbv2Bucket, ok := baseBucket.(*GocbV2Bucket); ok {
		return gocbv2Bucket, nil
	}
	return nil, fmt.Errorf("bucket is not a gocb bucket (type %T)", baseBucket)
}

func (b *GocbV2Bucket) GetName() string {
	return b.Spec.BucketName
}

func (b *GocbV2Bucket) UUID() (string, error) {
	bucketInfo, err := b.agent.GetBucket(context.Background(), &cbmgmtx.GetBucketOptions{
		BucketName: b.Spec.BucketName,
	})
	if err != nil {
		return "", fmt.Errorf("Unable to determine bucket UUID for %v: %w", b.GetName(), err)
	}
	return bucketInfo.UUID, nil
}

// GetAgent returns the underlying gocbcorex.Agent
func (b *GocbV2Bucket) GetAgent() *gocbcorex.Agent {
	return b.agent
}

// Close closes the agent connection to the bucket.
func (b *GocbV2Bucket) Close(ctx context.Context) {
	if err := b.agent.Close(); err != nil {
		WarnfCtx(ctx, "Error closing agent for bucket %s: %v", MD(b.BucketName()), err)
	}
	b.agent = nil
}

func (b *GocbV2Bucket) IsSupported(feature sgbucket.BucketStoreFeature) bool {
	switch feature {
	case sgbucket.BucketStoreFeatureSubdocOperations, sgbucket.BucketStoreFeatureXattrs, sgbucket.BucketStoreFeatureCrc32cMacroExpansion:
		// Available on all supported server versions
		return true
	case sgbucket.BucketStoreFeatureN1ql:
		// TODO: Check for query endpoints via management API
		return true
	case sgbucket.BucketStoreFeatureN1qlIfNotExistsDDL:
		return b.IsMinimumVersion(7, 1)
	case sgbucket.BucketStoreFeatureCreateDeletedWithXattr:
		// Available since Couchbase Server 6.6
		return b.IsMinimumVersion(6, 6)
	case sgbucket.BucketStoreFeaturePreserveExpiry, sgbucket.BucketStoreFeatureCollections:
		return b.IsMinimumVersion(7, 0)
	case sgbucket.BucketStoreFeatureSystemCollections, sgbucket.BucketStoreFeatureMultiXattrSubdocOperations:
		return b.IsMinimumVersion(7, 6)
	case sgbucket.BucketStoreFeatureMobileXDCR:
		return b.supportsHLV
	default:
		return false
	}
}

// IsMinimumVersion returns whether the connected cluster is at least the specified major/minor version.
func (b *GocbV2Bucket) IsMinimumVersion(requiredMajor, requiredMinor uint64) bool {
	return IsMinimumVersion(b.clusterCompatMajorVersion, b.clusterCompatMinorVersion, requiredMajor, requiredMinor)
}

func (b *GocbV2Bucket) StartDCPFeed(ctx context.Context, args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) error {
	groupID := ""
	return StartGocbDCPFeed(ctx, b, b.Spec.BucketName, args, callback, dbStats, DCPMetadataStoreInMemory, groupID)
}

func (b *GocbV2Bucket) GetStatsVbSeqno(maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, seqErr error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	uuids = make(map[uint16]uint64, maxVbno)
	highSeqnos = make(map[uint16]uint64, maxVbno)

	seqnoKey := "vb_%d:high_seqno"
	if useAbsHighSeqNo {
		seqnoKey = "vb_%d:abs_high_seqno"
	}

	for vbID := uint16(0); vbID < maxVbno; vbID++ {
		vb := vbID // capture for closure
		_, err := b.agent.StatsByVbucket(ctx, &gocbcorex.StatsByVbucketOptions{
			GroupName: "vbucket-seqno",
			VbucketID: vb,
		}, func(result gocbcorex.StatsDataResult) {
			expectedSeqKey := fmt.Sprintf(seqnoKey, vb)
			expectedUUIDKey := fmt.Sprintf("vb_%d:uuid", vb)

			if result.Key == expectedSeqKey {
				seqNo, parseErr := strconv.ParseUint(result.Value, 10, 64)
				if parseErr == nil {
					highSeqnos[vb] = seqNo
				}
			} else if result.Key == expectedUUIDKey {
				uuid, parseErr := strconv.ParseUint(result.Value, 10, 64)
				if parseErr == nil {
					uuids[vb] = uuid
				}
			}
		})
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get stats for vbucket %d: %w", vb, err)
		}
	}

	return uuids, highSeqnos, nil
}

func (b *GocbV2Bucket) GetMaxVbno() (uint16, error) {
	numVbs := b.agent.NumVbuckets()
	if numVbs == 0 {
		return 0, fmt.Errorf("unable to determine number of vbuckets from agent")
	}
	return uint16(numVbs), nil
}

// GetCCVSettings returns the highest CAS value across all vBuckets for a bucket with CCV enabled.
func (b *GocbV2Bucket) GetCCVSettings(ctx context.Context) (ccvEnabled bool, maxCAS map[VBNo]uint64, err error) {
	uri := "/pools/default/buckets/" + b.GetName()
	output, status, err := b.MgmtRequest(ctx, http.MethodGet, uri, "application/json", nil)
	if err != nil {
		return false, nil, RedactErrorf("unable to get CCV starting cas for bucket %q: %w", UD(b.GetName()), err)
	}
	if status != http.StatusOK {
		return false, nil, RedactErrorf("unable to get CCV starting cas for bucket %q, status %d", UD(b.GetName()), status)
	}

	var response struct {
		EnableCrossClusterVersioning *bool    `json:"enableCrossClusterVersioning"`
		VBucketsMaxCas               []string `json:"vBucketsMaxCas"`
	}
	if err := JSONUnmarshal(output, &response); err != nil {
		return false, nil, RedactErrorf("unable to parse bucket info JSON for %q: %w", UD(b.GetName()), err)
	}

	// In Server < 7.6.1 this field will not be present at all
	if response.EnableCrossClusterVersioning == nil {
		return false, nil, nil
	}
	// CCV supported but not enabled
	if !*response.EnableCrossClusterVersioning {
		InfofCtx(ctx, KeyAll, "Bucket %q does not have enableCrossClusterVersioning set", UD(b.GetName()))
		return false, nil, nil
	}

	numVBuckets, err := b.GetMaxVbno()
	if err != nil {
		return false, nil, fmt.Errorf("error getting vbucket count: %v", err)
	}

	highCAS := make(map[VBNo]uint64, numVBuckets)
	if len(response.VBucketsMaxCas) != int(numVBuckets) {
		InfofCtx(ctx, KeyBucket, "Bucket %q has enableCrossClusterVersioning=true but unexpected number of vbucket CAS values - expected %d, got %+v. Treating all imports as originating on this Couchbase Server cluster.", MD(b.GetName()), numVBuckets, response.VBucketsMaxCas)
		for i := range numVBuckets {
			highCAS[VBNo(i)] = 0
		}
		return true, highCAS, nil
	}

	for i, casStr := range response.VBucketsMaxCas {
		cas, err := parseUint64(casStr)
		if err != nil {
			return false, nil, fmt.Errorf("error parsing vbucket CAS value %q for vBucket %d: %v", casStr, i, err)
		}
		highCAS[VBNo(i)] = cas
	}

	return true, highCAS, nil
}

func (b *GocbV2Bucket) GetSpec() BucketSpec {
	return b.Spec
}

// This flushes the *entire* bucket associated with the collection (not just the collection).  Intended for test usage only.
func (b *GocbV2Bucket) Flush(ctx context.Context) error {

	if b.agent == nil {
		return fmt.Errorf("bucket %s has been closed", MD(b.GetName()))
	}

	workerFlush := func() (shouldRetry bool, err error, value any) {
		// TODO: Use gocbcorex management API to flush bucket
		uri := fmt.Sprintf("/pools/default/buckets/%s/controller/doFlush", b.GetName())
		_, statusCode, flushErr := b.MgmtRequest(ctx, http.MethodPost, uri, "", nil)
		if flushErr != nil {
			WarnfCtx(ctx, "Error flushing bucket %s: %v  Will retry.", MD(b.GetName()).Redact(), flushErr)
			return true, flushErr, nil
		}
		if statusCode != http.StatusOK {
			WarnfCtx(ctx, "Error flushing bucket %s: status %d  Will retry.", MD(b.GetName()).Redact(), statusCode)
			return true, fmt.Errorf("flush returned status %d", statusCode), nil
		}

		return false, nil, nil
	}

	err, _ := RetryLoop(ctx, "EmptyTestBucket", workerFlush, CreateDoublingSleeperFunc(12, 10))
	if err != nil {
		return err
	}

	// Wait until the bucket item count is 0, since flush is asynchronous
	worker := func() (shouldRetry bool, err error, value any) {
		itemCount, err := b.BucketItemCount(ctx)
		if err != nil {
			return false, err, nil
		}

		if itemCount == 0 {
			// bucket flushed, we're done
			return false, nil, nil
		}

		// Retry
		return true, nil, nil

	}

	// Kick off retry loop
	err, _ = RetryLoop(ctx, "Wait until bucket has 0 items after flush", worker, CreateMaxDoublingSleeperFunc(25, 100, 10000))
	if err != nil {
		return pkgerrors.Wrapf(err, "Error during Wait until bucket %s has 0 items after flush", MD(b.GetName()).Redact())
	}

	return nil

}

// BucketItemCount first tries to retrieve an accurate bucket count via N1QL,
// but falls back to the REST API if that cannot be done (when there's no index to count all items in a bucket)
func (b *GocbV2Bucket) BucketItemCount(ctx context.Context) (itemCount int, err error) {
	dataStoreNames, err := b.ListDataStores()
	if err != nil {
		return 0, err
	}

	for _, dsn := range dataStoreNames {
		ds, err := b.NamedDataStore(dsn)
		if err != nil {
			return 0, err
		}
		ns, ok := AsN1QLStore(ds)
		if !ok {
			return 0, fmt.Errorf("DataStore %v %T is not a N1QLStore", ds.GetName(), ds)
		}
		itemCount, err = QueryBucketItemCount(ctx, ns)
		if err == nil {
			return itemCount, nil
		}
	}

	// TODO: implement APIBucketItemCount for collections as part of CouchbaseBucketStore refactoring.  Until then, give flush a moment to finish
	time.Sleep(1 * time.Second)
	return 0, err
}

func (b *GocbV2Bucket) MgmtEps() (url []string, err error) {
	// TODO: Implement using gocbcorex agent endpoint listing
	// For now, build from the spec server address
	connSpec, parseErr := gocbconnstr.Parse(b.Spec.Server)
	if parseErr != nil {
		return nil, parseErr
	}
	var eps []string
	for _, host := range connSpec.Addresses {
		scheme := "http"
		port := gocbconnstr.DefaultHttpPort
		if b.Spec.IsTLS() {
			scheme = "https"
			port = gocbconnstr.DefaultSslHttpPort
		}
		if host.Port > 0 {
			port = host.Port
		}
		eps = append(eps, fmt.Sprintf("%s://%s:%d", scheme, host.Host, port))
	}
	if len(eps) == 0 {
		return nil, fmt.Errorf("No available Couchbase Server nodes")
	}
	return eps, nil
}

func (b *GocbV2Bucket) QueryEpsCount() (int, error) {
	// TODO: Implement using gocbcorex management API
	return 1, nil
}

// MetadataPurgeInterval gets the metadata purge interval for the bucket. Checks for a bucket-specific value before the cluster value.
func (b *GocbV2Bucket) MetadataPurgeInterval(ctx context.Context) (time.Duration, error) {
	return getMetadataPurgeInterval(ctx, b)
}

// VersionPruningWindow gets the version pruning window for the bucket.
func (b *GocbV2Bucket) VersionPruningWindow(ctx context.Context) (time.Duration, error) {
	uri := fmt.Sprintf("/pools/default/buckets/%s", b.GetName())
	respBytes, statusCode, err := b.MgmtRequest(ctx, http.MethodGet, uri, "application/json", nil)
	if err != nil {
		return 0, err
	}

	if statusCode == http.StatusForbidden {
		return 0, RedactErrorf("403 Forbidden attempting to access %s.  Bucket user must have Bucket Full Access and Bucket Admin roles to retrieve version pruning window.", UD(uri))
	} else if statusCode != http.StatusOK {
		return 0, fmt.Errorf("failed with status code %d", statusCode)
	}

	var response struct {
		VersionPruningWindowHrs int64 `json:"versionPruningWindowHrs,omitempty"`
	}
	if err := JSONUnmarshal(respBytes, &response); err != nil {
		return 0, err
	}

	return time.Duration(response.VersionPruningWindowHrs) * time.Hour, nil
}

func (b *GocbV2Bucket) MaxTTL(ctx context.Context) (int, error) {
	return getMaxTTL(ctx, b)
}

func (b *GocbV2Bucket) HttpClient(ctx context.Context) *http.Client {
	// TODO: Obtain HTTP client from gocbcorex agent
	return http.DefaultClient
}

func (b *GocbV2Bucket) BucketName() string {
	return b.GetName()
}

// MgmtRequest makes a request to the http couchbase management api. The uri is the non host part of the URL, such as /pools/default/buckets.
// This function will read the entire contents of the response and return the output bytes, the status code, and an error.
func (b *GocbV2Bucket) MgmtRequest(ctx context.Context, method, uri, contentType string, body io.Reader) ([]byte, int, error) {
	if contentType == "" && body != nil {
		return nil, 0, errors.New("Content-type must be specified for non-null body.")
	}

	mgmtEp, err := GoCBBucketMgmtEndpoint(b)
	if err != nil {
		return nil, 0, err
	}

	var username, password string
	if b.Spec.Auth != nil {
		username, password, _ = b.Spec.Auth.GetCredentials()
	}

	respBytes, statusCode, err := MgmtRequest(b.HttpClient(ctx), mgmtEp, method, uri, contentType, username, password, body)
	if err != nil {
		return nil, statusCode, err
	}

	return respBytes, statusCode, nil
}

// This prevents Sync Gateway from overflowing gocbcorex's pipeline
func (b *GocbV2Bucket) waitForAvailKvOp() {
	b.kvOps <- struct{}{}
}

func (b *GocbV2Bucket) releaseKvOp() {
	<-b.kvOps
}

// GetBucketOpDeadline returns a deadline for use in gocbcorex calls
func (b *GocbV2Bucket) getBucketOpDeadline() time.Time {
	opTimeout := DefaultGocbV2OperationTimeout
	configOpTimeout := b.Spec.BucketOpTimeout
	if configOpTimeout != nil {
		opTimeout = *configOpTimeout
	}
	return time.Now().Add(opTimeout)
}

func (b *GocbV2Bucket) GetCollectionManifest() (cbconfig.CollectionManifestJson, error) {
	ctx, cancel := context.WithDeadline(context.Background(), b.getBucketOpDeadline())
	defer cancel()

	manifest, err := b.agent.GetCollectionManifest(ctx, &cbmgmtx.GetCollectionManifestOptions{
		BucketName: b.Spec.BucketName,
	})
	if err != nil {
		return cbconfig.CollectionManifestJson{}, fmt.Errorf("failed to get collection manifest: %w", err)
	}

	return *manifest, nil
}

func GetIDForCollection(manifest cbconfig.CollectionManifestJson, scopeName, collectionName string) (uint32, bool) {
	for _, scope := range manifest.Scopes {
		if scope.Name != scopeName {
			continue
		}
		for _, coll := range scope.Collections {
			if coll.Name == collectionName {
				// UID in CollectionManifestJson may be a string, parse it
				uid, err := strconv.ParseUint(fmt.Sprintf("%v", coll.UID), 16, 32)
				if err != nil {
					return 0, false
				}
				return uint32(uid), true
			}
		}
	}
	return 0, false
}

// waitForAvailQueryOp prevents Sync Gateway from having too many concurrent
// queries against Couchbase Server
func (b *GocbV2Bucket) waitForAvailQueryOp() {
	b.queryOps <- struct{}{}
}

func (b *GocbV2Bucket) releaseQueryOp() {
	<-b.queryOps
}

func (b *GocbV2Bucket) ListDataStores() ([]sgbucket.DataStoreName, error) {
	if !b.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		return []sgbucket.DataStoreName{ScopeAndCollectionName{Scope: DefaultScope, Collection: DefaultCollection}}, nil
	}

	ctx, cancel := context.WithDeadline(context.Background(), b.getBucketOpDeadline())
	defer cancel()

	manifest, err := b.agent.GetCollectionManifest(ctx, &cbmgmtx.GetCollectionManifestOptions{
		BucketName: b.Spec.BucketName,
	})
	if err != nil {
		return nil, err
	}

	collections := make([]sgbucket.DataStoreName, 0)
	for _, s := range manifest.Scopes {
		// clients using system scopes should know what they're called,
		// and we don't want to accidentally iterate over other system collections
		if s.Name == SystemScope {
			continue
		}
		for _, c := range s.Collections {
			collections = append(collections, ScopeAndCollectionName{Scope: s.Name, Collection: c.Name})
		}
	}
	return collections, nil
}

// DropDataStore removes a collection from the bucket. This function will return immediately but the collection may take some time to delete.
func (b *GocbV2Bucket) DropDataStore(name sgbucket.DataStoreName) error {
	if b.agent == nil {
		return fmt.Errorf("bucket %s has been closed", MD(b.GetName()))
	}
	ctx, cancel := context.WithDeadline(context.Background(), b.getBucketOpDeadline())
	defer cancel()

	_, err := b.agent.DeleteCollection(ctx, &cbmgmtx.DeleteCollectionOptions{
		BucketName:     b.Spec.BucketName,
		ScopeName:      name.ScopeName(),
		CollectionName: name.CollectionName(),
	})
	return err
}

// CreateDataStore adds a collection from the bucket, and creates a scope if it does not exist. This code is synchronous and waits for the collection to be created.
func (b *GocbV2Bucket) CreateDataStore(ctx context.Context, name sgbucket.DataStoreName) error {
	if b.agent == nil {
		return fmt.Errorf("bucket %s has been closed", MD(b.GetName()))
	}

	opCtx, cancel := context.WithDeadline(ctx, b.getBucketOpDeadline())
	defer cancel()

	// create scope first (if it doesn't already exist)
	if name.ScopeName() != DefaultScope {
		_, err := b.agent.CreateScope(opCtx, &cbmgmtx.CreateScopeOptions{
			BucketName: b.Spec.BucketName,
			ScopeName:  name.ScopeName(),
		})
		if err != nil {
			// TODO: Improve error detection for scope-already-exists
			if !strings.Contains(err.Error(), "already exists") {
				return err
			}
		}
	}

	_, err := b.agent.CreateCollection(opCtx, &cbmgmtx.CreateCollectionOptions{
		BucketName:     b.Spec.BucketName,
		ScopeName:      name.ScopeName(),
		CollectionName: name.CollectionName(),
	})
	if err != nil {
		return err
	}

	// Wait until collection is usable
	return WaitForNoError(ctx, func() error {
		ds, dsErr := b.NamedDataStore(name)
		if dsErr != nil {
			return dsErr
		}
		_, existErr := ds.Exists("fakedocid")
		return existErr
	})
}

// DefaultDataStore returns the default collection for the bucket.
func (b *GocbV2Bucket) DefaultDataStore() sgbucket.DataStore {
	return &Collection{
		Bucket:         b,
		scopeName:      DefaultScope,
		collectionName: DefaultCollection,
	}
}

// NamedDataStore returns a collection on a bucket within the given scope and collection.
func (b *GocbV2Bucket) NamedDataStore(name sgbucket.DataStoreName) (sgbucket.DataStore, error) {
	c := &Collection{
		Bucket:         b,
		scopeName:      name.ScopeName(),
		collectionName: name.CollectionName(),
	}

	err := c.setCollectionID()
	if err != nil {
		if errors.Is(err, memdx.ErrUnknownCollectionID) {
			return nil, ErrAuthError
		}
		// TODO: check for scope not found error equivalent
		return nil, err
	}

	return c, nil
}

// ServerMetrics returns all the metrics for couchbase server.
func (b *GocbV2Bucket) ServerMetrics(ctx context.Context) (map[string]*dto.MetricFamily, error) {
	url := "/metrics/"
	resp, statusCode, err := b.MgmtRequest(ctx, http.MethodGet, url, "application/x-www-form-urlencoded", nil)
	if err != nil {
		return nil, err
	}

	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("Could not get metrics from %s. %s %s -> (%d) %s", b.GetName(), http.MethodGet, url, statusCode, string(resp))
	}

	// filter duplicates from couchbase server or TextToMetricFamilies will fail MB-43772
	lines := map[string]struct{}{}
	filteredOutput := []string{}
	for line := range strings.SplitSeq(string(resp), "\n") {
		_, ok := lines[line]
		if ok {
			continue
		}
		lines[line] = struct{}{}
		filteredOutput = append(filteredOutput, line)
	}
	filteredOutput = append(filteredOutput, "")
	var parser expfmt.TextParser
	mf, err := parser.TextToMetricFamilies(strings.NewReader(strings.Join(filteredOutput, "\n")))
	if err != nil {
		return nil, err
	}

	return mf, nil
}

// parseUint64 is a helper to parse a uint64 from a string, used in CCV settings parsing.
func parseUint64(s string) (uint64, error) {
	var val uint64
	_, err := fmt.Sscanf(s, "%d", &val)
	return val, err
}
