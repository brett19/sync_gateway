/*
Copyright 2023-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/cbqueryx"
	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
)

var _ N1QLStore = &ClusterOnlyN1QLStore{}

// ClusterOnlyN1qlStore implements the N1QLStore using only an agent connection.
// Currently still intended for use for operations against a single collection, but maintains that
// information via metadata, and so supports sharing of the underlying agent with other
// ClusterOnlyN1QLStore instances.
type ClusterOnlyN1QLStore struct {
	agent                    *gocbcorex.Agent
	bucketName               string // Used to build keyspace for query when not otherwise set
	scopeName                string // Used to build keyspace for query when not otherwise set
	collectionName           string // Used to build keyspace for query when not otherwise set
	supportsCollections      bool
	supportsIfNotExistsInDDL bool // 7.1.0+ MB-38737
}

func NewClusterOnlyN1QLStore(agent *gocbcorex.Agent, bucketName, scopeName, collectionName string) (*ClusterOnlyN1QLStore, error) {

	clusterOnlyn1qlStore := &ClusterOnlyN1QLStore{
		agent:          agent,
		bucketName:     bucketName,
		scopeName:      scopeName,
		collectionName: collectionName,
	}

	// TODO: Implement cluster version detection via gocbcorex
	// For now, assume modern cluster (7.1+)
	clusterOnlyn1qlStore.supportsCollections = true
	clusterOnlyn1qlStore.supportsIfNotExistsInDDL = true

	return clusterOnlyn1qlStore, nil

}

func (cl *ClusterOnlyN1QLStore) IsSupported(feature sgbucket.BucketStoreFeature) bool {
	switch feature {
	case sgbucket.BucketStoreFeatureN1ql:
		return true
	case sgbucket.BucketStoreFeatureCollections:
		return cl.supportsCollections
	case sgbucket.BucketStoreFeatureN1qlIfNotExistsDDL:
		return cl.supportsIfNotExistsInDDL
	default:
		return false
	}
}

func (cl *ClusterOnlyN1QLStore) GetName() string {
	return cl.bucketName
}

func (cl *ClusterOnlyN1QLStore) BucketName() string {
	return cl.bucketName
}

func (cl *ClusterOnlyN1QLStore) BuildDeferredIndexes(ctx context.Context, indexSet []string) error {
	return BuildDeferredIndexes(ctx, cl, indexSet)
}

func (cl *ClusterOnlyN1QLStore) CreateIndex(ctx context.Context, indexName string, expression string, filterExpression string, options *N1qlIndexOptions) error {
	return CreateIndex(ctx, cl, indexName, expression, filterExpression, options)
}

func (cl *ClusterOnlyN1QLStore) CreateIndexIfNotExists(ctx context.Context, indexName string, expression string, filterExpression string, options *N1qlIndexOptions) error {
	return CreateIndexIfNotExists(ctx, cl, indexName, expression, filterExpression, options)
}

func (cl *ClusterOnlyN1QLStore) CreatePrimaryIndex(ctx context.Context, indexName string, options *N1qlIndexOptions) error {
	return CreatePrimaryIndex(ctx, cl, indexName, options)
}

func (cl *ClusterOnlyN1QLStore) ExplainQuery(ctx context.Context, statement string, params map[string]any) (plan map[string]any, err error) {
	return ExplainQuery(ctx, cl, statement, params)
}

func (cl *ClusterOnlyN1QLStore) DropIndex(ctx context.Context, indexName string) error {
	return DropIndex(ctx, cl, indexName)
}

// IndexMetaKeyspaceID returns the value of keyspace_id for the system:indexes table for the collection.
func (cl *ClusterOnlyN1QLStore) IndexMetaKeyspaceID() string {
	return IndexMetaKeyspaceID(cl.bucketName, cl.scopeName, cl.collectionName)
}

// IndexMetaBucketID returns the value of bucket_id for the system:indexes table for the collection.
func (cl *ClusterOnlyN1QLStore) IndexMetaBucketID() string {
	if IsDefaultCollection(cl.scopeName, cl.collectionName) {
		return ""
	}
	return cl.bucketName
}

// IndexMetaScopeID returns the value of scope_id for the system:indexes table for the collection.
func (cl *ClusterOnlyN1QLStore) IndexMetaScopeID() string {
	if IsDefaultCollection(cl.scopeName, cl.collectionName) {
		return ""
	}
	return cl.scopeName
}

func (cl *ClusterOnlyN1QLStore) Query(ctx context.Context, statement string, params map[string]any, consistency ConsistencyMode, adhoc bool) (resultsIterator sgbucket.QueryResultIterator, err error) {
	keyspaceStatement := strings.Replace(statement, KeyspaceQueryToken, cl.EscapedKeyspace(), -1)

	// Convert named parameters to json.RawMessage
	namedArgs := make(map[string]json.RawMessage, len(params))
	for k, v := range params {
		paramBytes, marshalErr := JSONMarshal(v)
		if marshalErr != nil {
			return nil, marshalErr
		}
		namedArgs[k] = paramBytes
	}

	scanConsistency := cbqueryx.ScanConsistencyNotBounded
	if consistency == RequestPlus {
		scanConsistency = cbqueryx.ScanConsistencyRequestPlus
	}

	waitTime := 10 * time.Millisecond
	for i := 1; i <= MaxQueryRetries; i++ {
		TracefCtx(ctx, KeyQuery, "Executing N1QL query: %v - %+v", UD(keyspaceStatement), UD(params))
		queryResults, queryErr := cl.runQuery(ctx, keyspaceStatement, namedArgs, scanConsistency)
		if queryErr == nil {
			resultsIterator := &gocbcorexQueryIterator{
				stream:                     queryResults,
				concurrentQueryOpLimitChan: nil,
			}
			return resultsIterator, queryErr
		}

		// Timeout error - return named error
		if errors.Is(queryErr, context.DeadlineExceeded) {
			return resultsIterator, ErrViewTimeoutError
		}

		// Non-retry error - return
		if !isTransientIndexerError(queryErr) {
			WarnfCtx(ctx, "Error when querying index using statement: [%s] parameters: [%+v] error:%v", UD(keyspaceStatement), UD(params), queryErr)
			return resultsIterator, pkgerrors.WithStack(queryErr)
		}

		// Indexer error - wait then retry
		err = queryErr
		WarnfCtx(ctx, "Indexer error during query - retry %d/%d after %v.  Error: %v", i, MaxQueryRetries, waitTime, queryErr)
		time.Sleep(waitTime)

		waitTime = waitTime * 2
	}

	WarnfCtx(ctx, "Exceeded max retries for query when querying index using statement: [%s] parameters: [%+v], err:%v", UD(keyspaceStatement), UD(params), err)
	return nil, err
}

// executeQuery runs a N1QL query against the cluster.  Does not throttle query ops.
func (cl *ClusterOnlyN1QLStore) executeQuery(statement string) (sgbucket.QueryResultIterator, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queryResults, queryErr := cl.runQuery(ctx, statement, nil, cbqueryx.ScanConsistencyUnset)
	if queryErr != nil {
		return nil, queryErr
	}

	resultsIterator := &gocbcorexQueryIterator{
		stream:                     queryResults,
		concurrentQueryOpLimitChan: nil,
	}
	return resultsIterator, nil
}

func (cl *ClusterOnlyN1QLStore) executeStatement(statement string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	queryResults, queryErr := cl.runQuery(ctx, statement, nil, cbqueryx.ScanConsistencyUnset)
	if queryErr != nil {
		return queryErr
	}

	// Drain results to return any non-query errors
	for queryResults.HasMoreRows() {
		_, readErr := queryResults.ReadRow()
		if readErr != nil {
			return readErr
		}
	}
	return nil
}

func (cl *ClusterOnlyN1QLStore) runQuery(ctx context.Context, statement string, namedArgs map[string]json.RawMessage, scanConsistency cbqueryx.ScanConsistency) (gocbcorex.QueryResultStream, error) {
	return cl.agent.Query(ctx, &gocbcorex.QueryOptions{
		Statement:       statement,
		NamedArgs:       namedArgs,
		ScanConsistency: scanConsistency,
	})
}

func (cl *ClusterOnlyN1QLStore) clusterIndexManager(scopeName, collectionName string) *indexManager {
	return &indexManager{
		agent:          cl.agent,
		bucketName:     cl.bucketName,
		scopeName:      scopeName,
		collectionName: collectionName,
	}
}

func (cl *ClusterOnlyN1QLStore) WaitForIndexesOnline(ctx context.Context, indexNames []string, option WaitForIndexesOnlineOption) error {
	keyspace := strings.Join([]string{cl.bucketName, cl.scopeName, cl.collectionName}, ".")
	return WaitForIndexesOnline(ctx, keyspace, cl.clusterIndexManager(cl.scopeName, cl.collectionName), indexNames, option)
}

func (cl *ClusterOnlyN1QLStore) GetIndexMeta(ctx context.Context, indexName string) (exists bool, meta *IndexMeta, err error) {
	return GetIndexMeta(ctx, cl, indexName)
}

func (cl *ClusterOnlyN1QLStore) IsErrNoResults(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "no result")
}

// EscapedKeyspace returns the escaped fully-qualified identifier for the keyspace (e.g. `bucket`.`scope`.`collection`)
func (cl *ClusterOnlyN1QLStore) EscapedKeyspace() string {
	if !cl.supportsCollections {
		return fmt.Sprintf("`%s`", cl.bucketName)
	}
	return fmt.Sprintf("`%s`.`%s`.`%s`", cl.bucketName, cl.scopeName, cl.collectionName)
}

func (cl *ClusterOnlyN1QLStore) GetIndexes() (indexes []string, err error) {
	if cl.supportsCollections {
		return GetAllIndexes(cl.clusterIndexManager(cl.scopeName, cl.collectionName))
	} else {
		return GetAllIndexes(cl.clusterIndexManager("", ""))
	}
}

// waitUntilQueryServiceReady will wait for the specified duration until the query service is available.
func (cl *ClusterOnlyN1QLStore) waitUntilQueryServiceReady(timeout time.Duration) error {
	// TODO: Implement query service readiness check via gocbcorex
	time.Sleep(100 * time.Millisecond)
	return nil
}

// ClusterOnlyN1QLStore allows callers to set the scope and collection per operation
func (cl *ClusterOnlyN1QLStore) SetScopeAndCollection(scName ScopeAndCollectionName) {
	cl.scopeName = scName.Scope
	cl.collectionName = scName.Collection
}
