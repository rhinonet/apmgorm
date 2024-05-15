// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package apmgorm // import "github.com/rhinonet/apmgorm/v2"

import (
	"context"
	"database/sql"
	"fmt"

	"gorm.io/gorm"

	"go.elastic.co/apm/module/apmsql/v2"
	"go.elastic.co/apm/v2"
)

const (
	apmContextKey  = "elasticapm:context"
	callbackPrefix = "elasticapm"
)

// WithContext returns a copy of db with ctx recorded for use by
// the callbacks registered via RegisterCallbacks.
func WithContext(ctx context.Context, db *gorm.DB) *gorm.DB {
	return db.Set(apmContextKey, ctx)
}

// RegisterCallbacks registers callbacks on db for reporting spans
// to Elastic APM. This is called automatically by apmgorm.Open;
// it is provided for cases where a *gorm.DB is acquired by other
// means.
func RegisterCallbacks(db *gorm.DB) {
	registerCallbacks(db, apmsql.DSNInfo{})
}

func registerCallbacks(db *gorm.DB, dsnInfo apmsql.DSNInfo) {
	driverName := db.Name()
	switch driverName {
	case "postgres":
		driverName = "postgresql"
	}
	spanTypePrefix := fmt.Sprintf("db.%s.", driverName)
	querySpanType := spanTypePrefix + "query"
	execSpanType := spanTypePrefix + "exec"

	createName := "gorm:create"
	_ = db.Callback().Create().Before(createName).Register(
		fmt.Sprintf("%s:before:%s", callbackPrefix, createName),
		newBeforeCallback(execSpanType))
	_ = db.Callback().Create().After(createName).Register(
		fmt.Sprintf("%s:after:%s", callbackPrefix, createName),
		newAfterCallback(dsnInfo))

	deleteName := "gorm:delete"
	_ = db.Callback().Delete().Before(deleteName).Register(
		fmt.Sprintf("%s:before:%s", callbackPrefix, deleteName),
		newBeforeCallback(execSpanType))
	_ = db.Callback().Delete().After(deleteName).Register(
		fmt.Sprintf("%s:after:%s", callbackPrefix, deleteName),
		newAfterCallback(dsnInfo))

	queryName := "gorm:query"
	_ = db.Callback().Query().Before(queryName).Register(
		fmt.Sprintf("%s:before:%s", callbackPrefix, queryName),
		newBeforeCallback(querySpanType))
	_ = db.Callback().Query().After(queryName).Register(
		fmt.Sprintf("%s:after:%s", callbackPrefix, queryName),
		newAfterCallback(dsnInfo))

	updateName := "gorm:update"
	_ = db.Callback().Update().Before(updateName).Register(
		fmt.Sprintf("%s:before:%s", callbackPrefix, updateName),
		newBeforeCallback(execSpanType))
	_ = db.Callback().Update().After(updateName).Register(
		fmt.Sprintf("%s:after:%s", callbackPrefix, updateName),
		newAfterCallback(dsnInfo))

	rowName := "gorm:row_query"
	_ = db.Callback().Row().Before(rowName).Register(
		fmt.Sprintf("%s:before:%s", callbackPrefix, rowName),
		newBeforeCallback(querySpanType))
	_ = db.Callback().Row().After(rowName).Register(
		fmt.Sprintf("%s:after:%s", callbackPrefix, rowName),
		newAfterCallback(dsnInfo))

	db.Scopes()
}

func scopeContext(scope *gorm.DB) (context.Context, bool) {
	value, ok := scope.Get(apmContextKey)
	if !ok {
		return nil, false
	}
	ctx, _ := value.(context.Context)
	return ctx, ctx != nil
}

func newBeforeCallback(spanType string) func(db *gorm.DB) {
	return func(scope *gorm.DB) {
		ctx, ok := scopeContext(scope)
		if !ok {
			return
		}
		span, ctx := apm.StartSpanOptions(ctx, "", spanType, apm.SpanOptions{
			ExitSpan: true,
		})
		if span.Dropped() {
			span.End()
			ctx = nil
		}
		scope.Set(apmContextKey, ctx)
	}
}

func newAfterCallback(dsnInfo apmsql.DSNInfo) func(db *gorm.DB) {
	return func(scope *gorm.DB) {
		ctx, ok := scopeContext(scope)
		if !ok {
			return
		}
		span := apm.SpanFromContext(ctx)
		if span == nil {
			return
		}
		span.Name = apmsql.QuerySignature(scope.Statement.SQL.String())
		span.Context.SetDestinationAddress(dsnInfo.Address, dsnInfo.Port)
		span.Context.SetDatabase(apm.DatabaseSpanContext{
			Instance:  dsnInfo.Database,
			Statement: scope.Statement.SQL.String(),
			Type:      "sql",
			User:      dsnInfo.User,
		})
		defer span.End()

		// Capture errors, except for "record not found", which may be expected.
		if err := scope.Error; err != nil {
			if err == sql.ErrNoRows {
				return
			}
			if e := apm.CaptureError(ctx, err); e != nil {
				e.Send()
			}
		}
	}
}
