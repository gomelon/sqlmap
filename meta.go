package sqlmap

import (
	_ "embed"
	"github.com/gomelon/melon/data/engine"
	"github.com/gomelon/meta"
	"text/template"
)

//go:embed ctx_sql_db.tmpl
var TmplSQL string

func DefaultPkgGenFactory(defaultEngine engine.Engine) meta.PkgGenFactory {
	return meta.NewTmplPkgGenFactory(TmplSQL,
		meta.WithOutputFilename("sql_dao"),
		meta.WithFuncMapFactory(
			func(generator *meta.TmplPkgGen) template.FuncMap {
				return NewFunctions(generator, defaultEngine).FuncMap()
			},
		),
	)
}

const (
	MetaSqlTable  = "sql:table"
	MetaSqlSelect = "sql:select"
	MetaSqlInsert = "sql:insert"
	MetaSqlUpdate = "sql:update"
	MetaSqlDelete = "sql:delete"
	MetaSqlNone   = "sql:none"
)

var (
	MetaNames = []string{MetaSqlSelect, MetaSqlInsert, MetaSqlUpdate, MetaSqlDelete, MetaSqlNone}
)

func TableName(m *meta.Meta) string {
	return m.Property("name")
}

func TableDialect(m *meta.Meta) string {
	return m.Property("dialect")
}

func Query(m *meta.Meta) string {
	return m.Property("query")
}

func SetQuery(m *meta.Meta, query string) {
	m.SetProperty("query", query)
}
