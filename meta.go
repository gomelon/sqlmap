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
	MetaMapper = "+sqlmap.Mapper"
	MetaSelect = "+sqlmap.Select"
	MetaInsert = "+sqlmap.Insert"
	MetaUpdate = "+sqlmap.Update"
	MetaDelete = "+sqlmap.Delete"
	MetaNone   = "+sqlmap.None"
)

var (
	MetaNames = []string{MetaMapper, MetaSelect, MetaInsert, MetaUpdate, MetaDelete, MetaNone}
)

type Mapper struct {
	Table   string
	Dialect string
}

type Select struct {
	Query  string
	Master bool
}

type Insert struct {
	Query string
}

type Update struct {
	Query string
}

type Delete struct {
	Query string
}

type None struct {
}

func Table(m *meta.Meta) string {
	return m.Property("Table")
}

func Dialect(m *meta.Meta) string {
	return m.Property("Dialect")
}

func Query(m *meta.Meta) string {
	return m.Property("Query")
}

func SetQuery(m *meta.Meta, query string) {
	m.SetProperty("Query", query)
}
