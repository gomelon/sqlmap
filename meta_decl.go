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
	MetaMapper = "sqlmap.Mapper"
	MetaSelect = "sqlmap.Select"
	MetaInsert = "sqlmap.Insert"
	MetaUpdate = "sqlmap.Update"
	MetaDelete = "sqlmap.Delete"
	MetaNone   = "sqlmap.None"
)

var (
	MetaNames = []string{MetaMapper, MetaSelect, MetaInsert, MetaUpdate, MetaDelete, MetaNone}
)

//Mapper
//+meta.Decl
type Mapper struct {
	Table   string
	Dialect string
}

type Querier interface {
	GetQuery() string
}

//Select
//+meta.Decl
type Select struct {
	Query  string
	Master bool
}

func (s *Select) GetQuery() string {
	return s.Query
}

//Insert
//+meta.Decl
type Insert struct {
	Query string
}

func (i *Insert) GetQuery() string {
	return i.Query
}

//Update
//+meta.Decl
type Update struct {
	Query string
}

func (u *Update) GetQuery() string {
	return u.Query
}

//Delete
//+meta.Decl
type Delete struct {
	Query string
}

func (d *Delete) GetQuery() string {
	return d.Query
}

//None
//+meta.Decl
type None struct {
}
