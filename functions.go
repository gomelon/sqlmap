package sqlmap

import (
	"context"
	"errors"
	"fmt"
	"github.com/gomelon/melon/data"
	"github.com/gomelon/melon/data/engine"
	"github.com/gomelon/melon/data/query"
	"github.com/gomelon/melon/third_party/sqlx"
	"github.com/gomelon/meta"
	"github.com/gomelon/metas/msql/parser"
	"github.com/huandu/xstrings"
	"go/types"
	"strings"
	"text/template"
)

type functions struct {
	ruleParser    *data.RuleParser
	pkgParser     *meta.PkgParser
	metaParser    *meta.Parser
	defaultEngine engine.Engine
}

func NewFunctions(gen *meta.TmplPkgGen, defaultEngine engine.Engine) *functions {
	return &functions{
		ruleParser:    data.NewRuleParser(),
		pkgParser:     gen.PkgParser(),
		metaParser:    gen.MetaParser(),
		defaultEngine: defaultEngine,
	}
}

func (f *functions) FuncMap() template.FuncMap {
	return map[string]any{
		"queryType":         f.QueryType,
		"selectMeta":        f.SelectMeta,
		"rewriteSelectStmt": f.RewriteSelectStmt,
		"scanFields":        f.ScanFields,
		"deleteMeta":        f.DeleteMeta,
		"rewriteDeleteStmt": f.RewriteDeleteStmt,
		"queryArgs":         f.QueryArgs,
		"dialect":           f.Dialect,
	}
}

func (f *functions) QueryType(method types.Object) (queryType string, err error) {
	queryType, _, err = f.subjectMeta(method)
	if err != nil {
		return
	}

	if len(queryType) > 0 {
		return
	}

	subject, err := f.ruleParser.ParseSubject(method.Name())
	if err != nil {
		return "", err
	}
	switch subject {
	case query.SubjectFind, query.SubjectCount, query.SubjectExists:
		queryType = MetaSqlSelect
	case query.SubjectDelete:
		queryType = MetaSqlDelete
	}
	return
}

func (f *functions) SelectMeta(method types.Object, tableMeta *meta.Meta) (selectMeta *meta.Meta, err error) {
	metaName, selectMetaGroup, err := f.subjectMeta(method)
	if err != nil || (len(metaName) > 0 && metaName != MetaSqlSelect) {
		return
	}

	if selectMetaGroup == nil {
		selectMeta = meta.New(MetaSqlSelect)
	} else {
		originSelectMeta := selectMetaGroup[0]
		if len(Query(originSelectMeta)) > 0 {
			selectMeta = originSelectMeta
			return
		}
		selectMeta = meta.New(MetaSqlSelect)
		selectMeta.SetProperties(originSelectMeta.Properties())
	}

	parsedQuery, err := f.ruleParser.Parse(method.Name())
	if parsedQuery == nil ||
		(parsedQuery.Subject() != query.SubjectFind &&
			parsedQuery.Subject() != query.SubjectCount &&
			parsedQuery.Subject() != query.SubjectExists) {
		if selectMeta != nil {
			err = fmt.Errorf("can not parse method to query,method=%s, possible reasons is %w",
				method.String(), err)
		} else {
			err = nil
		}
		return
	}

	parsedQuery = parsedQuery.With(query.WithTable(query.NewTable(TableName(tableMeta))))
	if parsedQuery.FilterGroup() != nil {
		toArgMethodParams := f.methodParamsWithoutCtx(method)
		namedArgs := make([]string, 0, len(toArgMethodParams))
		for _, param := range toArgMethodParams {
			namedArgs = append(namedArgs, param.Name())
		}
		err = parsedQuery.FilterGroup().FillNamedArgs(namedArgs)
		if err != nil {
			return
		}
	}

	sql, err := f.translateQuery(tableMeta, parsedQuery)
	if err != nil {
		return
	}

	SetQuery(selectMeta, sql)
	return
}

func (f *functions) RewriteSelectStmt(method types.Object, table *meta.Meta, sel *meta.Meta) (query string, err error) {
	dialect := f.Dialect(table)

	originQuery := Query(sel)
	query, _, err = f.compileNamedQuery(originQuery, dialect)

	sqlParser, err := parser.New(dialect, query)
	if err != nil {
		err = fmt.Errorf("parse sql fail: %w,method=[%s],sql=%s", err, method.String(), originQuery)
		return
	}
	selectColumns, err := sqlParser.SelectColumns()
	if err != nil {
		err = fmt.Errorf("parse sql fail: %w", err)
		return
	}

	if len(selectColumns) == 1 && selectColumns[0].Alias == "*" {
		queryResultObject := f.pkgParser.FirstResult(method)
		rowType := f.pkgParser.UnderlyingType(queryResultObject.Type())
		rowStruct, ok := rowType.Underlying().(*types.Struct)
		if !ok {
			err = fmt.Errorf("parse sql fail: query result must a struct when select *, method=[%s],sql=%s",
				method.String(), originQuery)
			return
		}

		column := selectColumns[0]
		numFields := rowStruct.NumFields()
		columnNames := make([]string, 0, numFields)
		for i := 0; i < numFields; i++ {
			columnName := xstrings.ToSnakeCase(rowStruct.Field(i).Name())
			columnNames = append(columnNames, f.connectTableQualifier(column.TableQualifier, columnName))
		}

		qualifierStarStr := f.connectTableQualifier(column.TableQualifier, "*")
		selectColumnStr := strings.Join(columnNames, ", ")
		query = strings.Replace(query, qualifierStarStr, selectColumnStr, 1)
	}
	return
}

func (f *functions) DeleteMeta(method types.Object, tableMeta *meta.Meta) (deleteMeta *meta.Meta, err error) {
	directive, deleteMetaGroup, err := f.subjectMeta(method)
	if err != nil || (len(directive) > 0 && directive != MetaSqlDelete) {
		return
	}

	if deleteMetaGroup == nil {
		deleteMeta = meta.New(MetaSqlDelete)
	} else {
		originDeleteMeta := deleteMetaGroup[0]
		if len(Query(originDeleteMeta)) > 0 {
			deleteMeta = originDeleteMeta
			return
		}
		deleteMeta = meta.New(MetaSqlDelete)
		deleteMeta.SetProperties(originDeleteMeta.Properties())
	}

	parsedQuery, err := f.ruleParser.Parse(method.Name())
	if parsedQuery == nil || parsedQuery.Subject() != query.SubjectDelete {
		if deleteMeta != nil {
			err = fmt.Errorf("can not parse method to query,method=%s, possible reasons is %w",
				method.String(), err)
		} else {
			err = nil
		}
		return
	}

	parsedQuery = parsedQuery.With(query.WithTable(query.NewTable(TableName(tableMeta))))
	if parsedQuery.FilterGroup() != nil {
		toArgMethodParams := f.methodParamsWithoutCtx(method)
		namedArgs := make([]string, 0, len(toArgMethodParams))
		for _, param := range toArgMethodParams {
			namedArgs = append(namedArgs, param.Name())
		}
		err = parsedQuery.FilterGroup().FillNamedArgs(namedArgs)
		if err != nil {
			return
		}
	}

	sql, err := f.translateQuery(tableMeta, parsedQuery)
	if err != nil {
		return
	}

	SetQuery(deleteMeta, sql)
	return
}

func (f *functions) RewriteDeleteStmt(_ types.Object, table *meta.Meta, queryMeta *meta.Meta) (query string, err error) {
	dialect := f.Dialect(table)
	query, _, err = f.compileNamedQuery(Query(queryMeta), dialect)
	return
}

func (f *functions) ScanFields(method types.Object, table *meta.Meta, sql string, item string) (string, error) {
	dialect := f.Dialect(table)

	var err error

	sqlParser, err := parser.New(dialect, sql)
	if err != nil {
		return "", fmt.Errorf("parse sql fail: %w, method=[%s],sql=%s", err, method.String(), sql)
	}
	columns, err := sqlParser.SelectColumns()
	if err != nil {
		return "", fmt.Errorf("parse sql fail: %w,method=[%s],sql=%s", err, method.String(), sql)
	}

	queryResultObject := f.pkgParser.FirstResult(method)
	rowType := f.pkgParser.UnderlyingType(queryResultObject.Type())

	var result string
	switch rowType := rowType.(type) {
	case *types.Struct:
		result, err = f.scanFieldsForStruct(rowType, columns, item)
	case *types.Basic:
		result, err = f.scanFieldsForBasic(rowType, columns, item)
	}

	if err != nil {
		return "", fmt.Errorf("parse sql fail:%w, method=[%s],sql=%s", err, method.String(), sql)
	}

	return result, nil
}

func (f *functions) QueryArgs(method types.Object, table *meta.Meta, queryMeta *meta.Meta) (nameArgsStr string, err error) {
	dialect := f.Dialect(table)
	originQuery := Query(queryMeta)
	_, queryNames, err := f.compileNamedQuery(originQuery, dialect)
	if err != nil {
		return
	}

	toArgMethodParams := f.methodParamsWithoutCtx(method)
	if len(queryNames) == 0 {
		f.positionArgsStr(toArgMethodParams)
		return
	}

	nameArgsStr, err = f.nameArgsStr(queryNames, toArgMethodParams)
	if err != nil {
		err = fmt.Errorf("parse sql fail: %w, method=[%s],sql=%s", err, method.String(), originQuery)
	}
	return
}

func (f *functions) Dialect(table *meta.Meta) string {
	return f.engine(table).Dialect()
}

func (f *functions) positionArgsStr(toArgsMethodParams []types.Object) string {
	if len(toArgsMethodParams) == 0 {
		return ""
	}
	argsBuilder := strings.Builder{}
	argsBuilder.Grow(64)
	for _, queryName := range toArgsMethodParams {
		argsBuilder.WriteString(queryName.Name())
		argsBuilder.WriteRune(',')

	}
	return argsBuilder.String()
}

func (f *functions) nameArgsStr(queryNames []string, toArgsMethodParams []types.Object) (string, error) {
	if len(toArgsMethodParams) != len(queryNames) {
		err := fmt.Errorf("wrong number of args, want %d got %d", len(queryNames), len(toArgsMethodParams))
		return "", err
	}

	argsBuilder := strings.Builder{}
	argsBuilder.Grow(64)
	for _, queryName := range queryNames {
		argsBuilder.WriteString(queryName)
		argsBuilder.WriteRune(',')

	}
	return argsBuilder.String(), nil
}

func (f *functions) scanFieldsForBasic(_ *types.Basic, columns []*parser.Column,
	item string) (string, error) {
	if len(columns) > 1 || columns[0].Alias == "*" {
		return "", errors.New("when the query result is a basic type, select must be a specified field")
	}
	return "&" + item, nil
}

func (f *functions) scanFieldsForStruct(rowType *types.Struct, columns []*parser.Column,
	item string) (string, error) {
	if len(columns) == 1 && columns[0].Alias == "*" {
		return f.scanFieldsForStar(rowType, item)
	} else {
		return f.scanFieldsForMultipleColumn(rowType, columns, item)
	}

}

func (f *functions) scanFieldsForStar(rowType *types.Struct, item string) (string, error) {
	numFields := rowType.NumFields()
	toScanFieldNames := make([]string, 0, numFields)
	for i := 0; i < numFields; i++ {
		fieldName := rowType.Field(i).Name()
		toScanFieldName := "&" + item + "." + fieldName
		toScanFieldNames = append(toScanFieldNames, toScanFieldName)
	}
	return strings.Join(toScanFieldNames, ", "), nil
}

func (f *functions) scanFieldsForMultipleColumn(rowType *types.Struct, columns []*parser.Column,
	item string) (result string, err error) {

	toScanFieldNames := make([]string, 0, len(columns))
	structFieldNames := make(map[string]bool, rowType.NumFields())
	for i := 0; i < rowType.NumFields(); i++ {
		structFieldNames[rowType.Field(i).Name()] = true
	}
	for _, column := range columns {
		if column.Alias == "*" {
			err = fmt.Errorf("msql: unsupported * mixed with specified fields query")
			return
		}
		fieldName := xstrings.ToCamelCase(column.Alias)
		if !structFieldNames[fieldName] {
			err = fmt.Errorf("msql: can't find field name in struct, field=%s,rowType=%s",
				fieldName, rowType.String())
		}

		toScanFieldName := "&" + item + "." + fieldName
		toScanFieldNames = append(toScanFieldNames, toScanFieldName)
	}
	result = strings.Join(toScanFieldNames, ", ")
	return
}

func (f *functions) connectTableQualifier(tableQualifier, column string) string {
	if len(tableQualifier) == 0 {
		return column
	}
	return tableQualifier + "." + column
}

func (f *functions) engine(table *meta.Meta) engine.Engine {
	dialect := TableDialect(table)
	if len(dialect) == 0 || dialect == f.defaultEngine.Dialect() {
		return f.defaultEngine
	}
	//TODO support multiple engine
	return nil
}

func (f *functions) translateQuery(tableMeta *meta.Meta, q *query.Query) (sql string, err error) {
	dialectEngine := f.engine(tableMeta)
	if dialectEngine == nil {
		err = fmt.Errorf("unsupported dialect,dialect=%s", TableDialect(tableMeta))
		return
	}
	translator := query.NewRDBTranslator(dialectEngine)
	return translator.Translate(context.Background(), q)
}

func (f *functions) compileNamedQuery(namedQuery, dialect string) (query string, names []string, err error) {
	bindType := sqlx.BindType(dialect)
	if bindType == 0 {
		err = fmt.Errorf("unsupported dialect,dialect=%s", dialect)
		return
	}
	query, names, err = sqlx.CompileNamedQuery([]byte(namedQuery), bindType)
	return
}

func (f *functions) subjectMeta(method types.Object) (metaName string, group meta.Group, err error) {
	metaGroups := f.metaParser.ObjectMetaGroups(method, MetaNames...)
	if len(metaGroups) > 1 {
		err = fmt.Errorf("method can not use multiple %v,method=%s", MetaNames, method.String())
	} else if len(metaGroups) == 1 {
		for k, v := range metaGroups {
			metaName = k
			group = v
			break
		}
	}
	return
}

func (f *functions) methodParamsWithoutCtx(method types.Object) []types.Object {
	methodParams := f.pkgParser.Params(method)
	var toArgMethodParams []types.Object
	if len(methodParams) > 0 && f.pkgParser.AssignableToCtx(methodParams[0].Type()) {
		toArgMethodParams = methodParams[1:]
	} else {
		toArgMethodParams = methodParams
	}
	return toArgMethodParams
}
