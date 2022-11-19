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
	"github.com/gomelon/sqlmap/parser"
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
		"buildMapper":       f.BuildMapper,
		"queryType":         f.QueryType,
		"buildSelect":       f.BuildSelect,
		"buildDelete":       f.BuildDelete,
		"rewriteSelectStmt": f.RewriteSelectStmt,
		"rewriteDeleteStmt": f.RewriteDeleteStmt,
		"scanFields":        f.ScanFields,
		"queryArgs":         f.QueryArgs,
		"dialect":           f.Dialect,
	}
}

func (f *functions) BuildMapper(obj types.Object) (*Mapper, error) {
	objectMeta := f.metaParser.ObjectMeta(obj, MetaMapper)
	mapper := &Mapper{}
	err := objectMeta.MapTo(mapper)
	return mapper, err
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
		queryType = MetaSelect
	case query.SubjectDelete:
		queryType = MetaDelete
	}
	return
}

func (f *functions) BuildSelect(method types.Object, mapper *Mapper) (selectMeta *Select, err error) {
	metaName, selectMetaGroup, err := f.subjectMeta(method)
	if err != nil {
		return
	}

	if metaName != "" && metaName != MetaSelect {
		err = fmt.Errorf("expected %s but %s,method=%s", MetaSelect, metaName, method.String())
		return
	}

	selectMeta = &Select{}
	if selectMetaGroup != nil && len(selectMetaGroup) > 0 {
		err = selectMetaGroup[0].MapTo(selectMeta)
		return
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

	parsedQuery = parsedQuery.With(query.WithTable(query.NewTable(mapper.Table)))
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

	sql, err := f.translateQuery(mapper, parsedQuery)
	if err != nil {
		return
	}

	selectMeta.Query = sql
	return
}

func (f *functions) RewriteSelectStmt(method types.Object, mapper *Mapper, sel *Select) (query string, err error) {
	dialect := f.Dialect(mapper)

	originQuery := sel.Query
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

func (f *functions) BuildDelete(method types.Object, mapper *Mapper) (deleteMeta *Delete, err error) {
	metaName, deleteMetaGroup, err := f.subjectMeta(method)
	if err != nil {
		return
	}

	if len(metaName) > 0 && metaName != MetaDelete {
		err = fmt.Errorf("expected %s but %s,method=%s", MetaDelete, metaName, method.String())
		return
	}

	deleteMeta = &Delete{}
	if deleteMetaGroup != nil && len(deleteMetaGroup) > 0 {
		err = deleteMetaGroup[0].MapTo(deleteMeta)
		return
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

	parsedQuery = parsedQuery.With(query.WithTable(query.NewTable(mapper.Table)))
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

	sql, err := f.translateQuery(mapper, parsedQuery)
	if err != nil {
		return
	}

	deleteMeta.Query = sql
	return
}

func (f *functions) RewriteDeleteStmt(_ types.Object, mapper *Mapper, deleteMeta *Delete) (query string, err error) {
	dialect := f.Dialect(mapper)
	query, _, err = f.compileNamedQuery(deleteMeta.Query, dialect)
	return
}

func (f *functions) ScanFields(method types.Object, mapper *Mapper, sql string, item string) (string, error) {
	dialect := f.Dialect(mapper)

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

func (f *functions) QueryArgs(method types.Object, mapper *Mapper, querier Querier) (nameArgsStr string, err error) {
	dialect := f.Dialect(mapper)
	originQuery := querier.GetQuery()
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

func (f *functions) Dialect(mapper *Mapper) string {
	return f.engine(mapper).Dialect()
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

func (f *functions) engine(mapper *Mapper) engine.Engine {
	dialect := mapper.Dialect
	if len(dialect) == 0 || dialect == f.defaultEngine.Dialect() {
		return f.defaultEngine
	}
	//TODO support multiple engine
	return nil
}

func (f *functions) translateQuery(mapper *Mapper, q *query.Query) (sql string, err error) {
	dialectEngine := f.engine(mapper)
	if dialectEngine == nil {
		err = fmt.Errorf("unsupported dialect,dialect=%s", mapper.Dialect)
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
