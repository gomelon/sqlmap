package parser

import (
	"errors"
	"fmt"
	"github.com/xwb1989/sqlparser"
)

type mySQL struct {
	SQL  string
	stmt sqlparser.Statement
}

func NewMySQL(sql string) (*mySQL, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	return &mySQL{
		SQL:  sql,
		stmt: stmt,
	}, nil
}

func (m *mySQL) Type() (Type, error) {
	switch m.stmt.(type) {
	case *sqlparser.Select:
		return TypeSelect, nil
	case *sqlparser.Insert:
		return TypeInsert, nil
	case *sqlparser.Update:
		return TypeUpdate, nil
	case *sqlparser.Delete:
		return TypeDelete, nil
	default:
		return 0, errors.New("sql parser: unsupported sql type")
	}
}

func (m *mySQL) SelectColumns() ([]*Column, error) {
	stmt, ok := m.stmt.(*sqlparser.Select)
	if !ok {
		return []*Column{}, errors.New("sql parser: not a select query")
	}
	columns := make([]*Column, 0, len(stmt.SelectExprs))
	for _, selectExpr := range stmt.SelectExprs {
		column, err := m.selectColumn(selectExpr)
		if err != nil {
			return columns, err
		}
		columns = append(columns, column)
	}
	return columns, nil
}

func (m *mySQL) selectColumn(selectExpr sqlparser.SelectExpr) (*Column, error) {
	column := &Column{}
	switch expr := selectExpr.(type) {
	case *sqlparser.StarExpr:
		column.Alias = "*"
		column.TableQualifier = expr.TableName.Name.String()
	case *sqlparser.AliasedExpr:
		as := expr.As.String()
		if len(as) > 0 {
			column.Alias = as
			column.TableQualifier = ""
		} else {
			switch aliasedExprExpr := expr.Expr.(type) {
			case *sqlparser.ColName:
				column.Alias = aliasedExprExpr.Name.String()
				column.TableQualifier = aliasedExprExpr.Qualifier.Name.String()
			default:
				return column, fmt.Errorf("sql parser: unsupported column expr [%T]", expr)
			}
		}
	default:
		return column, fmt.Errorf("sql parser: unsupported column expr [%T]", expr)
	}
	return column, nil
}
