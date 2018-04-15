package dat

import (
	"bytes"
)

// InsertQueryBuilder contains the clauses for an INSERT statement
type InsertQueryBuilder struct {
	Execer

	isInterpolated bool
	table          string
	cols           []string
	query		   *Expression
	returnings     []string
}

// NewInsertBuilder creates a new InsertBuilder for the given table.
func NewInsertQueryBuilder(table string) *InsertQueryBuilder {
	if table == "" {
		logger.Error("InsertInto requires a table name.")
		return nil
	}
	return &InsertQueryBuilder{table: table, isInterpolated: EnableInterpolation}
}

// Columns appends columns to insert in the statement
func (b *InsertQueryBuilder) Columns(columns ...string) *InsertQueryBuilder {
	b.cols = columns
	return b
}

// Returning sets the columns for the RETURNING clause
func (b *InsertQueryBuilder) Returning(columns ...string) *InsertQueryBuilder {
	b.returnings = columns
	return b
}

func (b *InsertQueryBuilder) Query(sqlOrBuilder interface{}, a ...interface{}) *InsertQueryBuilder {
	switch t := sqlOrBuilder.(type) {
	default:
		panic("sqlOrbuilder accepts only {string, Builder} type")
	case Builder:
		sql, args := t.ToSQL()
		b.query = Expr(sql, args...)
	case string:
		b.query = Expr(t, a...)
	}
	return b
}

// ToSQL serialized the InsertBuilder to a SQL string
// It returns the string with placeholders and a slice of query arguments
func (b *InsertQueryBuilder) ToSQL() (string, []interface{}) {
	if len(b.table) == 0 {
		panic("no table specified")
	}
	lenCols := len(b.cols)
	if lenCols == 0 {
		panic("no columns specified")
	}

	var sql bytes.Buffer
	var args []interface{}
	var placeholderStartPos int64 = 1

	sql.WriteString("INSERT INTO ")
	sql.WriteString(b.table)
	sql.WriteString(" (")

	for i, c := range b.cols {
		if i > 0 {
			sql.WriteRune(',')
		}
		Dialect.WriteIdentifier(&sql, c)
	}
	sql.WriteString(") ")

	b.query.WriteRelativeArgs(&sql, &args, &placeholderStartPos)

	// Go thru the returning clauses
	for i, c := range b.returnings {
		if i == 0 {
			sql.WriteString(" RETURNING ")
		} else {
			sql.WriteRune(',')
		}
		Dialect.WriteIdentifier(&sql, c)
	}

	return sql.String(), args
}
