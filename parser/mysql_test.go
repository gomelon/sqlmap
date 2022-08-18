package parser

import (
	"reflect"
	"testing"
)

func Test_mySQLParser_Type(t *testing.T) {
	type fields struct {
		SQL string
	}
	tests := []struct {
		name    string
		fields  fields
		want    Type
		wantErr bool
	}{
		{
			name:    "Select",
			fields:  fields{SQL: "SELECT * FROM user"},
			want:    TypeSelect,
			wantErr: false,
		},
		{
			name:    "Insert",
			fields:  fields{SQL: "INSERT INTO user(name, age) VALUES ('Lucy', 18)"},
			want:    TypeInsert,
			wantErr: false,
		},
		{
			name:    "Update",
			fields:  fields{SQL: "UPDATE user SET name = 'Lily' WHERE id = 1"},
			want:    TypeUpdate,
			wantErr: false,
		},
		{
			name:    "Delete",
			fields:  fields{SQL: "DELETE FROM user WHERE id = 1"},
			want:    TypeDelete,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := NewMySQL(tt.fields.SQL)
			if (err != nil) != tt.wantErr {
				t.Errorf("Type() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got, err := m.Type()
			if (err != nil) != tt.wantErr {
				t.Errorf("Type() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Type() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mySQLParser_SelectColumns(t *testing.T) {
	type fields struct {
		SQL string
	}
	tests := []struct {
		name    string
		fields  fields
		want    []*Column
		wantErr bool
	}{
		{
			name:   "Simple",
			fields: fields{SQL: "SELECT name FROM user"},
			want: []*Column{
				{
					Alias:          "name",
					TableQualifier: "",
				},
			},
			wantErr: false,
		},
		{
			name: "Star",
			fields: fields{
				SQL: "SELECT * FROM `user` u WHERE username = 'Lily'",
			},
			want: []*Column{
				{
					Alias:          "*",
					TableQualifier: "",
				},
			},
			wantErr: false,
		},
		{
			name: "Star With Table Qualifier",
			fields: fields{
				SQL: "SELECT u.*, a.phone FROM `user` u INNER JOIN address a ON u.id = a.user_id WHERE username = 'Lily'",
			},
			want: []*Column{
				{
					Alias:          "*",
					TableQualifier: "u",
				},
				{
					Alias:          "phone",
					TableQualifier: "a",
				},
			},
			wantErr: false,
		},
		{
			name: "Complex",
			fields: fields{
				SQL: "SELECT id, name, sex AS gender, count(1) count, " +
					"(CASE WHEN age > 18 THEN 1 ELSE 0 END) is_adult " +
					"FROM `user` u WHERE username = 'Lily'",
			},
			want: []*Column{
				{
					Alias:          "id",
					TableQualifier: "",
				},
				{
					Alias:          "name",
					TableQualifier: "",
				},
				{
					Alias:          "gender",
					TableQualifier: "",
				},
				{
					Alias:          "count",
					TableQualifier: "",
				},
				{
					Alias:          "is_adult",
					TableQualifier: "",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := NewMySQL(tt.fields.SQL)
			if (err != nil) != tt.wantErr {
				t.Errorf("Type() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got, err := m.SelectColumns()
			if (err != nil) != tt.wantErr {
				t.Errorf("SelectColumns() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SelectColumns() got = %v, want %v", got, tt.want)
			}
		})
	}
}
