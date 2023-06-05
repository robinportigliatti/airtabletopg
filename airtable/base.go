package airtable

import (
	"github.com/mehanizm/airtable"
)

func FindTableByName(tables []*airtable.TableSchema, name string) (*airtable.TableSchema, bool) {
	for _, table := range tables {
		if table.Name == name {
			return table, true
		}
	}
	return nil, false
}

func FindFieldByName(fields []*airtable.Field, name string) (*airtable.Field, bool) {
	for _, field := range fields {
		if field.Name == name {
			return field, true
		}
	}
	return nil, false
}
