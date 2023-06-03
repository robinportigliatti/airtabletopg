package airtable

import (
	"github.com/mehanizm/airtable"
)

// IsOneToMany checks if the field represents a OneToMany relationship
func IsOneToMany(f *airtable.Field, field *airtable.Field) bool {
	fPrefersSingleRecordLink := f.Options["prefersSingleRecordLink"].(bool)
	fieldPrefersSingleRecordLink := field.Options["prefersSingleRecordLink"].(bool)
	if !fPrefersSingleRecordLink && fieldPrefersSingleRecordLink{
		return true
	}
	return false
}

// IsManyToOne checks if the field represents a ManyToOne relationship
func IsManyToOne(f *airtable.Field, field *airtable.Field) bool {
	fPrefersSingleRecordLink := f.Options["prefersSingleRecordLink"].(bool)
	fieldPrefersSingleRecordLink := field.Options["prefersSingleRecordLink"].(bool)
	if fPrefersSingleRecordLink && !fieldPrefersSingleRecordLink{
		return true
	}
	return false
}

// IsManyToMany checks if the field represents a ManyToMany relationship
func IsManyToMany(f *airtable.Field, field *airtable.Field) bool {
	fPrefersSingleRecordLink := f.Options["prefersSingleRecordLink"].(bool)
	fieldPrefersSingleRecordLink := field.Options["prefersSingleRecordLink"].(bool)
	if !fPrefersSingleRecordLink && !fieldPrefersSingleRecordLink{
		return true
	}
	return false
}
