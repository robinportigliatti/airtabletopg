package airtable

import (
	"github.com/mehanizm/airtable"
)

// prefersSingleRecordLink is a helper function that gets the value of "prefersSingleRecordLink" from the field options.
func prefersSingleRecordLink(f *airtable.Field) bool {
	return f.Options["prefersSingleRecordLink"].(bool)
}

// IsOneToMany checks if the field represents a OneToMany relationship.
func IsOneToMany(f *airtable.Field, field *airtable.Field) bool {
	return !prefersSingleRecordLink(f) && prefersSingleRecordLink(field)
}

// IsManyToOne checks if the field represents a ManyToOne relationship.
func IsManyToOne(f *airtable.Field, field *airtable.Field) bool {
	return prefersSingleRecordLink(f) && !prefersSingleRecordLink(field)
}

// IsManyToMany checks if the field represents a ManyToMany relationship.
func IsManyToMany(f *airtable.Field, field *airtable.Field) bool {
	return !prefersSingleRecordLink(f) && !prefersSingleRecordLink(field)
}
