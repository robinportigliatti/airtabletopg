package airtableapi

type Field struct {
	ID          string                 `json:"id"`
	Type        string                 `json:"type"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Options     map[string]interface{} `json:"options"`
}

// IsOneToMany checks if the field represents a OneToMany relationship
func (f *Field) IsOneToMany(field *Field) bool {
	fPrefersSingleRecordLink := f.Options["prefersSingleRecordLink"].(bool)
	fieldPrefersSingleRecordLink := field.Options["prefersSingleRecordLink"].(bool)
	if !fPrefersSingleRecordLink && fieldPrefersSingleRecordLink{
		return true
	}
	return false
}

// IsManyToOne checks if the field represents a ManyToOne relationship
func (f *Field) IsManyToOne(field *Field) bool {
	fPrefersSingleRecordLink := f.Options["prefersSingleRecordLink"].(bool)
	fieldPrefersSingleRecordLink := field.Options["prefersSingleRecordLink"].(bool)
	if fPrefersSingleRecordLink && !fieldPrefersSingleRecordLink{
		return true
	}
	return false
}

// IsManyToMany checks if the field represents a ManyToMany relationship
func (f *Field) IsManyToMany(field *Field) bool {
	fPrefersSingleRecordLink := f.Options["prefersSingleRecordLink"].(bool)
	fieldPrefersSingleRecordLink := field.Options["prefersSingleRecordLink"].(bool)
	if !fPrefersSingleRecordLink && !fieldPrefersSingleRecordLink{
		return true
	}
	return false
}
