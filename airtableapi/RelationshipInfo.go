package airtableapi

type RelationshipInfo struct {
	Table 	   	   	Table
	Field 	   		Field
	RelatedTable 	Table
	RelatedField	Field
	RelationType   string // "OneToMany", "ManyToOne", or "ManyToMany"
}

func FindFieldByID(relations []RelationshipInfo, id string) (*RelationshipInfo, bool) {
	for _, relation := range relations {
		if relation.Field.ID == id {
			return &relation, true
		}
	}
	return nil, false
}

func FindRelationByTableIDAndFieldID(relations []RelationshipInfo, tableID string, fieldID string) (*RelationshipInfo, bool) {
	for _, relation := range relations {
		if relation.Table.ID == tableID {
			if relation.Field.ID == fieldID {
				return &relation, true
			}
		}
	}
	return nil, false
}