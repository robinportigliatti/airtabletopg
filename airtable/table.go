package airtable

import (
	"github.com/mehanizm/airtable"
)

func FindTableByID(tables []*airtable.TableSchema, id string) (*airtable.TableSchema, bool) {
	for _, table := range tables {
		if table.ID == id {
			return table, true
		}
	}
	return nil, false
}

func FindFieldByID(t *airtable.TableSchema, id string) (*airtable.Field, bool) {
	for _, field := range t.Fields {
		if field.ID == id {
			return field, true
		}
	}
	return nil, false
}

func FindFieldByIDInAllTables(tables []*airtable.TableSchema, id string) (*airtable.Field, bool) {
	for _, table := range tables {
		for _, field := range table.Fields {
			if field.ID == id {
				return field, true
			}
		}
	}
	return nil, false
}

func AnalyzeRelationships(t []*airtable.TableSchema) []*RelationshipInfo {
	var relations []*RelationshipInfo
	for _, table := range t { 
		for _, field := range table.Fields {
			// If the field is not a multipleRecordLink, skip it
			if field.Type != "multipleRecordLinks" {
				continue
			}
			//fmt.Println(field)
			relatedTable, _ := FindTableByID(t, field.Options["linkedTableId"].(string)) 
			relatedField, _ := FindFieldByIDInAllTables(t, field.Options["inverseLinkFieldId"].(string))
			relation := RelationshipInfo{
				Table: *table,
				Field: *field,
				RelatedTable: *relatedTable,
				RelatedField: *relatedField,
			}


			// Determine the type of relation
			if IsOneToMany(field, relatedField) {
				relation.RelationType = "OneToMany"
			} else if IsManyToOne(field, relatedField) {
				relation.RelationType = "ManyToOne"
			} else if IsManyToMany(field, relatedField) {
				relation.RelationType = "ManyToMany"
			} else {
				// If none of the conditions match, it's an unsupported type of relation
				relation.RelationType = "Unsupported"
			}
			//fmt.Println(table.ID, table.Name, field.ID, field.Name, field.Type, relation.RelatedTableID, relation.RelatedFieldID)
			// fmt.Println(fmt.Sprintf("%s.%s is related to %s.%s by a %s relationship", table.Name, field.Name, relatedTable.Name, relatedField.Name, relation.RelationType))

			// Add the relation to the list
			relations = append(relations, &relation)
		}
	}

	return relations
}