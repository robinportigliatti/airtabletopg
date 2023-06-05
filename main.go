package main

import (
	"github.com/mehanizm/airtable"
	a "airtabletopg/airtable"
	"airtabletopg/postgres"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"os"
//	"sort"
	"strconv"
	"strings"
)

func exportTableToCSV(client *airtable.Client, b *airtable.Base, s *airtable.Tables, r []*a.RelationshipInfo, tableName string) error {
//	fmt.Printf("Processing table: %s\n", tableName)
	// Get schema and current table
	currentTable, _ := a.FindTableByName(s.Tables, tableName)
	table := client.GetTable(b.ID, tableName)
	records,_ := table.GetRecords().Do()


	fileName := fmt.Sprintf("%s.csv", tableName)
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	writer.Comma = ';' // définir le délimiteur si besoin, par défaut c'est une virgule
	writer.UseCRLF = true

	defer writer.Flush()

	for _, record := range records.Records {
		row, err := processRecord(record, currentTable, s, r, client, b)
		if err != nil {
			return err
		}

		err = writer.Write(row)
		if err != nil {
			return err
		}
	}

	writer.Flush()

	if len(records.Records) != 0 {
		var keys []string
		for _, f := range currentTable.Fields {
			var appendBool = true
			currentRelationIfFound, _ := a.FindRelationByTableIDAndFieldID(r, currentTable.ID, f.ID)
			if currentRelationIfFound != nil {
				if currentRelationIfFound.RelationType == "OneToMany" || currentRelationIfFound.RelationType == "ManyToMany" {
					appendBool = false
				} else if currentRelationIfFound.RelationType == "ManyToOne" {
					appendBool = true
				}
			}
			if appendBool {
				keys = append(keys, f.Name)
			}
		}
		
		var sql []string
		sql = append(sql, fmt.Sprintf("\\COPY %s (" + strings.Join(postgres.NameFormatJoin(keys), ", ") + ") FROM '%s' DELIMITER '%s' CSV;", postgres.NameFormat(tableName), fileName, string(writer.Comma) ))
		writeCOPYSQL(sql, "data.sql")
		err = removeLastLine(fileName)
	}
	return nil
}

func removeLastLine(fileName string) error {
	fmt.Println(fileName)
	// Ouvrez le fichier en mode lecture et écriture.
	file, err := os.OpenFile(fileName, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()

	// Obtenez la taille actuelle du fichier.
	stat, err := file.Stat()
	if err != nil {
		return err
	}

	// Supprimez le dernier caractère (saut de ligne).
	err = file.Truncate(stat.Size() - 2)
	if err != nil {
		return err
	}

	// Assurez-vous que toutes les modifications sont écrites sur le disque.
	err = file.Sync()
	if err != nil {
		return err
	}

	return nil
}


func processRecord(record *airtable.Record, currentTable *airtable.TableSchema, s *airtable.Tables, r []*a.RelationshipInfo, client *airtable.Client, b *airtable.Base) ([]string, error) {
	var row []string
	
	for _, f := range currentTable.Fields {
		var appendBool = true
		var valueToStore = record.Fields[f.Name]
		if valueToStore == nil {
			valueToStore = ""
		}
		
		var err = errors.New("")
		if f.Type == "multipleRecordLinks" {
			appendBool, valueToStore, err = processField(client, b, r, currentTable, f, valueToStore)
			if err != nil {
				return nil, err
			}
		}
		
		if appendBool {
			row = append(row, fmt.Sprintf("%v", valueToStore))
		}
	}
	return row, nil

}

func processField(client *airtable.Client, b *airtable.Base, r []*a.RelationshipInfo, currentTable *airtable.TableSchema, currentField *airtable.Field, fieldValue interface{}) (bool, interface{}, error) {
	appendBool := true
	valueToStore := fieldValue
	currentRelationIfFound, _ := a.FindRelationByTableIDAndFieldID(r, currentTable.ID, currentField.ID)
	if currentRelationIfFound != nil {
		if currentRelationIfFound.RelationType == "OneToMany" || currentRelationIfFound.RelationType == "ManyToMany" {
			appendBool = false
		} else if currentRelationIfFound.RelationType == "ManyToOne" {
			fieldValueSlice, ok := fieldValue.([]interface{})
			if !ok {
				return false, nil, fmt.Errorf("fieldValue n'est pas un slice")
			}
			id, ok := fieldValueSlice[0].(string)
			if !ok {
				return false, nil, fmt.Errorf("cannot convert fieldValue to string")
			}

			var relatedTable = client.GetTable(b.ID, currentRelationIfFound.RelatedTable.Name)
			currentRecord, err := relatedTable.GetRecord(id)
			if err != nil {
				return false, nil, err
			}
			
			primaryField, _ := a.FindFieldByID(&currentRelationIfFound.RelatedTable, currentRelationIfFound.RelatedTable.PrimaryFieldID)

			valueToStore = currentRecord.Fields[primaryField.Name]
		}
	}

	return appendBool, valueToStore, nil
}


func writeSQL(lines []string, fileName string) error {
	// Créer le fichier
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	// Parcourir chaque ligne et l'écrire dans le fichier
	for _, str := range lines {
		_, err := file.WriteString(str + "\n")
		if err != nil {
			return err
		}
	}

	return nil
}

func writeCOPYSQL(lines []string, fileName string) error {
    // Ouvre le fichier en mode append, crée le fichier s'il n'existe pas, 
    // et ouvre le fichier en mode écriture seulement
    file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return err
    }
    defer file.Close()

    // Parcourir chaque ligne et l'écrire dans le fichier
    for _, str := range lines {
        _, err := file.WriteString(str + "\n")
        if err != nil {
            return err
        }
    }

    return nil
}

func deleteFile(filename string) {
	err := os.Remove(filename)
	if err != nil {
		// Si une erreur se produit lors de la suppression du fichier
		fmt.Println("Erreur lors de la suppression du fichier", err)
	}
}

func HandlingManyToManyRelationShipsToCSV(client *airtable.Client, b *airtable.Base, r []*a.RelationshipInfo) error {
	//	fmt.Printf("Processing table: %s\n", tableName)
		// Get schema and current table
	// schema, err := client.GetBaseSchema(b.ID).Do()
	alreadyProcessed := []string{}
	for _, relation := range r {
		if relation.RelationType == "OneToMany" || relation.RelationType == "ManyToOne" {
			continue
		} else if relation.RelationType == "ManyToMany" {
			if !postgres.Contains(alreadyProcessed, postgres.NameFormat(relation.Table.Name)) && !postgres.Contains(alreadyProcessed, postgres.NameFormat(relation.RelatedTable.Name)) {
				// On récupère tous les records de relation.Table
				var table = client.GetTable(b.ID, relation.Table.Name)
				var relatedTable = client.GetTable(b.ID, relation.RelatedTable.Name)
				tablePrimaryField, _ := a.FindFieldByID(&relation.Table, relation.Table.PrimaryFieldID)
				relatedPrimaryField, _ := a.FindFieldByID(&relation.RelatedTable, relation.RelatedTable.PrimaryFieldID)

				// On dit que les deux tables ont été faites
				alreadyProcessed = append(alreadyProcessed, postgres.NameFormat(relation.Table.Name))
				alreadyProcessed = append(alreadyProcessed, postgres.NameFormat(relation.RelatedTable.Name))
				// On crée le fichier
				fileName := fmt.Sprintf("%s_%s.csv", relation.Table.Name, relation.RelatedTable.Name)

				var sql []string
				file, err := os.Create(fileName)
				if err != nil {
					return err
				}
				defer file.Close()

				writer := csv.NewWriter(file)
				writer.Comma = ';' // définir le délimiteur si besoin, par défaut c'est une virgule
				writer.UseCRLF = true
			
				defer writer.Flush()
				sql = append(
					sql, fmt.Sprintf(
						"\\COPY %s (%s, %s) FROM '%s' DELIMITER '%s' CSV;",
						postgres.NameFormat(relation.Table.Name + "_" + relation.RelatedTable.Name),
						postgres.NameFormat(tablePrimaryField.Name + "_" + relation.Table.Name),
						postgres.NameFormat(relatedPrimaryField.Name + "_" + relation.RelatedTable.Name),
						string(writer.Comma),
						fileName))
				writeCOPYSQL(sql, "data.sql")

				// On récupère les records de relation.Table
				var records,_ = table.GetRecords().Do()
				// fmt.Println(fmt.Sprintf("%s,%s", relation.Field.Name, relation.RelatedField.Name))
				for _, record := range records.Records {
					// On récupère la valeur du champs
					var fieldValue = record.Fields[relation.Field.Name]
					// Vu qu'on est dans une relation ManyToMany je vais avoir [id1, id2, id3]
					// Faut donc faire un slice
					fieldValueSlice, _ := fieldValue.([]interface{})
					if len(fieldValueSlice) != 0 {
						// On récupère un par un les records de la table relation.RelatedTable.Name correspond à
						// une recherche avec fieldValueSlice
						
						for _, id := range fieldValueSlice {
							var row []string
							var valueToStore string
							currentRecord, _ := relatedTable.GetRecord(id.(string))
							switch v := record.Fields[tablePrimaryField.Name].(type) {
							case float64:
								valueToStore = strconv.FormatFloat(v, 'f', -1, 64)
							default:
								valueToStore = v.(string)
							}
							row = append(row, fmt.Sprintf("%v", valueToStore))

							switch v := currentRecord.Fields[relatedPrimaryField.Name].(type) {
							case float64:
								valueToStore = strconv.FormatFloat(v, 'f', -1, 64)
							default:
								valueToStore = v.(string)
							}
							row = append(row, fmt.Sprintf("%v", valueToStore))
							//fmt.Println(row)
							err = writer.Write(row)
							if err != nil {
								return err
							}
						}
					}
				}
				_ = removeLastLine(fileName)
			}				

		}
	}

	return nil
}	

func main() {
	apiKey := flag.String("api-key", "", "API key for Airtable")
	dbName := flag.String("dbname", "", "Database name")
	preData := flag.Bool("pre-data", false, "Export pre-data, true by default")
	data := flag.Bool("data", false, "Export data, true by default")
	postData := flag.Bool("post-data", false, "Export post-data, true by default")

	flag.Parse()

	if *apiKey == "" || *dbName == "" {
		fmt.Println("Both --api-key and --dbname must be provided")
		return
	}
	client := airtable.NewClient(*apiKey)

	bases, err := client.GetBases().Do()

	if err != nil {
		fmt.Println("Error while fetching bases: ", err)
		return
	}

	for _, base := range bases.Bases {
		if *dbName == base.Name {
			postgresBase := postgres.Base{Name: base.Name}

			schema, err := client.GetBaseSchema(base.ID).Do()
			if err != nil {
				fmt.Println("Error while fetching tables: ", err)
				continue
			}

			for _, table := range schema.Tables {
				//fmt.Println("")
				//fmt.Println(table)
				postgresTable := postgres.Table{ID: table.ID, Name: table.Name, PrimaryFieldId: table.PrimaryFieldID}
				for _, field := range table.Fields {
					//fmt.Println(field)
					column := postgres.Column{ID: field.ID, Name: field.Name, Type: field.Type}
					postgresTable.Columns = append(postgresTable.Columns, column)
				}

				postgresBase.Tables = append(postgresBase.Tables, postgresTable)
			}
			postgresBase.RelationshipInfos = a.AnalyzeRelationships(schema.Tables)
			if !*preData && !*data && !*postData {
				*preData = true
				*data = true
				*postData = true
			}
			if *preData {
				lines := []string{}
				lines = append(lines, postgresBase.GetCreateDatabaseStatement())
				for _, line := range postgresBase.PreDataSQL() {
					lines = append(lines, line)
				}
				
				writeSQL(lines, "pre-data.sql")
			}
			if *data {
				deleteFile("data.sql")
				for _, table := range postgresBase.Tables {
					err := exportTableToCSV(client, base, schema, postgresBase.RelationshipInfos, table.Name)
					if err != nil {
						fmt.Printf("Error processing table %s: %v\n", table, err)
					}
				}
				// Handling ManyToMany RelationShips
				HandlingManyToManyRelationShipsToCSV(client, base, postgresBase.RelationshipInfos)
			}
			if *postData {
				writeSQL(postgresBase.PostDataSQL(), "post-data.sql")
			}
		}
	}
}
