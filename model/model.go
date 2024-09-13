package model

// Config
// Собственно, тут все +- понятно
// -DSN строки для подключения БД
// -CollectionName и -DatabaseName нужны для указания файлов таблиц, так как разные БД имеют несколько разнаю логику подключения
// И слайс полей для анонимизации и удаления из целевой таблицы
type Config struct {
	SourceDatabaseDSN         string   `yaml:"source_database_dsn" mapstructure:"source_database_dsn"`
	SourceCollectionName      string   `yaml:"source_collection_name" mapstructure:"source_collection_name"`
	SourceDatabaseName        string   `yaml:"source_database_name" mapstructure:"source_database_name"`
	DestinationDatabaseDSN    string   `yaml:"destination_database_dsn" mapstructure:"destination_database_dsn"`
	DestinationCollectionName string   `yaml:"destination_collection_name" mapstructure:"destination_collection_name"`
	DestinationDatabaseName   string   `yaml:"destination_database_name" mapstructure:"destination_database_name"`
	FieldsToAnonymise         []string `yaml:"fields_to_anonymise" mapstructure:"fields_to_anonymise"`
	FieldsToDelete            []string `yaml:"fields_to_delete" mapstructure:"fields_to_delete"`
}
