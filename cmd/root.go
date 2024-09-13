package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"postgre-to-mongo/connector"
	"postgre-to-mongo/model"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "postgre-to-mongo",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
			examples and usage of using your application. For example:

			Cobra is a CLI library for Go that empowers applications.
			This application is a tool to generate the needed files
			to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("Application has started")
		cfg, err := initConfig()
		if err != nil {
			log.Error(err)
			return
		}

		// создаем контексты для работы pgx с БД
		ctxSrc, cancelSrc := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelSrc()

		ctxDst, cancelDst := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelDst()

		//получаем данные из БД-источника

		//data, err := getDataFromSourceLimit(ctxSrc, cfg, 2, 1)
		data, err := getDataFromSource(ctxSrc, cfg)
		if err != nil {
			log.Error(err)
			return
		}
		log.Println("Data has been successfully retrieved.")

		//удаляем указанные поля
		data, err = deleteData(data, cfg)
		if err != nil {
			log.Error(err)
			return
		}
		log.Println("Data has been successfully deleted.")

		//анонимизируем указанные поля
		data, err = anonymiseData(data, cfg)
		if err != nil {
			log.Error(err)
			return
		}
		log.Println("Data has been successfully anonymised.")

		//записываем обработанные данные в целевую БД
		err = writeDataToSrc(ctxDst, cfg, data)
		if err != nil {
			log.Error(err)
			return
		}
		log.Println("Data has been successfully written.")
	},
}

func getDataFromSource(ctx context.Context, cfg *model.Config) ([]map[string]any, error) {
	var sourceConnector connector.Connector

	uSrc, err := url.Parse(cfg.SourceDatabaseDSN)

	if err != nil {
		log.Error(err)
		return nil, err
	}
	//определение типа бд для получения данных
	switch uSrc.Scheme {
	case "postgresql":
		sourceConnector = &connector.PostgresConnector{}
	case "mongodb":
		sourceConnector = &connector.MongoConnector{
			DBName: cfg.SourceDatabaseName,
		}
	default:
		return nil, errors.New(fmt.Sprintf("Unsupported database type: %s", uSrc.Scheme))
	}
	//подключаемся к БД
	err = sourceConnector.Connect(ctx, cfg.SourceDatabaseDSN)

	if err != nil {
		return nil, err
	}

	defer func() {
		err := sourceConnector.Close(ctx)
		if err != nil {
			log.Error(err)
			return
		}
	}()
	//Достаем данные из БД
	data, err := sourceConnector.GetData(ctx, cfg.SourceCollectionName)

	if err != nil {
		return nil, err
	}
	return data, nil
}

func getDataFromSourceLimit(ctx context.Context, cfg *model.Config, limit int64, offset int64) ([]map[string]any, error) {
	var sourceConnector connector.Connector

	uSrc, err := url.Parse(cfg.SourceDatabaseDSN)

	if err != nil {
		log.Error(err)
		return nil, err
	}
	//определение типа бд для получения данных
	switch uSrc.Scheme {
	case "postgresql":
		sourceConnector = &connector.PostgresConnector{}
	case "mongodb":
		sourceConnector = &connector.MongoConnector{
			DBName: cfg.SourceDatabaseName,
		}
	default:
		return nil, errors.New(fmt.Sprintf("Unsupported database type: %s", uSrc.Scheme))
	}
	//подключаемся к БД
	err = sourceConnector.Connect(ctx, cfg.SourceDatabaseDSN)

	if err != nil {
		return nil, err
	}

	defer func() {
		err := sourceConnector.Close(ctx)
		if err != nil {
			log.Error(err)
			return
		}
	}()
	//Достаем данные из БД
	data, err := sourceConnector.GetDataByLimit(ctx, cfg.SourceCollectionName, limit, offset)

	if err != nil {
		return nil, err
	}
	return data, nil
}

func deleteData(data []map[string]any, cfg *model.Config) ([]map[string]any, error) {
	//Если нет полей для удаления, то сразу пропускаем шаг
	if len(cfg.FieldsToDelete) == 0 {
		return data, nil
	}
	for idx, line := range data { // проходим по слайсу мап, получаем каждую запись/строчку к бд
		var foundField = false  //это необходимо для обозначения нашли ли мы указанное поле в бд или нет
		for key := range line { //смотрим на ключ в мапе
			for _, fieldToDelete := range cfg.FieldsToDelete { // проходим по указанным полям в конфиге
				if key == fieldToDelete { // если совпадают ключ мапы и указанное поле в конфиге, то удаляем запись из мапы
					foundField = true
					delete(data[idx], key)
				}
			}
		}
		if !foundField {
			return nil, errors.New("field to delete not found")
		}
	}
	return data, nil
}

func anonymiseData(data []map[string]any, cfg *model.Config) ([]map[string]any, error) {
	if len(cfg.FieldsToAnonymise) == 0 {
		return data, nil
	}
	anonymisedData := make([]map[string]any, len(data))
	for idx, line := range data { // проходим по слайсу мап, получаем каждую запись/строчку к бд
		anonymisedData[idx] = make(map[string]any)
		var foundField = false         //это необходимо для обозначения нашли ли мы указанное поле в бд или нет
		for key, value := range line { //смотрим на ключ и значение в мапе
			for _, fieldToAnonymise := range cfg.FieldsToAnonymise { // проходим по указанным полям в конфиге
				if key == fieldToAnonymise { // если совпадают ключ мапы и указанное поле в конфиге, то значение изменяем на нужное нам
					foundField = true
					switch value.(type) {
					default:
						return nil, errors.New("anonymised value is not supported")
					case string:
						value = "Anonymised"
					case int:
						value = 0
					}
				}
				anonymisedData[idx][key] = value //Записываем в бд
			}
		}
		if !foundField {
			return nil, errors.New("anonymised field not found")
		}
	}
	return anonymisedData, nil
}

func writeDataToSrc(ctx context.Context, cfg *model.Config, data []map[string]any) error {

	var destinationConnector connector.Connector

	uDst, err := url.Parse(cfg.DestinationDatabaseDSN)
	if err != nil {
		return err
	}

	switch uDst.Scheme {
	case "postgresql":
		destinationConnector = &connector.PostgresConnector{}
	case "mongodb":
		destinationConnector = &connector.MongoConnector{
			DBName: cfg.DestinationDatabaseName,
		}
	default:
		return errors.New(fmt.Sprintf("Unsupported database type: %s", uDst.Scheme))
	}

	err = destinationConnector.Connect(ctx, cfg.DestinationDatabaseDSN)
	if err != nil {
		return err
	}

	defer func() {
		if err := destinationConnector.Close(ctx); err != nil {
			log.Error(err)
			return
		}
	}()

	err = destinationConnector.WriteData(ctx, cfg.DestinationCollectionName, data)
	if err != nil {
		return err
	}

	return nil
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	log.SetFormatter(&log.TextFormatter{})
	if err != nil {
		os.Exit(1)
	}

}

func init() {

	rootCmd.Flags().StringVarP(&cfgFile, "config", "c", "", "config file (default is ./config.yaml)")

}

// initConfig reads in config file and ENV variables if set.
func initConfig() (*model.Config, error) {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {

		// Search config in home directory with name ".postgre-to-mongo" (without extension).
		viper.AddConfigPath("./")
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
	}

	viper.AutomaticEnv() // read in environment variables that match

	//Парсим конфиг и пишем его в структурку
	var cfg *model.Config
	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
