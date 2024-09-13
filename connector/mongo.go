package connector

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoConnector - тип, необходимый для представления подключения базы MongoDB к приложению
type MongoConnector struct {
	client *mongo.Client
	DBName string //эта строчка необходима чтобы меньше параметров передавать в функции. В будущем сделаю вложенную структуру Database со всеми необходимыми полями
}

func (connector *MongoConnector) Connect(ctx context.Context, DBDsn string) error {
	clientOptions := options.Client().ApplyURI(DBDsn)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return err
	}

	connector.client = client

	err = connector.client.Ping(ctx, nil)
	if err != nil {
		return err
	}

	return nil
}

func (connector *MongoConnector) Close(ctx context.Context) error {
	return connector.client.Disconnect(ctx)
}

func (connector *MongoConnector) GetData(ctx context.Context, collectionName string) ([]map[string]any, error) {
	collection := connector.client.Database(connector.DBName).Collection(collectionName)

	cursor, err := collection.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := cursor.Close(ctx); err != nil {
			return
		}
	}()
	var results []map[string]any

	for cursor.Next(ctx) {
		var result map[string]any
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		delete(result, "_id")
		results = append(results, result)
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (connector *MongoConnector) GetDataByLimit(ctx context.Context, collectionName string, limit int64, offset int64) ([]map[string]any, error) {
	collection := connector.client.Database(connector.DBName).Collection(collectionName)
	//Тут я не очень уверен, стоит ли делать int64 как тип аргумента функции
	//или в аргумент давать int а тут конвертировать в int64, так как драйвер бд кушает int64
	opts := options.Find().SetLimit(limit).SetSkip(offset)

	cursor, err := collection.Find(ctx, bson.D{}, opts)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := cursor.Close(ctx); err != nil {
			return
		}
	}()

	var results []map[string]any

	for cursor.Next(ctx) {
		var result map[string]any
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		delete(result, "_id")
		results = append(results, result)
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (connector *MongoConnector) WriteData(ctx context.Context, collectionName string, data []map[string]any) error {
	collection := connector.client.Database(connector.DBName).Collection(collectionName)

	var docs []any
	for _, doc := range data {
		docs = append(docs, doc)
	}
	_, err := collection.InsertMany(ctx, docs)
	return err
}
