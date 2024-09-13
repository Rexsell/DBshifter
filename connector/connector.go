package connector

import "context"

// Connector - интерфейс необходим для обеспечения гибкости взаимодействия основной программы с другими БД
// Как видно, достаточно реализовать несколько методов для возможности работы с любой производной БД
type Connector interface {
	Connect(ctx context.Context, DBDsn string) error
	Close(ctx context.Context) error
	GetData(ctx context.Context, collectionName string) ([]map[string]any, error)
	GetDataByLimit(ctx context.Context, collectionName string, limit int64, offset int64) ([]map[string]any, error)
	WriteData(ctx context.Context, collectionName string, data []map[string]any) error
}
