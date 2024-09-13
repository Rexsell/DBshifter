package connector

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"strings"
)

// PostgresConnector - тип, необходимый для представления подключения базы Postgres к приложению
type PostgresConnector struct {
	conn *pgx.Conn
}

func (connector *PostgresConnector) Connect(ctx context.Context, DBDsn string) error {
	c, err := pgx.Connect(ctx, DBDsn)
	if err != nil {
		return err
	}
	connector.conn = c
	return nil
}

func (connector *PostgresConnector) Close(ctx context.Context) error {
	return connector.conn.Close(ctx)
}

func (connector *PostgresConnector) GetData(ctx context.Context, collectionName string) ([]map[string]any, error) {
	query := fmt.Sprintf("select * from %s", collectionName)

	rows, err := connector.conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	results := make([]map[string]any, 0)

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		rowMap := make(map[string]any)
		for i, field := range fields {
			rowMap[field.Name] = values[i]
		}
		results = append(results, rowMap)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (connector *PostgresConnector) GetDataByLimit(ctx context.Context, collectionName string, limit int64, offset int64) ([]map[string]any, error) {
	query := fmt.Sprintf("select * from %s limit %d offset %d", collectionName, limit, offset)

	rows, err := connector.conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	results := make([]map[string]any, 0)

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		rowMap := make(map[string]any)
		for i, field := range fields {
			rowMap[field.Name] = values[i]
		}
		results = append(results, rowMap)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (connector *PostgresConnector) WriteData(ctx context.Context, collectionName string, data []map[string]any) error {
	transaction, err := connector.conn.Begin(ctx) //тут начало транзакции, чтобы при ошибке программной или аппаратной, не произошло невалидной записи
	if err != nil {
		return err
	}
	defer func() {
		if err := transaction.Rollback(ctx); err != nil {
			return
		} //Rollback если в процессе обработки транзакция не получила коммит
	}()

	for _, row := range data {
		columns := make([]string, 0, len(data))
		values := make([]any, 0, len(data))
		placeholders := make([]string, 0, len(data))

		i := 1
		for col, val := range row {
			columns = append(columns, col)
			values = append(values, val)
			placeholders = append(placeholders, fmt.Sprintf("$%d", i))
			i++
		}

		query := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			collectionName,
			strings.Join(columns, ","),
			strings.Join(placeholders, ","),
		)

		_, err = transaction.Exec(ctx, query, values...) //Выполнение транзакции
		if err != nil {
			return err
		}
	}
	err = transaction.Commit(ctx) //Коммит для возможности версионирования/логирования изменений
	if err != nil {
		return err
	}
	return nil
}
