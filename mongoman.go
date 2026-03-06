package mongoman

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MgoMan is the struct for mongo manager.
type MgoMan struct {
	client       *mongo.Client
	db           *mongo.Database
	queryTimeout time.Duration
}

// New Set initial params.
func New(uri, dbName string, timeout time.Duration) (*MgoMan, error) {
	clientOpts := options.Client().
		ApplyURI(uri).
		SetMaxPoolSize(50).
		SetMinPoolSize(5).
		SetRetryWrites(true)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	return &MgoMan{
		client:       client,
		db:           client.Database(dbName),
		queryTimeout: timeout,
	}, nil
}

func (m *MgoMan) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, m.queryTimeout)
}

func (m *MgoMan) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}

func (m *MgoMan) Health(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return m.client.Ping(ctx, nil)
}

// GetOne Simplifies get one document.
func (m *MgoMan) GetOne(ctx context.Context, table string, filter any, opts ...*options.FindOneOptions) (bson.Raw, error) {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()
	collection := m.db.Collection(table)

	var result bson.Raw
	result, err := collection.FindOne(ctx, filter, opts...).Raw()
	if err != nil {
		return nil, err
	}

	return result, nil
}

// GetMany Simplifies get multiple documents.
func (m *MgoMan) GetMany(ctx context.Context, table string, filter any, opts ...*options.FindOptions) ([]bson.Raw, error) {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()

	var results []bson.Raw
	collection := m.db.Collection(table)

	cur, err := collection.Find(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}

	defer cur.Close(ctx)

	for cur.Next(ctx) {
		leItem := cur.Current
		results = append(results, leItem)
	}

	if err := cur.Err(); err != nil {
		return nil, err
	}

	//results = reverse(results)

	return results, nil
}

// PushOne Simplifies write one document.
func (m *MgoMan) PushOne(ctx context.Context, table string, data any, opts ...*options.InsertOneOptions) (any, error) {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()

	collection := m.db.Collection(table)
	insertResult, err := collection.InsertOne(ctx, data, opts...)

	if err != nil {
		return nil, err
	}

	return insertResult.InsertedID, nil
}

// PushMany Simplifies write multiple document.
func (m *MgoMan) PushMany(ctx context.Context, table string, data []any, opts ...*options.InsertManyOptions) ([]any, error) {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()

	//defer m.disconn(client, ctx)
	collection := m.db.Collection(table)
	insertManyResult, err := collection.InsertMany(ctx, data, opts...)

	if err != nil {
		return nil, err
	}

	return insertManyResult.InsertedIDs, nil
}

// UpdateOne Simplifies update one document.
func (m *MgoMan) UpdateOne(ctx context.Context, table string, filter bson.M, update bson.M, opts ...*options.UpdateOptions) (int64, error) {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()

	collection := m.db.Collection(table)
	updateResult, err := collection.UpdateOne(ctx, filter, update, opts...)

	if err != nil {
		return 0, err
	}

	return updateResult.ModifiedCount, nil
}

// UpdateMany Simplifies update multiple documents.
func (m *MgoMan) UpdateMany(ctx context.Context, table string, filter any, update any, opts ...*options.UpdateOptions) (int64, error) {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()

	collection := m.db.Collection(table)
	updateResult, err := collection.UpdateMany(ctx, filter, update, opts...)

	if err != nil {
		return 0, err
	}

	return updateResult.ModifiedCount, nil
}

// DeleteOne delete one document.
func (m *MgoMan) DeleteOne(ctx context.Context, table string, filter bson.M, opts ...*options.DeleteOptions) (int64, error) {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()

	collection := m.db.Collection(table)
	result, err := collection.DeleteOne(ctx, filter, opts...)
	if err != nil {
		return 0, err
	}

	return result.DeletedCount, nil
}

// DeleteMany delete multiple documents.
func (m *MgoMan) DeleteMany(ctx context.Context, table string, filter any, opts ...*options.DeleteOptions) (int64, error) {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()

	collection := m.db.Collection(table)
	result, err := collection.DeleteMany(ctx, filter, opts...)
	if err != nil {
		return 0, err
	}

	return result.DeletedCount, nil
}

// Count shows total mumber of documents.
func (m *MgoMan) Count(ctx context.Context, table string, filter bson.M, opts ...*options.CountOptions) (int64, error) {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()

	collection := m.db.Collection(table)
	result, err := collection.CountDocuments(ctx, filter, opts...)

	if err != nil {
		return 0, err
	}

	return result, nil
}

// Tools:
/*
func reverse(input []bson.Raw) []bson.Raw {
	newArr := make([]bson.Raw, 0, len(input))
	for i := len(input) - 1; i >= 0; i-- {
		newArr = append(newArr, input[i])
	}
	return newArr
}
*/
