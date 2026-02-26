package mongoman

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// MgoMan is the struct for mongo manager.
type MgoMan struct {
	mongoDBHost string
}

// New Set initial params.
func New(mgoDBhost string) *MgoMan {
	r := &MgoMan{mongoDBHost: mgoDBhost}
	return r
}

//Make a connection.

func (m *MgoMan) conn(ctx context.Context) (*mongo.Client, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(m.mongoDBHost))
	if err != nil {
		return nil, err
	}
	err = client.Ping(ctx, readpref.Primary())

	if err != nil {
		return nil, err
	}
	return client, nil
}

//Disconnect.

func (m *MgoMan) disconn(client *mongo.Client, ctx context.Context) {
	if err := client.Disconnect(ctx); err != nil {
		return
	}
}

// GetOne Simplifies get one document.
func (m *MgoMan) GetOne(ctx context.Context, database, table string, filter any, opts ...*options.FindOneOptions) (bson.Raw, error) {
	client, err := m.conn(ctx)
	if err != nil {
		return nil, err
	}

	defer m.disconn(client, ctx)
	collection := client.Database(database).Collection(table)

	var result bson.Raw
	result, err = collection.FindOne(ctx, filter, opts...).Raw()
	if err != nil {
		return nil, err
	}

	return result, nil
}

// GetMany Simplifies get multiple documents.
func (m *MgoMan) GetMany(ctx context.Context, database, table string, filter any, opts ...*options.FindOptions) ([]bson.Raw, error) {
	client, err := m.conn(ctx)
	if err != nil {
		return nil, err
	}

	defer m.disconn(client, ctx)

	var results []bson.Raw
	collection := client.Database(database).Collection(table)

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
func (m *MgoMan) PushOne(ctx context.Context, database, table string, data any, opts ...*options.InsertOneOptions) (any, error) {
	client, err := m.conn(ctx)
	if err != nil {
		return nil, err
	}

	defer m.disconn(client, ctx)
	collection := client.Database(database).Collection(table)
	insertResult, err := collection.InsertOne(ctx, data, opts...)

	if err != nil {
		return nil, err
	}

	return insertResult.InsertedID, nil
}

// PushMany Simplifies write multiple document.
func (m *MgoMan) PushMany(ctx context.Context, database, table string, data []any, opts ...*options.InsertManyOptions) ([]any, error) {
	client, err := m.conn(ctx)
	if err != nil {
		return nil, err
	}

	defer m.disconn(client, ctx)
	collection := client.Database(database).Collection(table)
	insertManyResult, err := collection.InsertMany(ctx, data, opts...)

	if err != nil {
		return nil, err
	}

	return insertManyResult.InsertedIDs, nil
}

// UpdateOne Simplifies update one document.
func (m *MgoMan) UpdateOne(ctx context.Context, database, table string, filter bson.M, update bson.M, opts ...*options.UpdateOptions) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := m.conn(ctx)
	if err != nil {
		return 0, err
	}

	defer m.disconn(client, ctx)
	collection := client.Database(database).Collection(table)
	updateResult, err := collection.UpdateOne(ctx, filter, update, opts...)

	if err != nil {
		return 0, err
	}

	return updateResult.ModifiedCount, nil
}

// UpdateMany Simplifies update multiple documents.
func (m *MgoMan) UpdateMany(ctx context.Context, database, table string, filter any, update any, opts ...*options.UpdateOptions) (int64, error) {
	client, err := m.conn(ctx)
	if err != nil {
		return 0, err
	}

	defer m.disconn(client, ctx)
	collection := client.Database(database).Collection(table)
	updateResult, err := collection.UpdateMany(ctx, filter, update, opts...)

	if err != nil {
		return 0, err
	}

	return updateResult.ModifiedCount, nil
}

// DeleteOne delete one document.
func (m *MgoMan) DeleteOne(ctx context.Context, database, table string, filter bson.M, opts ...*options.DeleteOptions) (int64, error) {
	client, err := m.conn(ctx)
	if err != nil {
		return 0, err
	}

	defer m.disconn(client, ctx)
	collection := client.Database(database).Collection(table)
	result, err := collection.DeleteOne(ctx, filter, opts...)
	if err != nil {
		return 0, err
	}

	return result.DeletedCount, nil
}

// DeleteMany delete multiple documents.
func (m *MgoMan) DeleteMany(ctx context.Context, database, table string, filter any, opts ...*options.DeleteOptions) (int64, error) {
	client, err := m.conn(ctx)
	if err != nil {
		return 0, err
	}

	defer m.disconn(client, ctx)
	collection := client.Database(database).Collection(table)
	result, err := collection.DeleteMany(ctx, filter, opts...)
	if err != nil {
		return 0, err
	}

	return result.DeletedCount, nil
}

// Count shows total mumber of documents.
func (m *MgoMan) Count(ctx context.Context, database, table string, filter bson.M, opts ...*options.CountOptions) (int64, error) {
	client, err := m.conn(ctx)
	if err != nil {
		return 0, err
	}

	defer m.disconn(client, ctx)
	collection := client.Database(database).Collection(table)
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
