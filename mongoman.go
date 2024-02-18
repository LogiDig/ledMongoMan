package mongoman

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// MgoMan is the struct for mongo manager
type MgoMan struct {
	mongoDBHost string
}

// New Set initial params
func New(mgoDBhost string) MgoMan {
	r := MgoMan{mongoDBHost: mgoDBhost}
	return r
}

//Make a connection

func (m MgoMan) conn(ctx context.Context, cancel context.CancelFunc) (*mongo.Client, error) {
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

//Disconnect

func (m MgoMan) disconn(client *mongo.Client, ctx context.Context) {
	if err := client.Disconnect(ctx); err != nil {
		return
	}
}

// GetOne Simplifies get one record.
func (m MgoMan) GetOne(database, table string, filter bson.M, opts *options.FindOneOptions) (bson.Raw, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := m.conn(ctx, cancel)
	if err != nil {
		return nil, err
	}

	defer m.disconn(client, ctx)
	collection := client.Database(database).Collection(table)

	var result bson.Raw
	if opts != nil {
		result, err = collection.FindOne(ctx, filter, opts).Raw()
		if err != nil {
			return nil, err
		}
	} else {
		result, err = collection.FindOne(ctx, filter).Raw()
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// GetAll Simplifies get multiple data.
func (m MgoMan) GetAll(database, table string, filter bson.M, opts *options.FindOptions) ([]bson.Raw, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := m.conn(ctx, cancel)
	if err != nil {
		return nil, err
	}

	defer m.disconn(client, ctx)

	var results []bson.Raw
	collection := client.Database(database).Collection(table)

	cur, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}

	for cur.Next(ctx) {
		leItem := cur.Current
		results = append(results, leItem)
	}

	if err := cur.Err(); err != nil {
		return nil, err
	}

	cur.Close(ctx)

	return results, nil
}

// PushOne Simplifies write one element.
func (m MgoMan) PushOne(database, table string, data interface{}) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := m.conn(ctx, cancel)
	if err != nil {
		return nil, err
	}

	defer m.disconn(client, ctx)
	collection := client.Database(database).Collection(table)
	insertResult, err := collection.InsertOne(ctx, data)

	if err != nil {
		return nil, err
	}

	return insertResult.InsertedID, nil
}

// PushAll Simplifies write multiple data.
func (m MgoMan) PushAll(database, table string, filters []interface{}) ([]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := m.conn(ctx, cancel)
	if err != nil {
		return nil, err
	}

	defer m.disconn(client, ctx)
	collection := client.Database(database).Collection(table)
	insertManyResult, err := collection.InsertMany(ctx, filters)

	if err != nil {
		return nil, err
	}

	return insertManyResult.InsertedIDs, nil
}

// UpdateOne Simplifies update one element.
func (m MgoMan) UpdateOne(database, table string, filter bson.M, update bson.M, opts ...*options.UpdateOptions) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := m.conn(ctx, cancel)
	if err != nil {
		return 0, err
	}

	defer m.disconn(client, ctx)
	collection := client.Database(database).Collection(table)
	updateResult, err := collection.UpdateOne(ctx, filter, update)

	if err != nil {
		return 0, err
	}

	return updateResult.ModifiedCount, nil
}

// DeleteOne delete one document.
func (m MgoMan) DeleteOne(database, table string, filter bson.M, opts *options.DeleteOptions) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := m.conn(ctx, cancel)
	if err != nil {
		return 0, err
	}

	defer m.disconn(client, ctx)
	collection := client.Database(database).Collection(table)
	result, err := collection.DeleteOne(ctx, filter, opts)
	if err != nil {
		return 0, err
	}

	return result.DeletedCount, nil
}

// DeleteAll delete multiple documents.
func (m MgoMan) DeleteAll(database, table string, filter bson.M, opts *options.DeleteOptions) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := m.conn(ctx, cancel)
	if err != nil {
		return 0, err
	}

	defer m.disconn(client, ctx)
	collection := client.Database(database).Collection(table)
	result, err := collection.DeleteMany(ctx, filter, opts)
	if err != nil {
		return 0, err
	}

	return result.DeletedCount, nil
}

// Count shows total mumber of documents.
func (m MgoMan) Count(database, table string, filter bson.M, opts *options.CountOptions) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := m.conn(ctx, cancel)
	if err != nil {
		return 0, err
	}

	defer m.disconn(client, ctx)
	collection := client.Database(database).Collection(table)
	result, err := collection.CountDocuments(ctx, filter, opts)

	if err != nil {
		return 0, err
	}

	return result, nil
}
