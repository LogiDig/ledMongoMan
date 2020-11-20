package mongoman

import (
	"context"
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var (
	log = logrus.New()
)

//MgoMan is the struct for mongo manager
type MgoMan struct {
	mongoDBHost string
}

//New Set initial params
func New(mgoDBhost string) MgoMan {
	r := MgoMan{mongoDBHost: mgoDBhost}
	return r
}

//GetOne Simplifies get data.
func (m MgoMan) GetOne(database string, table string, filter bson.M, opts *options.FindOneOptions) (bson.Raw, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(m.mongoDBHost))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = client.Connect(ctx)

	if err != nil {
		fmt.Println("[0211] ", err)
		return nil, err
	}

	defer client.Disconnect(ctx)
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = client.Ping(ctx, readpref.Primary())

	if err != nil {
		fmt.Println("[0212] ", err)
		return nil, err
	}

	collection := client.Database(database).Collection(table)

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var result bson.Raw
	if opts != nil {
		result, err = collection.FindOne(ctx, filter, opts).DecodeBytes()
	} else {
		result, err = collection.FindOne(ctx, filter).DecodeBytes()
	}
	//result, err := collection.FindOne(ctx, filter, opts).DecodeBytes()

	if err != nil {
		return nil, err
	}

	return result, nil
}

//GetAll Simplifies get massive data.
func (m MgoMan) GetAll(database string, table string, filter bson.M, opts *options.FindOptions) ([]bson.Raw, error) { //([]interface{}, error)
	client, err := mongo.NewClient(options.Client().ApplyURI(m.mongoDBHost))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = client.Connect(ctx)

	if err != nil {
		fmt.Println("[124121] ", err)
		return nil, err
	}

	defer client.Disconnect(ctx)
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = client.Ping(ctx, readpref.Primary())

	if err != nil {
		fmt.Println("[124122] ", err)
		return nil, err
	}

	//findOptions := options.Find()
	//findOptions.SetLimit(limit)

	var results []bson.Raw

	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	collection := client.Database(database).Collection(table)

	cur, err := collection.Find(ctx, filter, opts)
	if err != nil {
		fmt.Println("[12413] ", err)
		return nil, err
	}

	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for cur.Next(ctx) {
		leItem := cur.Current
		results = append(results, leItem)
	}

	if err := cur.Err(); err != nil {
		fmt.Println("[124124] ", err)
		return nil, err
	}

	cur.Close(ctx)

	return results, nil
}

//PushOne Simplifies write data.
func (m MgoMan) PushOne(database string, table string, data interface{}) (interface{}, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(m.mongoDBHost))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = client.Connect(ctx)

	if err != nil {
		return nil, err
	}

	defer client.Disconnect(ctx)
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = client.Ping(ctx, readpref.Primary())

	if err != nil {
		return nil, err
	}

	collection := client.Database(database).Collection(table)

	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	insertResult, err := collection.InsertOne(ctx, data)

	if err != nil {
		return nil, err
	}

	return insertResult.InsertedID, nil
}

//PushAll Simplifies write massive data.
func (m MgoMan) PushAll(database string, table string, filters []interface{}) ([]interface{}, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(m.mongoDBHost))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = client.Connect(ctx)

	if err != nil {
		return nil, err
	}

	defer client.Disconnect(ctx)
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = client.Ping(ctx, readpref.Primary())

	if err != nil {
		return nil, err
	}

	collection := client.Database(database).Collection(table)

	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	insertManyResult, err := collection.InsertMany(ctx, filters)

	if err != nil {
		return nil, err
	}

	return insertManyResult.InsertedIDs, nil
}

//UpdateOne Simplifies update data.
func (m MgoMan) UpdateOne(database string, table string, filter bson.M, update bson.M, opts ...*options.UpdateOptions) (int64, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(m.mongoDBHost))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = client.Connect(ctx)

	if err != nil {
		return 0, err
	}

	defer client.Disconnect(ctx)
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = client.Ping(ctx, readpref.Primary())

	if err != nil {
		return 0, err
	}

	collection := client.Database(database).Collection(table)

	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	updateResult, err := collection.UpdateOne(ctx, filter, update)

	if err != nil {
		return 0, err
	}

	return updateResult.ModifiedCount, nil
}

//Count Count documents.
func (m MgoMan) Count(database string, table string, filter bson.M, opts *options.CountOptions) (int64, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(m.mongoDBHost))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = client.Connect(ctx)

	if err != nil {
		return 0, err
	}

	defer client.Disconnect(ctx)
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = client.Ping(ctx, readpref.Primary())

	if err != nil {
		return 0, err
	}

	collection := client.Database(database).Collection(table)

	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := collection.CountDocuments(ctx, filter, opts)
	if err != nil {
		return 0, err
	}

	return result, nil
}
