package mongoman

import (
	"context"
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

type mgoman struct {
	mongoDBHost string
}

func (m mgoman) getOne(database string, table string, filter bson.M) (bson.Raw, error) {
	//Debug:
	//fmt.Println("type: single")
	//fmt.Println("database: ", database)
	//fmt.Println("table: ", table)
	//fmt.Println("filter: ", filter)
	////////////////////////////////////

	client, err := mongo.NewClient(options.Client().ApplyURI(m.mongoDBHost))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = client.Connect(ctx)

	if err != nil {
		log.Errorln("[02115611717] ", err)
		return nil, err
	}

	//defer client.Disconnect(ctx)
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = client.Ping(ctx, readpref.Primary())

	if err != nil {
		log.Errorln("[0212414] ", err)
		return nil, err
	}

	collection := client.Database(database).Collection(table)

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := collection.FindOne(ctx, filter).DecodeBytes()

	if err != nil {
		//fmt.Println("[0213] ", err)

		return nil, err
	}

	client.Disconnect(ctx)

	return result, nil
}
