package mongoman

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

type dataA struct {
	ID       string
	Name     string
	Summary  string
	Disabled bool
}

func TestWriteOne(t *testing.T) {
	mongoDBHost := "mongodb://localhost:27017"
	mgoman := New(mongoDBHost)
	db := "ledDB"
	tbl := "products"
	d := dataA{
		ID:       "12154125",
		Name:     "Lemon",
		Summary:  "A fruit",
		Disabled: false,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := mgoman.PushOne(
		ctx,
		db,
		tbl,
		d,
		nil,
	)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestWriteMulti(t *testing.T) {
	mongoDBHost := "mongodb://localhost:27017"
	mgoman := New(mongoDBHost)
	db := "ledDB"
	tbl := "products"

	dd := []dataA{
		{
			ID:       "12154130",
			Name:     "Orange",
			Summary:  "An orange fruit",
			Disabled: false,
		}, {
			ID:       "12154131",
			Name:     "Banana",
			Summary:  "A yellow fruit",
			Disabled: false,
		}, {
			ID:       "12154132",
			Name:     "Watermelon",
			Summary:  "A green fruit",
			Disabled: false,
		}, {
			ID:       "12154133",
			Name:     "Apple",
			Summary:  "A red fruit",
			Disabled: false,
		}}

	d2s := []interface{}{}

	for _, v := range dd {
		d2s = append(d2s, v)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := mgoman.PushMany(
		ctx,
		db,
		tbl,
		d2s,
		nil,
	)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestUpdateOne(t *testing.T) {
	mongoDBHost := "mongodb://localhost:27017"
	mgoman := New(mongoDBHost)
	db := "ledDB"
	tbl := "products"
	filter := bson.M{"Name": "Lemon"}
	update := bson.M{"$set": bson.M{
		"Summary": "A fruit.",
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := mgoman.UpdateOne(
		ctx,
		db,
		tbl,
		filter,
		update,
	)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestUpdateMulti(t *testing.T) {
	mongoDBHost := "mongodb://localhost:27017"
	mgoman := New(mongoDBHost)
	db := "ledDB"
	tbl := "products"
	filter := bson.M{"Disabled": false}
	update := bson.M{"$set": bson.M{
		"Disabled": true,
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := mgoman.UpdateOne(
		ctx,
		db,
		tbl,
		filter,
		update,
	)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestDeleteOne(t *testing.T) {
	mongoDBHost := "mongodb://localhost:27017"
	mgoman := New(mongoDBHost)
	db := "ledDB"
	tbl := "products"
	filter := bson.M{"Name": "Lemon"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := mgoman.DeleteOne(
		ctx,
		db,
		tbl,
		filter,
	)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestDeleteMulti(t *testing.T) {
	mongoDBHost := "mongodb://localhost:27017"
	mgoman := New(mongoDBHost)
	db := "ledDB"
	tbl := "products"
	filter := bson.M{"Disabled": false}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := mgoman.DeleteMany(
		ctx,
		db,
		tbl,
		filter,
	)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestReadOne(t *testing.T) {
	mongoDBHost := "mongodb://localhost:27017"
	mgoman := New(mongoDBHost)
	db := "ledDB"
	tbl := "products"
	fil := bson.M{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := mgoman.GetOne(
		ctx,
		db,
		tbl,
		fil,
		nil,
	)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestReadMulti(t *testing.T) {
	mongoDBHost := "mongodb://localhost:27017"
	mgoman := New(mongoDBHost)
	db := "ledDB"
	tbl := "products"
	fil := bson.M{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := mgoman.GetMany(
		ctx,
		db,
		tbl,
		fil,
		nil,
	)
	if err != nil {
		t.Errorf(err.Error())
	}
}
