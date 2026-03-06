package mongoman

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

type dataA struct {
	ID       string
	Name     string
	Summary  string
	Disabled bool
}

func TestWriteOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uri := "mongodb://localhost:27017"
	db := "ledDB"
	mgoman, err := New(uri, db, time.Second*3)
	if err != nil {
		t.Errorf(err.Error())
	}

	tbl := "products"
	d := dataA{
		ID:       "12154125",
		Name:     "Lemon",
		Summary:  "A fruit",
		Disabled: false,
	}

	defer mgoman.Close(ctx)
	_, err = mgoman.PushOne(
		ctx,
		tbl,
		d,
		nil,
	)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestWriteMulti(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uri := "mongodb://localhost:27017"
	db := "ledDB"
	mgoman, err := New(uri, db, time.Second*3)
	if err != nil {
		t.Errorf(err.Error())
	}

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

	defer mgoman.Close(ctx)
	_, err = mgoman.PushMany(
		ctx,
		tbl,
		d2s,
		nil,
	)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestUpdateOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uri := "mongodb://localhost:27017"
	db := "ledDB"
	mgoman, err := New(uri, db, time.Second*3)
	if err != nil {
		t.Errorf(err.Error())
	}

	tbl := "products"
	filter := bson.M{"Name": "Lemon"}
	update := bson.M{"$set": bson.M{
		"Summary": "A fruit.",
	}}

	defer mgoman.Close(ctx)
	_, err = mgoman.UpdateOne(
		ctx,
		tbl,
		filter,
		update,
	)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestUpdateMulti(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uri := "mongodb://localhost:27017"
	db := "ledDB"
	mgoman, err := New(uri, db, time.Second*3)
	if err != nil {
		t.Errorf(err.Error())
	}

	tbl := "products"
	filter := bson.M{"Disabled": false}
	update := bson.M{"$set": bson.M{
		"Disabled": true,
	}}

	defer mgoman.Close(ctx)
	_, err = mgoman.UpdateOne(
		ctx,
		tbl,
		filter,
		update,
	)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestDeleteOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uri := "mongodb://localhost:27017"
	db := "ledDB"
	mgoman, err := New(uri, db, time.Second*3)
	if err != nil {
		t.Errorf(err.Error())
	}

	tbl := "products"
	filter := bson.M{"Name": "Lemon"}

	defer mgoman.Close(ctx)
	_, err = mgoman.DeleteOne(
		ctx,
		tbl,
		filter,
	)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestDeleteMulti(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uri := "mongodb://localhost:27017"
	db := "ledDB"
	mgoman, err := New(uri, db, time.Second*3)
	if err != nil {
		t.Errorf(err.Error())
	}
	tbl := "products"
	filter := bson.M{"Disabled": false}

	defer mgoman.Close(ctx)
	_, err = mgoman.DeleteMany(
		ctx,
		tbl,
		filter,
	)
	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestReadOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uri := "mongodb://localhost:27017"
	db := "ledDB"
	mgoman, err := New(uri, db, time.Second*3)
	if err != nil {
		t.Errorf(err.Error())
	}
	tbl := "products"
	fil := bson.M{}
	defer mgoman.Close(ctx)

	r, err := mgoman.GetOne(
		ctx,
		tbl,
		fil,
		nil,
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	fmt.Println(r)
}

func TestReadMulti(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uri := "mongodb://localhost:27017"
	db := "ledDB"
	mgoman, err := New(uri, db, time.Second*3)
	if err != nil {
		t.Errorf(err.Error())
	}
	defer mgoman.Close(ctx)
	tbl := "products"
	fil := bson.M{}

	_, err = mgoman.GetMany(
		ctx,
		tbl,
		fil,
		nil,
	)
	if err != nil {
		t.Errorf(err.Error())
	}

}
