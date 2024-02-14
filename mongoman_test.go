package mongoman

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestReadOne(t *testing.T) {
	mongoDBHost := "mongodb://localhost:27017"
	mgoman := New(mongoDBHost)
	db := ""
	tbl := ""
	fil := bson.M{}
	_, err := mgoman.GetOne(
		db,
		tbl,
		fil,
		nil,
	)
	if err != nil {
		t.Errorf(err.Error())
	}
}
