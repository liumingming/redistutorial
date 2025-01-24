package redistutorial

import (
	"context"
	"github.com/redis/go-redis/v9"
	"testing"
)

func TestRanking(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	ctx := context.Background()
	ranking := NewRanking(client, "DB-ranking")

	res, err := ranking.SetWeight(ctx, 157, "Redis")
	if err != nil {
		t.Error(err)
	}
	if res != true {
		t.Error("res should be true")
	}

	res, err = ranking.SetWeight(ctx, 118, "SQLite")
	if err != nil {
		t.Error(err)
	}
	if res != true {
		t.Error("res should be true")
	}

	res, err = ranking.SetWeight(ctx, 1101, "MySQL")
	if err != nil {
		t.Error(err)
	}
	if res != true {
		t.Error("res should be true")
	}

	res, err = ranking.SetWeight(ctx, 634, "PostgresSQL")
	if err != nil {
		t.Error(err)
	}
	if res != true {
		t.Error("res should be true")
	}

	topN, err := ranking.TopN(ctx, 2)
	if err != nil {
		t.Error(err)
	}
	if len(topN) != 2 {
		t.Error("topN should contain 2 items")
	}

	if topN[0].Member != "MySQL" {
		t.Error("topN[0] should be MySQL")
	}
	if topN[1].Member != "PostgresSQL" {
		t.Error("topN[1] should be SQLite")
	}
}
