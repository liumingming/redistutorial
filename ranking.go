package redistutorial

import (
	"context"
	"github.com/redis/go-redis/v9"
)

type Ranking struct {
	Client *redis.Client
	Key    string
}

func NewRanking(client *redis.Client, key string) *Ranking {
	return &Ranking{
		Client: client,
		Key:    key,
	}
}

func (r *Ranking) SetWeight(ctx context.Context, score int64, member string) (bool, error) {
	members := []redis.Z{
		{
			Score:  float64(score),
			Member: member,
		},
	}
	res := r.Client.ZAdd(ctx, r.Key, members...)
	if res.Err() != nil {
		return false, res.Err()
	}
	return res.Val() == 1, nil
}

func (r *Ranking) GetWeight(ctx context.Context, member string) (int64, error) {
	res := r.Client.ZScore(ctx, r.Key, member)
	if res.Err() != nil {
		return 0, res.Err()
	}
	return int64(res.Val()), nil
}

func (r *Ranking) UpdateWeight(ctx context.Context, score int64, member string) (int64, error) {
	res := r.Client.ZIncrBy(ctx, r.Key, float64(score), member)
	if res.Err() != nil {
		return 0, res.Err()
	}
	return int64(res.Val()), nil
}

func (r *Ranking) Remove(ctx context.Context, member string) (bool, error) {
	res := r.Client.ZRem(ctx, r.Key, member)
	if res.Err() != nil {
		return false, res.Err()
	}
	return res.Val() == 1, nil
}

func (r *Ranking) Length(ctx context.Context) (int64, error) {
	res := r.Client.ZCard(ctx, r.Key)
	if res.Err() != nil {
		return 0, res.Err()
	}
	return res.Val(), nil
}

func (r *Ranking) TopN(ctx context.Context, top int64) ([]redis.Z, error) {
	var start int64 = 0
	var end = top - 1
	res := r.Client.ZRevRangeWithScores(ctx, r.Key, start, end)
	if res.Err() != nil {
		return nil, res.Err()
	}
	return res.Val(), nil
}
func (r *Ranking) BottomN(ctx context.Context, bottom int64) ([]redis.Z, error) {
	var start int64 = 0
	var end = bottom - 1
	res := r.Client.ZRangeWithScores(ctx, r.Key, start, end)
	if res.Err() != nil {
		return nil, res.Err()
	}
	return res.Val(), nil
}
