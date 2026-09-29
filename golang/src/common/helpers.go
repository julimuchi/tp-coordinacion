package common

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func TopFruits(fruits []fruititem.FruitItem, topSize int) []fruititem.FruitItem {
	sort.SliceStable(fruits, func(i, j int) bool {
		return fruits[j].Less(fruits[i])
	})
	return fruits[:min(topSize, len(fruits))]
}

func BuildExchangeRouteKeys(amount int, prefix string) []string {
	routeKeys := make([]string, amount)
	for i := range amount {
		routeKeys[i] = fmt.Sprintf("%s_%d", prefix, i)
	}
	return routeKeys
}

func fruitNameToAggregatorIndex(fruitName string, aggTotal int) int {
	hasher := fnv.New32a()
	hasher.Write([]byte(fruitName))
	return int(hasher.Sum32() % uint32(aggTotal))
}

func CloseMiddlewares(middlewares []middleware.Middleware) {
	for _, m := range middlewares {
		if err := m.Close(); err != nil {
			slog.Warn("While closing middleware", "err", err)
		}
	}
}

func SplitFruitsByAggregator(fruits map[string]fruititem.FruitItem, aggTotal int) [][]fruititem.FruitItem {
	sets := make([][]fruititem.FruitItem, aggTotal)
	for _, fruit := range fruits {
		agg_box_id := fruitNameToAggregatorIndex(fruit.Fruit, aggTotal)
		sets[agg_box_id] = append(sets[agg_box_id], fruit)
	}
	return sets
}
