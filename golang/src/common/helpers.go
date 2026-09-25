package common

import (
	"fmt"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
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
