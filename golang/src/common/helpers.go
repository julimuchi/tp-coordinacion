package common

import (
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

func TopFruits(fruits []fruititem.FruitItem, topSize int) []fruititem.FruitItem {
	sort.SliceStable(fruits, func(i, j int) bool {
		return fruits[j].Less(fruits[i])
	})
	return fruits[:min(topSize, len(fruits))]
}
