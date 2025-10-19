package main

import (
	"fmt"
	"sort"
	"strconv"
)

type sortedSetMember struct {
	score  float64
	member string
}

type sortedSet struct {
	members map[string]float64
	ranks   []sortedSetMember
}

func newSortedSet() *sortedSet {
	return &sortedSet{
		members: make(map[string]float64),
		ranks:   make([]sortedSetMember, 0),
	}
}

func (set *sortedSet) add(member string, score float64) bool {
	_, exists := set.members[member]
	set.members[member] = score

	set.ranks = make([]sortedSetMember, 0, len(set.members))
	for m, s := range set.members {
		set.ranks = append(set.ranks, sortedSetMember{score: s, member: m})
	}

	// sort by score, then lexicographically
	sort.Slice(set.ranks, func(i, j int) bool {
		if set.ranks[i].score != set.ranks[j].score {
			return set.ranks[i].score < set.ranks[j].score
		}

		return set.ranks[i].member < set.ranks[j].member
	})

	return !exists
}

func (set *sortedSet) addMultiple(args []string) (int, error) {
	if len(args) < 2 || len(args)%2 != 0 {
		return 0, fmt.Errorf("wrong number of arguments")
	}

	addedCount := 0
	for i := 0; i < len(args); i += 2 {
		scoreStr := args[i]
		member := args[i+1]

		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR value is not a valid float")
		}

		if set.add(member, score) {
			addedCount++
		}
		debug("ZADD: Added/updated member %s with score %f\n", member, score)
	}

	return addedCount, nil
}

func (set *sortedSet) rank(member string) (int, bool) {
	score, exists := set.members[member]
	if !exists {
		debug("Member %s not found in sorted set\n", member)
		return -1, false
	}

	for i, m := range set.ranks {
		if m.member == member && m.score == score {
			return i, true
		}
	}

	return -1, false
}

func (set *sortedSet) getValues() []string {
	values := make([]string, 0, len(set.ranks))
	for _, m := range set.ranks {
		values = append(values, m.member)
	}

	return values
}

func (set *sortedSet) size() int {
	return len(set.members)
}

func (set *sortedSet) getScore(member string) (float64, bool) {
	score, exists := set.members[member]
	return score, exists
}

func (set *sortedSet) remove(member string) bool {
	_, exists := set.members[member]
	if !exists {
		return false
	}

	delete(set.members, member)

	// rebuid ranks 
	set.ranks = make([]sortedSetMember, 0, len(set.members))
	for m, s := range set.members {
		set.ranks = append(set.ranks, sortedSetMember{score: s, member: m})
	}
	// sort by score, then lexicographically
	sort.Slice(set.ranks, func(i, j int) bool {
		if set.ranks[i].score != set.ranks[j].score {
			return set.ranks[i].score < set.ranks[j].score
		}
		return set.ranks[i].member < set.ranks[j].member
	})

	return true
}