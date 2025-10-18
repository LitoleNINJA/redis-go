package main

import "sort"

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
