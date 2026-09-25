package features

import (
	"strings"
	"unicode"
)

// JaccardThreshold is the minimum bigram Jaccard similarity to count a hype-word hit.
const JaccardThreshold = 0.65

// HypeWords are reaction tokens common in esports Twitch chat (presence scored via fuzzy match).
var HypeWords = []string{
	"omg", "wtf", "holy", "wow", "www", "nooooo", "omgg", "omfg", "let's go", "damn", "insane",
	"sato", "shy", "woow", "no way", "looooooooool", "ggwp", "wtffff", "holllyyy", "ooo",
	"clean", "cinema", "lol", "wtff", "oo", "kekw", "lmao",
}

// SimpleNormalize mirrors ValSparks HypeMonitorConsumer.simpleNormalize:
// lowercase, collapse whitespace, trim, cap runs of the same character at 2.
func SimpleNormalize(s string) string {
	s = strings.ToLower(s)
	s = strings.Join(strings.Fields(s), " ")
	s = strings.TrimSpace(s)
	if s == "" {
		return s
	}

	var b strings.Builder
	b.Grow(len(s))
	var prev rune
	run := 0
	for _, r := range s {
		if unicode.IsSpace(r) {
			b.WriteRune(' ')
			prev = 0
			run = 0
			continue
		}
		if r == prev {
			run++
			if run > 2 {
				continue
			}
		} else {
			prev = r
			run = 1
		}
		b.WriteRune(r)
	}
	return b.String()
}

// HypeScore counts how many hype words fuzzy-match the window text (presence, not count).
// Matching uses character-bigram Jaccard similarity >= JaccardThreshold.
// Each candidate token/phrase votes for at most one hype word (best score) so
// "omg" does not also count "omgg" / "omfg".
func HypeScore(joinedNormalizedText string) float64 {
	candidates := matchCandidates(joinedNormalizedText)
	if len(candidates) == 0 {
		return 0
	}

	normalized := make([]string, len(HypeWords))
	for i, w := range HypeWords {
		normalized[i] = SimpleNormalize(w)
	}

	hits := make(map[string]struct{})
	for _, c := range candidates {
		bestIdx := -1
		best := 0.0
		for i, hw := range normalized {
			if hw == "" {
				continue
			}
			var j float64
			if c == hw {
				j = 1
			} else {
				j = bigramJaccard(hw, c)
			}
			if j >= JaccardThreshold && j > best {
				best = j
				bestIdx = i
			}
		}
		if bestIdx >= 0 {
			hits[normalized[bestIdx]] = struct{}{}
		}
	}
	return float64(len(hits))
}

// matchCandidates returns tokens plus 2- and 3-gram phrases from the window text.
func matchCandidates(text string) []string {
	tokens := strings.Fields(text)
	if len(tokens) == 0 {
		return nil
	}
	out := make([]string, 0, len(tokens)*2)
	out = append(out, tokens...)
	for n := 2; n <= 3; n++ {
		for i := 0; i+n <= len(tokens); i++ {
			out = append(out, strings.Join(tokens[i:i+n], " "))
		}
	}
	return out
}

// bigramJaccard is |bigrams(a) ∩ bigrams(b)| / |bigrams(a) ∪ bigrams(b)|.
// Single-rune strings fall back to exact equality (already handled) or 0.
func bigramJaccard(a, b string) float64 {
	A := charBigrams(a)
	B := charBigrams(b)
	if len(A) == 0 || len(B) == 0 {
		return 0
	}
	inter := 0
	for g := range A {
		if _, ok := B[g]; ok {
			inter++
		}
	}
	union := len(A) + len(B) - inter
	if union == 0 {
		return 0
	}
	return float64(inter) / float64(union)
}

func charBigrams(s string) map[string]struct{} {
	runes := []rune(s)
	if len(runes) < 2 {
		// Treat single char as a unary "gram" so tiny tokens can still compare.
		if len(runes) == 1 {
			return map[string]struct{}{string(runes[0]): {}}
		}
		return nil
	}
	out := make(map[string]struct{}, len(runes)-1)
	for i := 0; i < len(runes)-1; i++ {
		out[string(runes[i:i+2])] = struct{}{}
	}
	return out
}
