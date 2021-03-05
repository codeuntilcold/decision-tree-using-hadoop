#!/usr/bin/python3

from __future__ import division
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env


class MRMostUsedWord(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_attr,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]

    def mapper_get_attr(self, _, line):
        # yield each attribute, class value in the line
        selected = jobconf_from_env('my.job.settings.select')
        selected = selected.strip().split(',')
        attrValues = line.split(',')
        match = True
        for i in range(len(selected)):
            if selected[i] != '#' and selected[i] != attrValues[i]:
                match = False
                break
        if match:
            classVal = attrValues[-1]
            # yield str("lines"), 1
            yield str(classVal), 1
            for idx, val in enumerate(attrValues[:-1]):
                if selected[idx] == '#':
                    yield (idx, val, classVal), 1

    def combiner_count_words(self, comb, counts):
        # optimization: sum the combinations we've seen so far
        yield (comb, sum(counts))

    def reducer_count_words(self, comb, counts):
        #	print(comb,type(comb))
        if type(comb) in [str, unicode]:
            yield comb, sum(counts)
        else:
            yield (comb[0], comb[1]), (comb[-1], sum(counts))

    def reducer_find_max_word(self, word, word_count_pairs):
        from math import log
        if type(word) in [str, unicode]:
            yield word, sum(word_count_pairs)
        else:
            mp = {}
            total = 0
            ent = 0
            for classVal, count in word_count_pairs:
                if classVal in mp:
                    mp[classVal] += count
                else:
                    mp[classVal] = count
                total += count
            max_p, classval = -1, None
            for c in mp:
                v = mp[c]
                p = v/total
                if p > max_p:
                    max_p, classval = p, c
                ent -= p*log(p, 2)
            yield word[0], (word[1], total, ent, classval)


if __name__ == '__main__':
    MRMostUsedWord.run()
