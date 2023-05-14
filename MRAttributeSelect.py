#!/usr/bin/python3

from __future__ import division
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env


class MRFindBestSplit(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_attr,
                   combiner=self.combiner_count,
                   reducer=self.reducer_count),
            MRStep(reducer=self.reducer_find_max_p)
        ]

    # yield each attribute, class value in the line
    # by default emit all attribute in a row
    def mapper_get_attr(self, _, line):
        filters = jobconf_from_env('my.job.settings.select')
        filters = filters.strip().split(',')
        values = line.split(',')
        match = True
        for (filter, value) in zip(filters, values):
            if filter != '#' and filter != value:
                match = False
                break
        if match:
            class_val = values[-1]
            yield str(class_val), 1

            for attr_index, val in enumerate(values[:-1]):
                yield (attr_index, val, class_val), 1

    def combiner_count(self, comb, counts):
        # optimization: sum the combinations we've seen so far
        yield (comb, sum(counts))

    def reducer_count(self, comb, counts):
        # print(comb,type(comb))
        if type(comb) in [str]:
            yield comb, sum(counts)
        else:
            rownum, val, class_val = comb
            yield (rownum, val), (class_val, sum(counts))

    def reducer_find_max_p(self, word, class_count_pairs):
        from math import log
        if type(word) in [str]:
            yield word, sum(class_count_pairs)
        else:
            mp = {}
            total = 0
            entropy = 0
            attr_idx, val = word
            # For a given attribute and attribute value
            for class_val, count in class_count_pairs:
                if class_val in mp:
                    mp[class_val] += count
                else:
                    mp[class_val] = count
                total += count
            max_p, class_max_p = -1, None
            for class_val, class_count in mp.items():
                p = class_count/total
                if p > max_p:
                    max_p, class_max_p = p, class_val
                entropy -= p*log(p, 2)
            yield attr_idx, (val, total, entropy, class_max_p)


if __name__ == '__main__':
    MRFindBestSplit.run()
