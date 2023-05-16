#!/usr/bin/python3

from __future__ import division
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env


def count_within_value(list_of_tuples):
    mp = {}
    for val, class_val in list_of_tuples:
        if val not in mp:
            mp[val] = {class_val: 1}
        else:
            if class_val not in mp[val]:
                mp[val][class_val] = 1
            else:
                mp[val][class_val] += 1
    return mp


def count_regardless_of_value(list_of_tuples):
    mp = {}
    for _, class_val in list_of_tuples:
        if class_val not in mp:
            mp[class_val] = 1
        else:
            mp[class_val] += 1
    return mp


class MRFindBestSplit(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_attr,
                   # combiner=self.combiner_count,
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
            if filter != '#':
                if filter[0] == '>' and float(value) < float(filter[1:]):
                    match = False
                    break
                elif filter[0] == '<' and float(value) >= float(filter[1:]):
                    match = False
                    break
                if filter != value:
                    match = False
                    break
        if match:
            class_val = values[-1]
            yield str(class_val), 1

            for attr_index, val in enumerate(values[:-1]):
                yield attr_index, (val, class_val)

    # optimization: sum the combinations we've seen so far
    # only apply when the value of map is numerical
    def combiner_count(self, comb, counts):
        yield (comb, sum(counts))

    def reducer_count(self, comb, counts):
        # print(comb,type(comb))
        if type(comb) in [str, bytes]:
            yield comb, sum(counts)
        else:
            attr_index = comb
            val_class_list = counts

            attr_types = jobconf_from_env('my.job.settings.attributetypes')
            this_type = attr_types.split(",")[attr_index]

            if this_type == 'continuous':
                # Sort the list by value
                val_class_list = list(
                    map(lambda x: (float(x[0]), x[1]), val_class_list))
                val_class_list.sort(key=lambda x: x[0])

                # then split the array to less and greater
                # then emit the split data
                for index in range(1, len(val_class_list) - 1):
                    less_list = val_class_list[:index]
                    more_list = val_class_list[index:]
                    val = val_class_list[index][0]

                    less_map = count_regardless_of_value(less_list)
                    more_map = count_regardless_of_value(more_list)

                    set_of_class_vals = set({**less_map,
                                             **more_map}.keys())
                    for class_val in set_of_class_vals:
                        less = less_map[class_val] \
                            if class_val in less_map else 0
                        more = more_map[class_val] \
                            if class_val in more_map else 0
                        yield (attr_index, val), (class_val, less, more)
            else:
                # (val1, cls1), (val1, cls2), (val1, cls1)
                # val1: { cls1: 10, cls2: 5, ...]
                v_map = count_within_value(val_class_list)
                for val, val_map in v_map.items():
                    for class_val, count in val_map.items():
                        yield (attr_index, val), (class_val, count)

    def reducer_find_max_p(self, word, class_count_pairs):
        from math import log
        if type(word) in [str, bytes]:
            yield word, sum(class_count_pairs)
        else:
            mp = {}
            total = 0
            less_count, more_count = 0, 0
            entropy = 0
            less_entropy, more_entropy = 0, 0

            attr_idx, val = word
            attr_types = jobconf_from_env('my.job.settings.attributetypes')
            this_type = attr_types.split(",")[attr_idx]
            is_continuous = this_type == 'continuous'

            # For a given attribute and attribute value
            for class_count in class_count_pairs:
                if not is_continuous:
                    class_val, count = class_count
                    if class_val in mp:
                        mp[class_val] += count
                    else:
                        mp[class_val] = count
                    total += count
                else:
                    class_val, less, more = class_count
                    if class_val not in mp:
                        mp[class_val] = (less, more)
                    else:
                        mp[class_val][0] += less
                        mp[class_val][1] += more
                    less_count += less
                    more_count += more

            max_p, class_max_p = -1, None
            for class_val, class_count in mp.items():
                if not is_continuous:
                    p = class_count / total
                    if p > max_p:
                        max_p, class_max_p = p, class_val
                    entropy -= p*log(p, 2)
                else:
                    less, more = class_count
                    p_less = less / less_count
                    less_entropy -= 0 if p_less == 0 else p_less * log(p_less, 2)
                    p_more = more / more_count
                    more_entropy -= 0 if p_more == 0 else p_more * log(p_more, 2)
                    p = less + more / (less_count + more_count)
                    if p > max_p:
                        max_p, class_max_p = p, class_val

            if not is_continuous:
                yield attr_idx, (val, total, entropy, class_max_p)
            else:
                split_entropy = \
                    (less_count * less_entropy + more_count * more_entropy) / (less_count + more_count)
                yield attr_idx, (val, split_entropy, class_max_p)


if __name__ == '__main__':
    MRFindBestSplit.run()
