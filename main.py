from __future__ import division
from math import log
from MRAttributeSelect import MRFindBestSplit
import pprint

pp = pprint.PrettyPrinter(indent=2)

MIN_ENTROPY = 0.001

# Uncomment the necessary runner
runner_name = 'inline'
# runner_name = 'local'
# runner_name = 'hadoop'

# Set the details of the input dataset
# input_file = 'playtennis.txt'
# attribute_names = ["outlook", "temperature", "humimdity", "wind"]
# attribute_types = ["discrete", "discrete", "discrete", "discrete"]

# input_file = 'playtennis_float.txt'
# attribute_names = ["outlook", "temperature", "humimdity", "wind_speed"]
# attribute_types = ["discrete", "discrete", "discrete", "continuous"]

# input_file = 'gpt.txt'
# attribute_names = ["age", "gender", "income"]
# attribute_types = ["continuous", "discrete", "discrete"]

# input_file = 'iris.txt'
# attribute_names = ["sepal_length",
#                    "sepal_width", "petal_length", "petal_width"]
# attribute_types = ["continuous", "continuous", "continuous", "continuous"]

# input_file='tictactoe.txt'
# attribute_names = ['0', '1', '2', '3', '4', '5', '6', '7', '8']

input_file = 'kdd/kddcup.data_10_percent_corrected_trimmed'
attribute_names = []
attribute_types = []
with open('kdd/kddcup.names') as f:
    lines = f.readlines()
    for line in lines[1:]:
        name, typ = line.split(": ")
        attribute_names.append(name)
        attribute_types.append(typ[:-2])

# Set output file name
op_file = 'rules.txt'
default_filter = ','.join('#' * len(attribute_names))


def is_class_label(v):
    return type(v) != int


def mapred_at_node(filter=default_filter, depth=0, prev_rules=""):
    filter = filter.split(',')
    attr_splits = {}
    class_count = {}
    total_rows = 0
    before_split_gain = 0
    best_split_gain, best_split_attr = -1, None

    print('Current filter: ', filter)

    mr_args = ['-r', runner_name,
               '--jobconf', 'my.job.settings.select=' + ','.join(filter),
               '--jobconf', 'my.job.settings.attributetypes=' +
               ','.join(attribute_types),
               input_file]
    mr_job = MRFindBestSplit(args=mr_args)

    # Run the job
    with mr_job.make_runner() as runner:
        runner.run()

        mr_result = mr_job.parse_output(runner.cat_output())
        # print("MR: ", dict(mr_result))

        # Store results of map reduce job in dict
        for key, value in mr_result:
            if is_class_label(key):
                total_rows += value
                class_count[key] = value
            elif key in attr_splits:
                attr_splits[key].append(value)
            else:
                attr_splits[key] = [value]

        # Show split
        # pp.pprint(attr_splits)

        # Calculate gain before splitting
        for _, count in class_count.items():
            p = count / total_rows
            before_split_gain -= p * log(p, 2)

        # Calculate best split point from all possible splits
        for attr, splits in attr_splits.items():
            if attribute_types[attr] == 'continuous':
                for split in splits:
                    val, entropy, _, _, _, _ = split
                    gain = before_split_gain - entropy
                    if gain > best_split_gain:
                        print(
                            f"gain {gain} for value {val} of attr {attr} at depth {depth}")
                        best_split_gain, best_split_attr = gain, attr
                        best_split_continous = split
            else:
                gain = 0
                for split in splits:
                    _, row, entropy, _ = split
                    gain += (row / total_rows) * entropy
                gain = before_split_gain - gain
                print(f"gain {gain} for attr {attr} at depth {depth}")
                if gain > best_split_gain:
                    best_split_gain, best_split_attr = gain, attr

        best_attr_name = attribute_names[best_split_attr] \
            if best_split_attr is not None else ""
        split_info = attr_splits[best_split_attr] \
            if best_split_attr in attr_splits else []
        best_attr_is_continuous = attribute_types[best_split_attr] == 'continuous' \
            if best_split_attr is not None else False

        # If this is the last split, write rule to file and return
        if depth == len(filter) - 1:
            print("At last splitting point")
            if best_attr_is_continuous:
                val, _, _, less_cls, _, more_cls = best_split_continous
                with open(op_file, 'a') as f:
                    f.write("{}{} <{}, {}\n".format(
                        prev_rules, best_attr_name, val, less_cls))
                    f.write("{}{} >={}, {}\n".format(
                        prev_rules, best_attr_name, val, more_cls))
            else:
                for split, _, _, cls in split_info:
                    with open(op_file, 'a') as f:
                        f.write("{}{} {}, {}\n".format(
                            prev_rules, best_attr_name, split, cls))
            return

        try:
            if best_attr_is_continuous:
                split_value, _, less_ent, less_cls, \
                    more_ent, more_cls = best_split_continous

                if less_ent <= MIN_ENTROPY:
                    with open(op_file, 'a') as f:
                        f.write(
                            f"{prev_rules}{best_attr_name} <{split_value}, {less_cls}\n")
                else:
                    clone_filter = filter[:]
                    clone_filter[best_split_attr] = "<" + str(split_value)
                    mapred_at_node(
                        filter=','.join(clone_filter),
                        depth=depth + 1,
                        prev_rules=f"{prev_rules}{best_attr_name} <{split_value}, ")

                if more_ent <= MIN_ENTROPY:
                    with open(op_file, 'a') as f:
                        f.write(
                            f"{prev_rules}{best_attr_name} >={split_value}, {more_cls}\n")
                else:
                    clone_filter = filter[:]
                    clone_filter[best_split_attr] = ">" + str(split_value)
                    mapred_at_node(
                        filter=','.join(clone_filter),
                        depth=depth + 1,
                        prev_rules=f"{prev_rules}{best_attr_name} >={split_value}, ")

            else:
                # Evaluate possible splits in best split
                for split, _, entropy, cls in split_info:
                    # If the split is uniform, write to file and not recurse
                    if entropy <= MIN_ENTROPY:
                        with open(op_file, 'a') as f:
                            f.write(
                                f"{prev_rules}{best_attr_name} {split}, {cls}\n")
                        continue

                    filter[best_split_attr] = split

                    mapred_at_node(
                        filter=','.join(filter),
                        depth=depth + 1,
                        prev_rules=f"{prev_rules}{best_attr_name} {split}, ")

        except KeyError as err:
            print("depth: ", depth)
            print("prev_rules: ", prev_rules)
            print("args: ", filter)
            print("attr_splits: ", attr_splits)
            print("best_split_attr: ", best_split_attr)
            print('--------------------------------')
            print('\n')
            raise (err)


# Clear the rules file
open(op_file, 'w').close()

mapred_at_node()
