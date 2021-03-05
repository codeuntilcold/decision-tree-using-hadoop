from __future__ import division
from math import log
from MRAttributeSelect import MRMostUsedWord

# Uncomment the necessary runner
runner_name='inline'
# runner_name='local'
# runner_name='hadoop'

# Set the details of the input dataset
input_file='playtennis.txt'
attribute_names = ["outlook", "temperature", "humimdity", "wind"]
# input_file='tictactoe.txt'
# attribute_names = ['0', '1', '2', '3', '4', '5', '6', '7', '8']


# Set output file name
op_file='rules.txt'


default_args=','.join(['#' for _ in attribute_names])

def mapred(args=default_args, depth=0, path=""):
    args=args.split(',')
    attrs = {}
    counts = {}
    total = 0
    before_split_gain = 0
    best_split = -1, None

    a = ['-r', runner_name, '--jobconf', 'my.job.settings.select=' +
         ','.join(args), input_file]
    mr_job = MRMostUsedWord(args=a)

    # Run the job
    with mr_job.make_runner() as runner:
        runner.run()

        # Store results of map reduce job in dict
        for key, value in mr_job.parse_output(runner.cat_output()):
            if type(key) != int:
                total += value
                counts[key] = value
                continue
            if key in attrs:
                attrs[key].append(value)
            else:
                attrs[key] = [value]
        
        # Calculate gain before splitting
        for classval in counts:
            p = counts[classval]/total
            before_split_gain -= p * log(p, 2)

        # Calculate best split point from all possible splits
        for attr in attrs:
            gain = 0
            for split in attrs[attr]:
                gain += (split[1]/total) * split[2]
            gain = before_split_gain - gain
            if gain > best_split[0]:
                best_split = gain, attr

        # If this is the last split, write rule to file and return
        if depth == len(args)-1:
            for split, _, _, classVal in attrs[best_split[1]]:
                with open(op_file, 'a') as f:
                    f.write(path+"{} {}, {}\n".format(attribute_names[best_split[1]], split, classVal))
            return


        try:
            # Evaluate possible splits in best split
            for split, _, entropy, classVal in attrs[best_split[1]]:
                # If the split is uniform, write to file and not recurse
                if entropy <= 0.001:
                    with open(op_file, 'a') as f:
                        f.write(path+"{} {}, {}\n".format(attribute_names[best_split[1]], split, classVal))
                    continue

                args[best_split[1]] = split.encode('ascii')
                
                mapred(','.join(args), depth+1, path+"{} {}, ".format(attribute_names[best_split[1]], split))
        except KeyError as err:
            print(depth)
            print(path)
            print(args)
            print(attrs)
            print(best_split)
            print('--------------------------------')
            print('\n')
            raise(err)


# Clear the rules file
open(op_file, 'w').close()

mapred()
