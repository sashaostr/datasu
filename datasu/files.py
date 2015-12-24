__author__ = 'sashaostr'

import gzip


def reduce_data_gz(source_filename, target_filename, ids_dict, by_column):

    with gzip.open(source_filename, 'r') as source_file , open(target_filename,'w') as target_file:
        #write header row
        line = source_file.readline()
        cols = line.split(',')
        cols = [col.strip() for col in cols]
        val_index = cols.index(by_column)
        target_file.write(line)

        source_line_count=0
        target_line_count = 0
        while True:
            line = source_file.readline()
            if not line:
                break

            source_line_count+=1
            vals = line.split(',')
            if vals[val_index] in ids_dict:
                target_file.write(line)
                target_line_count+=1
            if source_line_count%10000000 == 0:
                print "source_line_count = "+str(source_line_count)+" target_line_count = "+str(target_line_count)