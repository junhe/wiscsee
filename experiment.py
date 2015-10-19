import csv
import os
import pprint

import config
import utils

def csv_to_table(fpath, fieldnames = None, delimiter = ';'):
    """
    It reads a csv and puts the contents to a list.
    """
    table = []
    with open(fpath) as csvfile:
        reader = csv.DictReader(csvfile, fieldnames = fieldnames,
            delimiter = delimiter)
        for row in reader:
            table.append(row)

    return table

def get_general_stats(sub_exp_dir):
    """
    This is a the stats from ftlsim.out.stats.
    """
    stats_path = os.path.join(sub_exp_dir, 'ftlsim.out.stats')
    stats_table = csv_to_table(stats_path)
    assert len(stats_table) == 1
    stats = stats_table[0]
    stats = {k: int(v) for k, v in stats.items()}

    return stats

def get_count_table(sub_exp_dir):
    """
    This is a the stats from ftlsim.out.stats.
    """
    table_path = os.path.join(sub_exp_dir, 'ftlsim.out.count_table')
    table = csv_to_table(table_path)

    return table

def get_cache_counter(count_table):
    d = {}
    for row in count_table:
        if row['counter.name'] == 'cache':
            d[row['item.name']] = int(row['count'])

    return d

def get_conf(sub_exp_dir):
    conf = config.Config()
    jsonpath = os.path.join(sub_exp_dir, 'config.json')

    if not os.path.exists(jsonpath):
        return None

    conf.load_from_json_file(jsonpath)

    return conf

def create_result_row(sub_exp_dir):
    conf = get_conf(sub_exp_dir)
    if conf == None:
        return None

    stats = get_general_stats(sub_exp_dir)
    convert_unit_to_byte(conf, stats)

    # calculate hit ratio
    count_table = get_count_table(sub_exp_dir)
    cache_info = get_cache_counter(count_table)

    row = conf['treatment']
    row.update(stats)
    row['hash'] = 'HASH' + str(conf['hash'])
    row['filesystem'] = conf['filesystem']
    row['cache_hit_ratio'] = float(cache_info['hit']) / \
        (cache_info['hit'] + cache_info['miss'])

    return row

def convert_unit_to_byte(conf, stats):
    """
    The unit in stats is count. This function convert it to bytes to make
    it more readable.
    """
    flash_page_size = conf['flash_page_size']
    flash_npage_per_block = conf['flash_npage_per_block']

    # TODO: Use column names here to be more specific
    for key, value in stats.items():
        if 'phy_block_erase' in key:
            # This is a block operation
            stats[key] = value * flash_npage_per_block * flash_page_size
        else:
            # This a page operation
            stats[key] = value * flash_page_size

def create_result_table(exp_dir):
    """
    Each directory in exp_dir is a sub experiment dir
    This is for dftl2
    """
    sub_exp_dirs = [d for d in os.listdir(exp_dir)
            if os.path.isdir(os.path.join(exp_dir, d))]
    sub_exp_dirs = [os.path.join(exp_dir, d) for d in sub_exp_dirs]

    colnames = set()
    table = []
    for sub_exp_dir in sub_exp_dirs:
        row = create_result_row(sub_exp_dir)
        if row == None:
            continue
        table.append(row)
        colnames.update(row.keys())

    # some row may miss some columns, we need to make sure every row
    # has the same columns so it can be output to a table
    colnames = list(colnames)
    # print colnames
    for row in table:
        for col in colnames:
            if not col in row.keys():
                row[col] = 'NA'

    pprint.pprint( table )
    utils.table_to_file(table, os.path.join(exp_dir, 'result-table.txt'))


################# For NKFTL #####################
def nkftl_create_result_row(sub_exp_dir):
    conf = get_conf(sub_exp_dir)
    if conf == None:
        return None

    stats = get_general_stats(sub_exp_dir)
    convert_unit_to_byte(conf, stats)

    row = {}
    row.update(stats)
    row['hash'] = 'HASH' + str(conf['hash'])
    row['filesystem'] = conf['filesystem']

    return row

def nkftl_create_result_table(exp_dir):
    """
    Each directory in exp_dir is a sub experiment dir
    """
    sub_exp_dirs = [d for d in os.listdir(exp_dir)
            if os.path.isdir(os.path.join(exp_dir, d))]
    sub_exp_dirs = [os.path.join(exp_dir, d) for d in sub_exp_dirs]

    colnames = set()
    table = []
    for sub_exp_dir in sub_exp_dirs:
        row = nkftl_create_result_row(sub_exp_dir)
        if row == None:
            continue
        table.append(row)
        colnames.update(row.keys())

    # some row may miss some columns, we need to make sure every row
    # has the same columns so it can be output to a table
    colnames = list(colnames)
    # print colnames
    for row in table:
        for col in colnames:
            if not col in row.keys():
                row[col] = 'NA'

    pprint.pprint( table )
    utils.table_to_file(table, os.path.join(exp_dir, 'result-table.txt'))


if __name__ == '__main__':
    # create_result_table('/tmp/results/newnew/')

    nkftl_create_result_table('/tmp/results/thrid/')

    # get cache info
    # table = get_count_table('/tmp/results/reallyreally/default-subexp-f2fs-09-21-21-54-02-2663993657917388495/')
    # print get_cache_counter(table)


