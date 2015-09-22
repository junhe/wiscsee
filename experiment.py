import csv
import os

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

def get_conf(sub_exp_dir):
    conf = config.Config()
    jsonpath = os.path.join(sub_exp_dir, 'config.json')
    conf.load_from_json_file(jsonpath)

    return conf

def create_result_row(sub_exp_dir):
    conf = get_conf(sub_exp_dir)
    stats = get_general_stats(sub_exp_dir)
    convert_unit_to_byte(conf, stats)

    row = conf['treatment']
    row.update(stats)
    row['hash'] = 'HASH' + str(conf['hash'])

    return row

def convert_unit_to_byte(conf, stats):
    """
    The unit in stats is count. This function convert it to bytes to make
    it more readable.
    """
    flash_page_size = conf['flash_page_size']
    flash_npage_per_block = conf['flash_npage_per_block']

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
    """
    sub_exp_dirs = [d for d in os.listdir(exp_dir)
            if os.path.isdir(os.path.join(exp_dir, d))]
    sub_exp_dirs = [os.path.join(exp_dir, d) for d in sub_exp_dirs]

    colnames = set()
    table = []
    for sub_exp_dir in sub_exp_dirs:
        row = create_result_row(sub_exp_dir)
        table.append(row)
        colnames.update(row.keys())

    # some row may miss some columns, we need to make sure every row
    # has the same columns so it can be output to a table
    colnames = list(colnames)
    print colnames
    for row in table:
        for col in colnames:
            if not col in row.keys():
                row[col] = 'NA'

    print table
    utils.table_to_file(table, os.path.join(exp_dir, 'result-table.txt'))

if __name__ == '__main__':
    # print create_result_row('/tmp/results/newnew/newsub-09-20-11-02-12-ext4--1297045593926475638/')
    create_result_table('/tmp/results/newnew/')

