import os
import csv

from commons import *
from ftlsim_commons import *
from .host import Host
from utilities import utils

from pyreuse.sysutils import blocktrace, blockclassifiers, dumpe2fsparser
from pyreuse.fsutils import ext4dumpextents



class GcLog(object):
    def __init__(self, device_path, result_dir, flash_page_size):
        self.device_path = device_path
        self.result_dir = result_dir
        self.flash_page_size = flash_page_size

        self.gclog_path = os.path.join(self.result_dir, 'gc.log')
        self.dumpe2fs_out_path = os.path.join(self.result_dir, 'dumpe2fs.out')
        self.extents_path = os.path.join(self.result_dir, 'extents.json')
        self.fs_block_size = 4096

    def classify_lpn_in_gclog(self):
        extents = self._get_extents()
        filepath_classifier = blockclassifiers.Ext4FileClassifier(extents,
                self.fs_block_size)

        range_table = self._get_range_table()
        classifier = blockclassifiers.Ext4BlockClassifier(range_table,
                self.fs_block_size)

        new_table = []
        with open(self.gclog_path , 'rb') as f:
            reader = csv.DictReader(f, skipinitialspace=True)
            for row in reader:
                newrow = dict(zip(row.keys()[0].split(), row.values()[0].split()))
                if newrow['lpn'] != 'NA':
                    offset = int(newrow['lpn']) * self.flash_page_size
                    sem = classifier.classify(offset)
                    if sem == 'UNKNOWN':
                        sem = filepath_classifier.classify(offset)
                else:
                    sem = 'NA'
                newrow['semantics'] = sem
                new_table.append(newrow)

        with open(self.gclog_path+'.parsed', 'w') as f:
            f.write(utils.table_to_str(new_table))

    def _get_extents(self):
        d = utils.load_json(self.extents_path)
        extents = d['extents']

        return extents

    def _get_range_table(self):
        with open(self.dumpe2fs_out_path, 'r') as f:
            text = f.read()

        header_text, bg_text = text.split("\n\n\n")

        range_table = dumpe2fsparser.parse_bg_text(bg_text)

        j_start, j_end = self._get_journal_block_ext(header_text)
        if j_start != -1:
            range_table.append( {'journal': (j_start, j_end)} )

        return range_table

    def _get_journal_block_ext(self, header_text):
        header_dict = dumpe2fsparser.parse_header_text(header_text)

        if header_dict.has_key('journal-inode') is not True:
            return -1, -1

        journal_inum = header_dict['journal-inode']
        journal_len = header_dict['journal-length']

        ext_text = ext4dumpextents.dump_extents_of_a_file(self.device_path,
                '<{}>'.format(journal_inum))
        table = ext4dumpextents.parse_dump_extents_output(ext_text)
        return table[0]['Physical_start'], table[0]['Physical_end']


