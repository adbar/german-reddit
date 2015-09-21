#!/usr/bin/python3
# -*- coding: utf-8 -*-


### Script available from https://github.com/adbar/german-reddit
### Copyright Adrien Barbaresi, 2015.
### MIT license


from __future__ import print_function

import argparse
import atexit
import io
from multiprocessing import Pool, Value, Lock
import re
import sys
import time
import ujson

# language specific imports
import langid    # https://github.com/saffsd/langid.py
import enchant   # https://github.com/rfk/pyenchant/ "python-enchant" on Debian/Ubuntu
dict_en = enchant.Dict("en_US")
dict_de = enchant.Dict("de_DE")

lock = Lock()

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--inputfile', dest='inputfile', help='name of the input file', required=True)
parser.add_argument('-o', '--outputfile', dest='outputfile', help='name of the output file', required=True)
parser.add_argument('-p', '--processes', dest='processes', help='number of processes (has to be an integer)', required=True)
args = parser.parse_args()


# line-by-line filtering
def process_line(line):
    line = line.strip()
    parsed_json = ujson.loads(line)
    sanitized_body = parsed_json['body'].replace('\r', '')
    sanitized_body = sanitized_body.replace('\n', ' ')
    sanitized_body = re.sub(r'\(?http[^ ]+\)?', '', sanitized_body)
    if len(sanitized_body) > 10 and sanitized_body != '[deleted]':
        ## first test
        tcount = 0
        errors_en = 0
        errors_de = 0
        for token in re.findall(r'\w+', sanitized_body, re.UNICODE):
            tcount += 1
            if dict_en.check(token) is False:
                errors_en += 1
            if dict_de.check(token) is False:
                errors_de += 1
        if tcount == 0:
            return
        if ((errors_en/tcount) > 0.3) and ((errors_de/tcount) < 0.7):
            ## second test
            langid_response = langid.classify(sanitized_body)
            if langid_response[0] == 'de':
                return (parsed_json['id'], sanitized_body, line)

# store result in file
def handle_result(result):
    if result:
        # lock necessary because of concurrency / race conditions
        with lock:
            with io.open(args.outputfile, 'a', encoding='utf8') as outputfh:
                outputfh.write(unicode(result[2]) + '\n')

# shut down processes nicely
@atexit.register
def the_end():
    pool.close()
    pool.terminate()

# launch multiprocessing and collect results
if __name__ == "__main__":
    start_time = time.time()
    print ('### starting:', args.inputfile)
    print ('### pool size:', args.processes)
    pool = Pool(processes = int(args.processes), maxtasksperchild=10000)
    with open(args.inputfile, 'r') as inputfh:
        results = pool.imap_unordered(process_line, inputfh, 50000)
        for r in results:
            handle_result(r)
    pool.close()
    pool.join()

