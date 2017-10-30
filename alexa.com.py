# coding=utf-8
import os
import re
import requests
import redis
import time
import sys
import traceback
import pymysql
import datetime
from enum import Enum
from bs4 import BeautifulSoup
from multiprocessing import Process


OutputType = Enum('OutputType', ('file', 'mysql'))
LoadSource = Enum('load_source', ('file', 'mysql'))
RedisKeyType = Enum('redis_key', ('list', 'set'))


class ConfigException(Exception):
    pass

class Config():
    # multiprocess
    proc_num = 30
    # redis
    redis_key = 'alexa_rank'
    redis_host = 'localhost'
    redis_password = None
    redis_key_type = RedisKeyType.set
    # output
    output_base_filename = './rank_results/alexa_result_'
    output_type = OutputType.mysql
    # load
    load_source = LoadSource.mysql
    clear_redis_before_load = True
    load_mysql_query = "select domain from alexa_rank"
    # mysql
    mysql_args = dict(host='localhost', user='root', passwd='passwd', db='test')

class Alexa(object):
    def __init__(self):
        self.base_url = 'https://www.alexa.com/siteinfo/'
        self._init_redis()
        if Config.output_type == OutputType.file:
            self._init_savefile()
        elif Config.output_type == OutputType.mysql:
            self._init_mysql()
        else:
            print('wrong output config')

    def _init_redis(self):
        self.rdb = redis.Redis(host=Config.redis_host, password=Config.redis_password, decode_responses=True)
        self.redis_key = Config.redis_key

    def _init_savefile(self):
        self.save_filename = Config.output_base_filename + str(time.time())
        self.save_dir = os.path.dirname(self.save_filename)
        if not os.path.exists(self.save_dir):
            os.mkdir(self.save_dir)

    def _init_mysql(self):
        self.mysql = pymysql.connect(**Config.mysql_args)
        self.mysql_cur = self.mysql.cursor()

    def craw(self, domain):
        url = self.base_url + domain
        try:
            response = requests.get(url, verify=False)
            return response.content
        except Exception as e:
            traceback.print_exc()
            return ''

    def _parse_visitors_rank_china(self, bs):
        section = bs.find('section', {'id':'visitors-content'})
        if section is None:
            return -1
        try:
            country_china = section.table.tbody.find('img', alt='China Flag')
        except Exception as e:
            traceback.print_exc()
            return -2
        if country_china is None:
            return 0
        try:
            china_rank = country_china.parent.parent.parent.find_all('td')[2].span.string
            rank = int(china_rank.strip().replace(',', ''))
            return rank
        except Exception as e:
            traceback.print_exc()
            return -3

    def _parse_global_rank_in_script(self, bs):
        pat = re.compile(r'"siteinfo":{"rank":{.*"global":([\d\w]+)')
        script = bs.find('script', string=pat)
        if script is None:
            return -1
        try:
            rank = re.search(pat, script.string).group(1)
        except Exception as e:
            traceback.print_exc()
            return -2
        if rank == 'false':
            return 0
        else:
            try:
                rank = int(rank)
            except Exception as e:
                traceback.print_exc()
                return -3
            return rank

    def _parse_global_rank(self, bs):
        section = bs.find('section', id='rank-panel-content')
        if section is None:
            return -1
        try:
            rank = section.find('span', {'data-cat': 'globalRank'}).find('img', title='Global rank icon').next_sibling.text.strip()
        except Exception as e:
            traceback.print_exc()
            return -2
        if rank == '-':
            return 0
        try:
            return int(rank.replace(',', ''))
        except Exception as e:
            traceback.print_exc()
            return -3

    def _parse_china_rank(self, bs):
        section = bs.find('section', id='rank-panel-content')
        if section is None:
            return -1
        span = section.find('span', class_='countryRank')
        if span is None:
            return 0
        china = span.find('img', title='China Flag')
        if china is None:
            return 0
        try:
            rank = china.next_sibling.text.strip()
        except Exception as e:
            traceback.print_exc()
            return -2
        if rank == '-':
            return 0
        try:
            return int(rank.replace(',', ''))
        except Exception as e:
            traceback.print_exc()
            return -3

    def parse(self, response):
        if not response:
            return -1,-1,-1,-1
        bs = BeautifulSoup(response, 'lxml')
        rank_china = self._parse_china_rank(bs)
        rank_global = self._parse_global_rank(bs)
        rank_china_visitors = self._parse_visitors_rank_china(bs)
        rank_global_script = self._parse_global_rank(bs)
        return rank_china, rank_global, rank_china_visitors, rank_global_script

    def get_domain(self):
        if Config.redis_key_type == RedisKeyType.list:
            domain = self.rdb.rpop(self.redis_key)
        elif Config.redis_key_type == RedisKeyType.set:
            domain = self.rdb.spop(self.redis_key)
        else:
            raise ConfigException("wrong config 'redis_key_type'")
        return domain

    def save_result_to_file(self, domain, results):
        with open(self.save_filename, 'a') as f:
            f.write(domain + ',' + ','.join([str(rank) for rank in results]) + '\n')

    def save_result_to_mysql(self, sql):
        try:
            return self.mysql_cur.execute(sql)
        except Exception as e:
            traceback.print_exc()
            self._init_mysql()
            return self.mysql_cur.execute(sql)

    def get_rank(self):
        while True:
            domain = self.get_domain()
            if not domain:
                break
            try:
                print('craw %s' % domain)
                response = self.craw(domain)
                results = self.parse(response)
            except Exception as e:
                traceback.print_exc()
                continue
            now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if Config.output_type == OutputType.file:
                self.save_result_to_file(domain, results)
            elif Config.output_type == OutputType.mysql:
                sql = '''
                      insert into alexa_rank (domain, china_1, global_1, china_2, global_2, craw_time) 
                      values ('%s', '%s', '%s', '%s', '%s', '%s')
                      ''' % (domain, results[0], results[1], results[2], results[3], now_str)
                self.save_result_to_mysql(sql)

    def load_domains_from_file(self, filename, file_encode):
        with open(filename, 'r', encoding=file_encode) as f:
            domains_to_push = []
            for line in f:
                domain = line.split(',')[0].split(' ')[0].strip()
                domains_to_push.append(domain)
                if len(domains_to_push) % 100 == 0:
                    self._load_to_redis(domains_to_push)
                    domains_to_push.clear()
            if len(domains_to_push) > 0:
                self._load_to_redis(domains_to_push)
                domains_to_push.clear()

    def load_domains_from_mysql(self, sql):
        self._init_mysql()
        self.mysql_cur.execute(sql)
        while True:
            query_result = self.mysql_cur.fetchmany(100)
            if len(query_result) == 0:
                break
            domains_to_push = [i[0] for i in query_result]
            self._load_to_redis(domains_to_push)

    def _load_to_redis(self, domains_to_push):
        if Config.redis_key_type == RedisKeyType.list:
            add = self.rdb.lpush
        elif Config.redis_key_type == RedisKeyType.set:
            add = self.rdb.sadd
        else:
            raise ConfigException("wrong config 'redis_key_type'")

        add(self.redis_key, *domains_to_push)

    def clear_redis(self):
        self.rdb.delete(self.redis_key)

    def load_domains(self, args):
        if Config.load_source == LoadSource.file:
            if len(args) < 3:
                print('please input load filename')
                sys.exit()
            filename = args[2]
            if len(args) > 3:
                file_encode = args[3]
            else:
                file_encode = 'gbk'
            self.load_domains_from_file(filename, file_encode)
        elif Config.load_source == LoadSource.mysql:
            sql = Config.load_mysql_query
            self.load_domains_from_mysql(sql)


def proc():
    alexa = Alexa()
    alexa.get_rank()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] != 'load':
            print("arg on position 1 can only be 'load'")
            sys.exit()

        alexa = Alexa()
        if Config.clear_redis_before_load:
            print('clear redis')
            alexa.clear_redis()
        print('load domains')
        alexa.load_domains(sys.argv)
        print('load complete')

    for i in range(Config.proc_num):
        Process(target=proc).start()
