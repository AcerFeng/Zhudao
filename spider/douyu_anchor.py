#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-01-07 22:18:07
# Project: douyu_anchor

from pyspider.libs.base_handler import *
import re

class Handler(BaseHandler):
    headers= {
    "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding":"gzip, deflate, sdch",
    "Accept-Language":"zh-CN,zh;q=0.8",
    "Cache-Control":"max-age=0",
    "Connection":"keep-alive",
    "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36"
    }
    crawl_config = {
        'itag': 'v001',
        'headers': headers,
    }
    
    def __init__(self):
        self.is_page_pt = re.compile(r'var \$PAGE = ')
        self.count_pt = re.compile(r'count: \"(\d+)\"')
        self.rk_pt = re.compile(r'\$PAGE\.rk= \"(\w+)\";')

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('https://www.douyu.com/directory/game/wzry', callback=self.index_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for script in response.doc('script').items():
            if self.is_page_pt.match(script.text()):
                re_count = self.count_pt.search(script.text())
                count = int(re_count.group(1)) if re_count else 0
                re_rk = self.rk_pt.search(script.text())
                rk = re_rk.group(1) if re_rk else ''
                if count > 0:
                    self.crawl('https://www.douyu.com/gapi/rkc/directory/%s/%s' % (rk,1), callback=self.list_page, save={
                        'rk': rk,
                        'count': count,
                        'currentPage': 1
                    })
                    
    def list_page(self, response):
        for item in response.json['data']['rl']:
            url = 'https://www.douyu.com%s' % item['url']
            self.crawl(url, callback=self.detail_page, save={
                        'room_name': item['rn'],
                        'room_id': item['url'][1:],
                        'name': item['nn'],
                        'cid2': item['cid2'],
                        'category_name': item['c2name'],
                        'img': item['rs1'],
                        'online': item['ol'],
                    })
        
        save = response.save;
        if (save['count'] <= save['currentPage']):
            return
        save['currentPage'] += 1
        self.crawl('https://www.douyu.com/gapi/rkc/directory/%s/%s' % (save['rk'], save['currentPage']), callback=self.list_page, save=save)
        
        
        
    @config(priority=2)
    def detail_page(self, response):
        
        return {
            "url": response.url,
            "results": response.doc('title').text(),
        }
