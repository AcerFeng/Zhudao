#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-01-11 23:47:00
# Project: douyu_cate_parent

from pyspider.libs.base_handler import *
import pymysql
from datetime import datetime

class Handler(BaseHandler):
    crawl_config = {
    }
    
    def __init__(self):
        try:
            self.connect = pymysql.connect(host='localhost', port=3306, user='root', passwd='123456', db='zhudao', charset='utf8mb4')
            
        except Exception as e:
            print('Cannot Connect To Mysql!/n', e)
            raise e

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('http://capi.douyucdn.cn/api/v1/getColumnList', callback=self.detail_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('a[href^="http"]').items():
            self.crawl(each.attr.href, callback=self.detail_page)

    @config(priority=2)
    def detail_page(self, response):
        return {
          'results': response.json['data']
        }
    
    def on_result(self,result):
        if not result:
            return
        self.save_data(**result)
        
    def save_data(self, **kw):

        if len(kw['results']) == 0:
            return

        for item in kw['results']:
            try:
                cursor = self.connect.cursor()
                cursor.execute('select id from cate_parent where short_name=%s', (item['short_name'],))
                result = cursor.fetchone()
                if result:
                    # 更新操作
                    sql = '''update cate_parent set 
                        cate_id=%s, 
                        cate_name=%s, 
                        update_time=%s
                        where short_name=%s and platform_id=%s'''
                    cursor.execute(sql, (item['cate_id'], 
                                         item['cate_name'], 
                                         datetime.now(), 
                                         item['short_name'], 
                                         1,))
                else:
                    # 插入操作
                    sql = '''insert into cate_parent(cate_id, cate_name, short_name, platform_id, created_time) 
                    values (%s, %s, %s, %s, %s)'''
                    cursor.execute(sql, (item['cate_id'], 
                                         item['cate_name'], 
                                         item['short_name'], 
                                         1, 
                                         datetime.now(),))
                self.connect.commit()

            except Exception as e:
                self.connect.rollback()
                raise e
