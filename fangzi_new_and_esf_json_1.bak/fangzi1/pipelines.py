# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


from scrapy.exporters import JsonLinesItemExporter
import pymysql
from scrapy.pipelines.images import ImagesPipeline
# twisted: 用于异步写入(包含数据库)的框架，cursor.execute()是同步写入
from twisted.enterprise import adbapi


class FangPipeline(object):
    def __init__(self):
        self.esfhouse_fp = open('esfhouse.json', 'wb')
        self.esfhouse_exporter = JsonLinesItemExporter(self.esfhouse_fp, ensure_ascii=False)

    def process_item(self, item, spider):
        self.esfhouse_exporter.export_item(item)
        return item

    def close_spider(self, spider):
        self.esfhouse_fp.close()


class MySQLTwistedPipeline(object):   # 异步数据库的搭建
    def __init__(self, pool):
        self.dbpool = pool

    @classmethod
    def from_settings(cls, settings):
        """
        这个函数名称是固定的，当爬虫启动的时候，scrapy会自动调用这些函数，加载配置数据。
        :param settings:
        :return:
        """
        params = dict(
            host=settings['MYSQL_HOST'],
            port=settings['MYSQL_PORT'],
            db=settings['MYSQL_DB'],
            user=settings['MYSQL_USER'],
            passwd=settings['MYSQL_PASSWD'],
            charset=settings['MYSQL_CHARSET'],
            cursorclass=pymysql.cursors.DictCursor
        )

        # 创建一个数据库连接池对象，这个连接池中可以包含多个connect连接对象。
        # 参数1：操作数据库的包名
        # 参数2：链接数据库的参数
        db_connect_pool = adbapi.ConnectionPool('pymysql', **params)

        # 初始化这个类的对象
        obj = cls(db_connect_pool)
        return obj

    def process_item(self, item, spider):
        """
        在连接池中，开始执行数据的多线程写入操作。
        :param item:
        :param spider:
        :return:
        """
        # 参数1：在线程中被执行的sql语句
        # 参数2：要保存的数据
        result = self.dbpool.runInteraction(self.insert, item)
        # 给result绑定一个回调函数，用于监听错误信息
        result.addErrback(self.error)

    def error(self, reason):
        print('--------', reason)

    # 线面这两步分别是数据库的插入语句，以及执行插入语句。这里把插入的数据和sql语句分开写了，跟何在一起写效果是一样的

    def insert(self, cursor, item):
        insert_sql = "INSERT INTO fang(province, city, name, rooms, floor, toward, year, address, area, price, unit, \
        origin_url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(insert_sql, (item['province'], item['city'], item['name'], item['rooms'],
                                    item['floor'], item['toward'], item['year'], item['address'], item['area'],
                                    item['price'], item['unit'], item['origin_url']))
        # 不需要commit()

