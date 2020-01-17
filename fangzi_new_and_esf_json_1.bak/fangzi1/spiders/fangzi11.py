# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
import scrapy
import re
from fangzi1.items import ESFHouseItem


class SfwSpiderSpider(scrapy.Spider):
    name = 'fangzi11'
    allowed_domains = ['fang.com']
    start_urls = ['http://www.fang.com/SoufunFamily.htm']

    def parse(self, response):
        trs = response.xpath("//div[@class='outCont']//tr")
        province = None
        for tr in trs:
            # 排除掉第一个td，两个第二个和第三个td标签
            tds = tr.xpath(".//td[not(@class)]")
            province_td = tds[0]
            province_text = province_td.xpath(".//text()").get()
            # 如果第二个td里面是空值，则使用上个td的省份的值
            province_text = re.sub(r"\s", "", province_text)
            if province_text:
                province = province_text
            # 排除海外城市
            if province == '其它':
                continue

            city_td = tds[1]
            city_links = city_td.xpath(".//a")
            for city_link in city_links:
                city = city_link.xpath(".//text()").get()
                city_url = city_link.xpath(".//@href").get()
                # print("省份：", province)
                # print("城市：", city)
                # print("城市链接：", city_url)
                # 下面通过获取的city_url拼接出新房和二手房的url链接
                # 城市url：http://cq.fang.com/
                # 二手房：http://esf.cq.fang.com/
                url_module = city_url.split("//")
                scheme = url_module[0]  # http:
                domain = url_module[1]  # cq.fang.com/
                if 'bj' in domain:
                    esf_url = ' http://esf.fang.com/'
                else:
                    # 二手房url
                    esf_url = scheme + '//' + "esf." + domain + "house/s/"
                yield scrapy.Request(url=esf_url,
                                     callback=self.parse_esf,
                                     meta={'info': (province, city)})

    def parse_esf(self, response):
        # 二手房
        province, city = response.meta.get('info')
        dls = response.xpath("//div[@class='shop_list shop_list_4']/dl")
        for dl in dls:
            item = ESFHouseItem(province=province, city=city)
            name = dl.xpath(".//span[@class='tit_shop']/text()").get()
            if name:
                infos = dl.xpath(".//p[@class='tel_shop']/text()").getall()
                infos = list(map(lambda x: re.sub(r"\s", "", x), infos))
                for info in infos:
                    if "厅" in info:
                        item["rooms"] = info
                    elif '层' in info:
                        item["floor"] = info
                    elif '向' in info:
                        item['toward'] = info
                    elif '㎡' in info:
                        item['area'] = info
                    elif '年建' in info:
                        item['year'] = re.sub("年建", "", info)
                item['address'] = dl.xpath(".//p[@class='add_shop']/span/text()").get()
                # 总价
                item['price'] = "".join(dl.xpath(".//span[@class='red']//text()").getall())
                # 单价
                item['unit'] = dl.xpath(".//dd[@class='price_right']/span[2]/text()").get()
                item['name'] = name
                detail = dl.xpath(".//h4[@class='clearfix']/a/@href").get()
                item['origin_url'] = response.urljoin(detail)
                yield item
        # 下一页
        next_url = response.xpath("//div[@class='page_al']/p/a/@href").get()
        if next_url:
            yield scrapy.Request(url=response.urljoin(next_url),
                                 callback=self.parse_esf,
                                 meta={'info': (province, city)}
                                 )
