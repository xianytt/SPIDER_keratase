# -*- coding:utf-8 -*-
# -*- coding:utf-8 -*-
import requests
from lxml import etree
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import selenium
import datetime
import base64
import pymysql
import random
import json
import re
import urllib.request

'''分析网站，从所有商品中爬取分类商品总览
      https://www.kerastase-usa.com/collections

    获取分类网页之后，，在获取不同分类商品下的产品
  # https://www.kerastase-usa.com/shampoos  洗发水
  # https://www.kerastase-usa.com/conditioners  护发素
  # https://www.kerastase-usa.com/styling  发胶

  获取产品详情，分析网页，获取所需数据（品牌logo、品牌名称、产品分类、产品图等等）


  '''


class Spider_kerastase(object):

    def __init__(self):
        '''初始化设置请求头和请求地址'''
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36",
        }
        self.proxies = [
            {'https': '183.128.243.81:6666'},
            {'https': '121.231.168.5:6666'},
            {'https': '121.231.168.123:6666'},
        ]
        self.proxy = self.proxies[random.randint(0, 2)]  # 创建代理IP
        self.start_url = 'https://www.kerastase-usa.com/collections'  # 爬虫进入接口
        self.url_header = 'https://www.kerastase-usa.com/{}'

    def initField(self):
        '''
        创建数据表，定义字段
        :return:
        '''
        self.db = pymysql.connect("112.124.58.109", "flypig", "****", "bigdt")
        self.cursor = self.db.cursor()
        # self.cursor.execute("DROP TABLE IF EXISTS product_list")  # 清空表数据
        # self.cursor.execute("DROP TABLE IF EXISTS brand_list")  # 清空表数据
        # 数据表分析1.0 https://shimo.im/sheets/75iuC0WMyNs7btRs/MODOC
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS product_list(
            produce_id int AUTO_INCREMENT primary key COMMENT '产品编号',
            FK_brand_id int NOT NULL COMMENT '品牌外键' ,
            product_name VARCHAR(80) NOT NULL COMMENT '产品名称'  ,
            product_type  VARCHAR(40) NOT NULL DEFAULT 'NULL'  COMMENT '产品分类',
            img_src longblob COMMENT '产品图片' ,
            product_description LONGTEXT  COMMENT '产品描述' ,
            how_to_use LONGTEXT  COMMENT '使用方法',
            volumetric CHAR(40) COMMENT '容量' ,
            price FLOAT DEFAULT 0.0 COMMENT '价格' ,
            sale INT  UNSIGNED COMMENT '销量',
            spider_time datetime COMMENT '爬取时间',
            is_delete BOOLEAN default 0  COMMENT '逻辑删除'
           )COMMENT='产品表' DEFAULT CHARSET=utf8;
        """
        ALTER_FK_product_BRANDID = '''
          ALTER TABLE product_list ADD CONSTRAINT FK_ID FOREIGN KEY(Fk_brand_id) REFERENCES brand_list(brand_id);
        '''
        create_table_brand_sql = '''
            CREATE TABLE IF NOT EXISTS brand_list(
                brand_id int AUTO_INCREMENT primary key COMMENT '品牌编号',
                brand_logo TEXT NOT NULL COMMENT '品牌logo'  ,
                brand_name  VARCHAR(40) NOT NULL UNIQUE DEFAULT 'NULL'  COMMENT '品牌名称' ,
                brand_origin VARCHAR(40) NOT NULL DEFAULT 'NULL' COMMENT '品牌发源地',
                spider_time datetime COMMENT '爬取时间',
                is_delete BOOLEAN default 0  COMMENT '逻辑删除'
               )COMMENT='品牌表' DEFAULT CHARSET=utf8;
        '''
        try:
            self.cursor.execute(create_table_sql)
            self.cursor.execute(create_table_brand_sql)
            self.cursor.execute(ALTER_FK_product_BRANDID)  #添加外键
            self.db.commit()
            print('----创建数据表成功-----')
        except:
            self.db.rollback()
            print('----创建数据表失败----')

        finally:
            self.cursor.close()

    def get_productList(self):
        '''
        搭建爬虫产品分类列表
        :return:
        '''
        response = requests.get(self.start_url, self.headers)  # 构建产品分类链接
        html = response.content.decode()
        html_class = etree.HTML(html)
        class_product = html_class.xpath(".//div[@class='categories']/div")
        for class_pro in class_product:
            pros = class_pro.xpath('./a/@href')
            if pros:
                self.get_url(pros[0])  # 打开商品分类列表链接
                # time.sleep(2)

    def get_url(self, pro_url):
        '''
        爬取商品分类页面，构建商品链接
        :param pro_url:   产品分类链接
        :return:
        '''
        response = requests.get(pro_url, self.headers)  # 构建产品分类链接
        html = response.content.decode()
        html_class = etree.HTML(html)

        '''
        https://www.kerastase-usa.com/collections/discipline?sz=10&start=10&format=ajax&lazy=trueg
        https://www.kerastase-usa.com/collections/nutritive?sz=13&start=13&format=ajax&lazy=true
        '''

        products = html_class.xpath(".//div[@class='product_tile_wrapper b-product_tile-wrapper']")
        for pro in products:
            p = pro.xpath('./div/a/@href')[0]
            self.get_data(self.url_header.format(p))  # 获取每一个商品的链接

    def patch_ajax(self):
        # url_start = 'https://www.kerastase-usa.com'
        url_list = ['https://www.kerastase-usa.com/collections/discipline?sz=10&start=10&format=ajax&lazy=true',
                    'https://www.kerastase-usa.com/collections/nutritive?sz=13&start=13&format=ajax&lazy=true',
                    'https://www.kerastase-usa.com/collections/densifique?sz=10&start=10&format=ajax&lazy=true',
                    'https://www.kerastase-usa.com/collections/densifique?sz=10&start=10&format=ajax&lazy=true',
                    'https://www.kerastase-usa.com/collections/resistance?sz=16&start=16&format=ajax&lazy=true',
                    'https://www.kerastase-usa.com/collections/specifique?sz=9&start=9&format=ajax&lazy=true',
                    'https://www.kerastase-usa.com/styling?sz=16&start=16&format=ajax&lazy=true',
                    'https://www.kerastase-usa.com/styling?sz=16&start=32&format=ajax&lazy=true',
                    'https://www.kerastase-usa.com/styling?sz=16&start=48&format=ajax&lazy=true',
                    'https://www.kerastase-usa.com/styling?sz=16&start=64&format=ajax&lazy=true',
                    'https://www.kerastase-usa.com/collections/elixir-ultime?sz=10&start=10&format=ajax&lazy=true',
                    ]
        for url in url_list:
            html = requests.get(url, headers=self.headers)
            html = etree.HTML(html.text)
            url_ajax = html.xpath('//div//a/@href')  # Ajax中的所有商品链接
            print("url_ajax:", url_ajax)
            if url_ajax:
                for url in url_ajax:
                    self.get_data(self.url_header.format(url))

    def rate_change(self):
        '''
        转换汇率，统一将其他货币转换成人民币(目前参数设定为美元转人民币)
        分析网站http://quote.forex.hexun.com/USDCNY.shtml
        监视API为：http://webforex.hermes.hexun.com/forex/quotelist?code=FOREXUSDCNY&column=Code,Name,DateTime,Price,Amount,Volume,LastClose,Open,High,Low,UpDown,UpDownRate,Speed,PriceWeight,AveragePrice,OpenTime,CloseTime,EntrustRatio,EntrustDiff,OutVolume,InVolume,ExchangeRatio,TotalPrice,LastSettle,SettlePrice,BuyPrice,BuyVolume,SellPrice,SellVolume,VolumeRatio,PE,LastVolume,LastCount,LastInOut,VibrationRatio,Total,DealCount,OpenPosition,ClosePosition,PositionDiff,LastPositions,AddPosition,OpenInterest
        稀释出需要的美元汇率参数   http://webforex.hermes.hexun.com/forex/quotelist?code=FOREXUSDCNY&column=Code,Price
        :return:
        '''
        url = "http://webforex.hermes.hexun.com/forex/quotelist?code=FOREXUSDCNY&column=Code,Price"
        req = urllib.request.Request(url)
        f = urllib.request.urlopen(req)
        html = f.read().decode("utf-8")
        # print(html)

        s = re.findall("{.*}", str(html))[0]
        sjson = json.loads(s)

        USDCNY = sjson["Data"][0][0][1] / 10000
        return USDCNY

    def insert_brand(self, brand_name, brand_logo):
        # 保存品牌信息
        r = requests.get(brand_logo)
        with open('F:\python_project\kerastase\logo_{}.svg'.format(brand_name), 'wb') as f:
            f.write(r.content)
        f = open('F:\python_project\kerastase\logo_{}.svg'.format(brand_name), 'rb')
        brand_logo = f.read()
        f.close()
        brand_logo = str(base64.b64encode(brand_logo))  # 防止被数据库转码
        # 插入产品信息
        insert_brand_sql = """
                                INSERT INTO brand_list(brand_logo, brand_name, brand_origin, spider_time)
                                VALUES(%s, %s, %s, %s)
                                """
        values = (brand_logo, pymysql.escape_string(brand_name), pymysql.escape_string('USA'),
                  pymysql.escape_string(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        self.cursor = self.db.cursor()
        try:
            self.cursor.execute(insert_brand_sql, values)
            self.db.commit()
            print('----存储品牌{}数据成功-------'.format(brand_name))
        except:
            self.db.rollback()
            print('----存储品牌{}数据失败-------'.format(brand_name))
        finally:
            self.cursor.close()

    def get_data(self, details_url):
        '发送请求，获取每一个商品页面内容提取页面数据'
        # response = requests.get(details_url, self.headers)
        # html = response.content.decode()
        # html_data = etree.HTML(html)
        '''
        获取不到销量的原因：反爬
        解决方案：selenium或者带cookie
        '''
        print('正在爬取商品{}'.format(details_url))
        options = webdriver.ChromeOptions()
        options.add_argument(
            'user-agent="Mozilla/5.0 (iPod; U; CPU iPhone OS 2_1 like Mac OS X; ja-jp) AppleWebKit/525.18.1 (KHTML, like Gecko) Version/3.1.1 Mobile/5F137 Safari/525.20"')
        # options.add_argument('--dns-prefetch-disable')
        browser = webdriver.Chrome(chrome_options=options)
        try:
            browser.get(details_url)
            browser.implicitly_wait(30)
            html = browser.page_source
        except:  # 超时处理TimeoutException
            browser.refresh()
        # print(html)
        html = etree.HTML(html)
        time.sleep(5)
        browser.quit()  # 退出浏览器
        # 需要爬取字段
        # brand_logo 、 brand_name、  product_story、 produce_img、  produce_name、  volumetric、  desscribe、 howtouse、  price、 saled

        # 识别品牌获取品牌编号
        brand_logo = html.xpath('//img[@class="logo_image class.header.logoimage"]/@src')
        brand_logo = 'https://www.kerastase-usa.com{}'.format(brand_logo[0])
        brand_name = html.xpath("//span[@class='logo_text class.header.logotext']/text()")[0]
        self.cursor = self.db.cursor()
        try:
            self.cursor.execute("SELECT * FROM brand_list WHERE brand_name=(%s)", (brand_name))
            find_brand_result = self.cursor.fetchall()
        except:
            self.db.rollback()
        else:
            # 如果第一次爬取该品牌,就存储品牌信息
            if len(find_brand_result) == 0:
                self.insert_brand(brand_name, brand_logo)


        self.cursor = self.db.cursor()
        try:
            self.cursor.execute('SELECT BRAND_ID FROM brand_list WHERE brand_name=(%s)', (brand_name))
            result = self.cursor.fetchall()
        except:
            print('-----获取产品编号失败-----')
            self.db.rollback()
        else:
            Fk_sign_id = result[0][0]
        finally:
            self.cursor.close()
        # time.sleep(2)

        product_name = html.xpath('//h1[@class="product_name product__name"]/text()')[0].strip()  # 产品名称
        try:
            self.cursor.execute("SELECT * FROM product_list WHERE product_name=(%s)", (product_name))
            find_product_result = self.cursor.fetchall()
        except:
            self.db.rollback()
        else:
            # 已经爬取过该商品
            if len(find_product_result) != 0:
                return


        product_type = html.xpath('//p[@class="regimen"]/text()')  # 产品分类
        if product_type:
            product_type = product_type[0]
        img_src = html.xpath('//img[@class="primary_image product_image   b-product_img"]/@src')[
            0]  # 产品图片  (扩充产品图片表，用来标记空白图片)
        product_description = html.xpath('//h2[@class="product_subtitle "]/text()')[0].strip()  # 描述
        how_to_use = html.xpath('//*[@id="tab_tips"]/p/text()')  # 使用方法
        if how_to_use == []:  # 匹配视频使用方法
            how_to_use = html.xpath('//div[@class="how-to-use-video-copy"]/text()')
        if how_to_use == []:
            how_to_use = html.xpath('//div[@class="how-to-use-copy"]/text()')
        if how_to_use:
            how_to_use = how_to_use[0].strip()
        volumetric = html.xpath('//span[@class="quantity-of-product"]/text()')[0].strip()[2:]  # 容量
        price = html.xpath("//p[@class='product_price price_sale b-product_price-sale b-product_price']/text()")[
            1].strip()  # 价格
        sale = html.xpath("//button[@class='bv_numReviews_text']/text()")  # 销售量
        if sale:
            sale = sale[0][1:-1]

        # 评价  (扩展数据表：评价内容+评价等级+(评价人+评价时间))
        '''
        https://www.kerastase-usa.com/collections/discipline/bain-fluidealiste-original-shampoo.html#tab_reviews
        商品链接+#tab_reviews
        '''

        # 构造数据库字段
        data_dict = dict(
            product_name=product_name,
            brand_name=brand_name,
            brand_logo=brand_logo,
            Fk_sign_id=Fk_sign_id,
            product_type=product_type,
            img_src=img_src,
            product_description=product_description,
            how_to_use=how_to_use,
            volumetric=volumetric,
            price=float(price[1:]),
            sale=sale,
            spider_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        )
        self.save_data(data_dict)

    def save_data(self, data_dict):
        # 保存数据
        '''
        数据库语句存储网页处理特殊符号
        :param data_dict:
        :return:
        '''

        # 下载图片并保存图片
        r = requests.get(data_dict['img_src'])
        with open('F:\python_project\kerastase\{}.jpg'.format(data_dict['product_name']), 'wb') as f:
            f.write(r.content)
        f = open('F:\python_project\kerastase\{}.jpg'.format(data_dict['product_name']), 'rb')
        data_dict['img_src'] = f.read()
        f.close()
        data_dict['img_src'] = str(base64.b64encode(data_dict['img_src']))  # 防止被数据库转码



        # 处理不适合数据
        if data_dict['how_to_use'] == []:
            data_dict['how_to_use'] = "NULL"
        if data_dict['product_description'] == []:
            data_dict['product_description'] = "NULL"
        if data_dict['product_type'] == []:
            data_dict['product_type'] = "NULL"
        if data_dict['sale'] == []:
            data_dict['sale'] = 0
        # 汇率处理
        if data_dict['price']:
            data_dict['price'] = data_dict['price'] * self.rate_change()

        # 打印输出调试
        print(data_dict["product_name"])
        print(data_dict["product_type"])
        print(data_dict['Fk_sign_id'])
        print(data_dict["img_src"])
        print(data_dict["product_description"])
        print(data_dict["how_to_use"])
        print(data_dict["volumetric"])
        print(data_dict["price"])
        print(data_dict["sale"])
        print(data_dict["spider_time"])

        # 插入产品信息
        insert_product_sql = """
                        INSERT INTO product_list(product_name, product_type,Fk_sign_id, img_src, product_description, how_to_use, volumetric, price,sale, spider_time)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
        values = (pymysql.escape_string(data_dict['product_name']), pymysql.escape_string(data_dict['product_type']),
                  data_dict['Fk_sign_id'], data_dict['img_src'], pymysql.escape_string(data_dict['product_description']),
                  data_dict['how_to_use'],
                  pymysql.escape_string(data_dict['volumetric']), data_dict['price'],
                  data_dict['sale'], datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        self.cursor = self.db.cursor()

        # self.cursor.execute(insert_product_sql, values)
        # self.db.commit()
        # self.cursor.close()

        try:
            self.cursor.execute(insert_product_sql, values)
            self.db.commit()
            print('----存储{}数据成功-------'.format(data_dict['product_name']))
        except:
            self.db.rollback()
            print('----存储{}数据失败-------'.format(data_dict['product_name']))
        finally:
            self.cursor.close()

    def run(self):
        # 创建数据表
        self.initField()
        # 发送请求
        self.get_productList()
        # 补丁ajax
        self.patch_ajax()

        # 测试
        # self.get_data('https://www.kerastase-usa.com//collections/discipline/fluidissime-anti-frizz-spray.html')
        # self.insert_brand('kerastase','https://www.kerastase-usa.com/on/demandware.static/Sites-kerastase-us-Site/-/default/dwb0e354d7/images/logo.svg')
        # self.get_url('https://www.kerastase-usa.com/collections/discipline')
        # self.get_data('https://www.kerastase-usa.com/collections/discipline/bain-fluidealiste-original-shampoo.html#start=1&cgid=discipline')
        # self.get_data('https://www.kerastase-usa.com//collections/discipline/fondant-fluidealiste-conditioner.html')
        # self.get_data('https://www.kerastase-usa.com/collections/nutritive/nectar-thermique-blow-dry-primer.html#start=1&cgid=nutritive')
        # self.get_data('https://www.kerastase-usa.com/collections/discipline/fondant-fluidealiste-conditioner.html#start=3&cgid=discipline')
        # self.get_data('https://www.kerastase-usa.com//collections/aura-botanica/coup-de-coeur-aura-botanica-luxury-gift-set.html')
        # self.get_data(
        #     'https://www.kerastase-usa.com//collections/aura-botanica/coup-de-coeur-aura-botanica-luxury-gift-set.html')
        # self.more('https://www.kerastase-usa.com/collections/aura-botanica')


if __name__ == '__main__':
    kerastase = Spider_kerastase()
    kerastase.run()
    kerastase.db.close()  # 关闭数据库
    print('-------程序完成，爬取成功------')



