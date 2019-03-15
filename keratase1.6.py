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

'''������վ����������Ʒ����ȡ������Ʒ����
      https://www.kerastase-usa.com/collections

    ��ȡ������ҳ֮�󣬣��ڻ�ȡ��ͬ������Ʒ�µĲ�Ʒ
  # https://www.kerastase-usa.com/shampoos  ϴ��ˮ
  # https://www.kerastase-usa.com/conditioners  ������
  # https://www.kerastase-usa.com/styling  ����

  ��ȡ��Ʒ���飬������ҳ����ȡ�������ݣ�Ʒ��logo��Ʒ�����ơ���Ʒ���ࡢ��Ʒͼ�ȵȣ�


  '''


class Spider_kerastase(object):

    def __init__(self):
        '''��ʼ����������ͷ�������ַ'''
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36",
        }
        self.proxies = [
            {'https': '183.128.243.81:6666'},
            {'https': '121.231.168.5:6666'},
            {'https': '121.231.168.123:6666'},
        ]
        self.proxy = self.proxies[random.randint(0, 2)]  # ��������IP
        self.start_url = 'https://www.kerastase-usa.com/collections'  # �������ӿ�
        self.url_header = 'https://www.kerastase-usa.com/{}'

    def initField(self):
        '''
        �������ݱ������ֶ�
        :return:
        '''
        self.db = pymysql.connect("112.124.58.109", "flypig", "****", "bigdt")
        self.cursor = self.db.cursor()
        # self.cursor.execute("DROP TABLE IF EXISTS product_list")  # ��ձ�����
        # self.cursor.execute("DROP TABLE IF EXISTS brand_list")  # ��ձ�����
        # ���ݱ����1.0 https://shimo.im/sheets/75iuC0WMyNs7btRs/MODOC
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS product_list(
            produce_id int AUTO_INCREMENT primary key COMMENT '��Ʒ���',
            FK_brand_id int NOT NULL COMMENT 'Ʒ�����' ,
            product_name VARCHAR(80) NOT NULL COMMENT '��Ʒ����'  ,
            product_type  VARCHAR(40) NOT NULL DEFAULT 'NULL'  COMMENT '��Ʒ����',
            img_src longblob COMMENT '��ƷͼƬ' ,
            product_description LONGTEXT  COMMENT '��Ʒ����' ,
            how_to_use LONGTEXT  COMMENT 'ʹ�÷���',
            volumetric CHAR(40) COMMENT '����' ,
            price FLOAT DEFAULT 0.0 COMMENT '�۸�' ,
            sale INT  UNSIGNED COMMENT '����',
            spider_time datetime COMMENT '��ȡʱ��',
            is_delete BOOLEAN default 0  COMMENT '�߼�ɾ��'
           )COMMENT='��Ʒ��' DEFAULT CHARSET=utf8;
        """
        ALTER_FK_product_BRANDID = '''
          ALTER TABLE product_list ADD CONSTRAINT FK_ID FOREIGN KEY(Fk_brand_id) REFERENCES brand_list(brand_id);
        '''
        create_table_brand_sql = '''
            CREATE TABLE IF NOT EXISTS brand_list(
                brand_id int AUTO_INCREMENT primary key COMMENT 'Ʒ�Ʊ��',
                brand_logo TEXT NOT NULL COMMENT 'Ʒ��logo'  ,
                brand_name  VARCHAR(40) NOT NULL UNIQUE DEFAULT 'NULL'  COMMENT 'Ʒ������' ,
                brand_origin VARCHAR(40) NOT NULL DEFAULT 'NULL' COMMENT 'Ʒ�Ʒ�Դ��',
                spider_time datetime COMMENT '��ȡʱ��',
                is_delete BOOLEAN default 0  COMMENT '�߼�ɾ��'
               )COMMENT='Ʒ�Ʊ�' DEFAULT CHARSET=utf8;
        '''
        try:
            self.cursor.execute(create_table_sql)
            self.cursor.execute(create_table_brand_sql)
            self.cursor.execute(ALTER_FK_product_BRANDID)  #������
            self.db.commit()
            print('----�������ݱ�ɹ�-----')
        except:
            self.db.rollback()
            print('----�������ݱ�ʧ��----')

        finally:
            self.cursor.close()

    def get_productList(self):
        '''
        ������Ʒ�����б�
        :return:
        '''
        response = requests.get(self.start_url, self.headers)  # ������Ʒ��������
        html = response.content.decode()
        html_class = etree.HTML(html)
        class_product = html_class.xpath(".//div[@class='categories']/div")
        for class_pro in class_product:
            pros = class_pro.xpath('./a/@href')
            if pros:
                self.get_url(pros[0])  # ����Ʒ�����б�����
                # time.sleep(2)

    def get_url(self, pro_url):
        '''
        ��ȡ��Ʒ����ҳ�棬������Ʒ����
        :param pro_url:   ��Ʒ��������
        :return:
        '''
        response = requests.get(pro_url, self.headers)  # ������Ʒ��������
        html = response.content.decode()
        html_class = etree.HTML(html)

        '''
        https://www.kerastase-usa.com/collections/discipline?sz=10&start=10&format=ajax&lazy=trueg
        https://www.kerastase-usa.com/collections/nutritive?sz=13&start=13&format=ajax&lazy=true
        '''

        products = html_class.xpath(".//div[@class='product_tile_wrapper b-product_tile-wrapper']")
        for pro in products:
            p = pro.xpath('./div/a/@href')[0]
            self.get_data(self.url_header.format(p))  # ��ȡÿһ����Ʒ������

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
            url_ajax = html.xpath('//div//a/@href')  # Ajax�е�������Ʒ����
            print("url_ajax:", url_ajax)
            if url_ajax:
                for url in url_ajax:
                    self.get_data(self.url_header.format(url))

    def rate_change(self):
        '''
        ת�����ʣ�ͳһ����������ת���������(Ŀǰ�����趨Ϊ��Ԫת�����)
        ������վhttp://quote.forex.hexun.com/USDCNY.shtml
        ����APIΪ��http://webforex.hermes.hexun.com/forex/quotelist?code=FOREXUSDCNY&column=Code,Name,DateTime,Price,Amount,Volume,LastClose,Open,High,Low,UpDown,UpDownRate,Speed,PriceWeight,AveragePrice,OpenTime,CloseTime,EntrustRatio,EntrustDiff,OutVolume,InVolume,ExchangeRatio,TotalPrice,LastSettle,SettlePrice,BuyPrice,BuyVolume,SellPrice,SellVolume,VolumeRatio,PE,LastVolume,LastCount,LastInOut,VibrationRatio,Total,DealCount,OpenPosition,ClosePosition,PositionDiff,LastPositions,AddPosition,OpenInterest
        ϡ�ͳ���Ҫ����Ԫ���ʲ���   http://webforex.hermes.hexun.com/forex/quotelist?code=FOREXUSDCNY&column=Code,Price
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
        # ����Ʒ����Ϣ
        r = requests.get(brand_logo)
        with open('F:\python_project\kerastase\logo_{}.svg'.format(brand_name), 'wb') as f:
            f.write(r.content)
        f = open('F:\python_project\kerastase\logo_{}.svg'.format(brand_name), 'rb')
        brand_logo = f.read()
        f.close()
        brand_logo = str(base64.b64encode(brand_logo))  # ��ֹ�����ݿ�ת��
        # �����Ʒ��Ϣ
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
            print('----�洢Ʒ��{}���ݳɹ�-------'.format(brand_name))
        except:
            self.db.rollback()
            print('----�洢Ʒ��{}����ʧ��-------'.format(brand_name))
        finally:
            self.cursor.close()

    def get_data(self, details_url):
        '�������󣬻�ȡÿһ����Ʒҳ��������ȡҳ������'
        # response = requests.get(details_url, self.headers)
        # html = response.content.decode()
        # html_data = etree.HTML(html)
        '''
        ��ȡ����������ԭ�򣺷���
        ���������selenium���ߴ�cookie
        '''
        print('������ȡ��Ʒ{}'.format(details_url))
        options = webdriver.ChromeOptions()
        options.add_argument(
            'user-agent="Mozilla/5.0 (iPod; U; CPU iPhone OS 2_1 like Mac OS X; ja-jp) AppleWebKit/525.18.1 (KHTML, like Gecko) Version/3.1.1 Mobile/5F137 Safari/525.20"')
        # options.add_argument('--dns-prefetch-disable')
        browser = webdriver.Chrome(chrome_options=options)
        try:
            browser.get(details_url)
            browser.implicitly_wait(30)
            html = browser.page_source
        except:  # ��ʱ����TimeoutException
            browser.refresh()
        # print(html)
        html = etree.HTML(html)
        time.sleep(5)
        browser.quit()  # �˳������
        # ��Ҫ��ȡ�ֶ�
        # brand_logo �� brand_name��  product_story�� produce_img��  produce_name��  volumetric��  desscribe�� howtouse��  price�� saled

        # ʶ��Ʒ�ƻ�ȡƷ�Ʊ��
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
            # �����һ����ȡ��Ʒ��,�ʹ洢Ʒ����Ϣ
            if len(find_brand_result) == 0:
                self.insert_brand(brand_name, brand_logo)


        self.cursor = self.db.cursor()
        try:
            self.cursor.execute('SELECT BRAND_ID FROM brand_list WHERE brand_name=(%s)', (brand_name))
            result = self.cursor.fetchall()
        except:
            print('-----��ȡ��Ʒ���ʧ��-----')
            self.db.rollback()
        else:
            Fk_sign_id = result[0][0]
        finally:
            self.cursor.close()
        # time.sleep(2)

        product_name = html.xpath('//h1[@class="product_name product__name"]/text()')[0].strip()  # ��Ʒ����
        try:
            self.cursor.execute("SELECT * FROM product_list WHERE product_name=(%s)", (product_name))
            find_product_result = self.cursor.fetchall()
        except:
            self.db.rollback()
        else:
            # �Ѿ���ȡ������Ʒ
            if len(find_product_result) != 0:
                return


        product_type = html.xpath('//p[@class="regimen"]/text()')  # ��Ʒ����
        if product_type:
            product_type = product_type[0]
        img_src = html.xpath('//img[@class="primary_image product_image   b-product_img"]/@src')[
            0]  # ��ƷͼƬ  (�����ƷͼƬ��������ǿհ�ͼƬ)
        product_description = html.xpath('//h2[@class="product_subtitle "]/text()')[0].strip()  # ����
        how_to_use = html.xpath('//*[@id="tab_tips"]/p/text()')  # ʹ�÷���
        if how_to_use == []:  # ƥ����Ƶʹ�÷���
            how_to_use = html.xpath('//div[@class="how-to-use-video-copy"]/text()')
        if how_to_use == []:
            how_to_use = html.xpath('//div[@class="how-to-use-copy"]/text()')
        if how_to_use:
            how_to_use = how_to_use[0].strip()
        volumetric = html.xpath('//span[@class="quantity-of-product"]/text()')[0].strip()[2:]  # ����
        price = html.xpath("//p[@class='product_price price_sale b-product_price-sale b-product_price']/text()")[
            1].strip()  # �۸�
        sale = html.xpath("//button[@class='bv_numReviews_text']/text()")  # ������
        if sale:
            sale = sale[0][1:-1]

        # ����  (��չ���ݱ���������+���۵ȼ�+(������+����ʱ��))
        '''
        https://www.kerastase-usa.com/collections/discipline/bain-fluidealiste-original-shampoo.html#tab_reviews
        ��Ʒ����+#tab_reviews
        '''

        # �������ݿ��ֶ�
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
        # ��������
        '''
        ���ݿ����洢��ҳ�����������
        :param data_dict:
        :return:
        '''

        # ����ͼƬ������ͼƬ
        r = requests.get(data_dict['img_src'])
        with open('F:\python_project\kerastase\{}.jpg'.format(data_dict['product_name']), 'wb') as f:
            f.write(r.content)
        f = open('F:\python_project\kerastase\{}.jpg'.format(data_dict['product_name']), 'rb')
        data_dict['img_src'] = f.read()
        f.close()
        data_dict['img_src'] = str(base64.b64encode(data_dict['img_src']))  # ��ֹ�����ݿ�ת��



        # �����ʺ�����
        if data_dict['how_to_use'] == []:
            data_dict['how_to_use'] = "NULL"
        if data_dict['product_description'] == []:
            data_dict['product_description'] = "NULL"
        if data_dict['product_type'] == []:
            data_dict['product_type'] = "NULL"
        if data_dict['sale'] == []:
            data_dict['sale'] = 0
        # ���ʴ���
        if data_dict['price']:
            data_dict['price'] = data_dict['price'] * self.rate_change()

        # ��ӡ�������
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

        # �����Ʒ��Ϣ
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
            print('----�洢{}���ݳɹ�-------'.format(data_dict['product_name']))
        except:
            self.db.rollback()
            print('----�洢{}����ʧ��-------'.format(data_dict['product_name']))
        finally:
            self.cursor.close()

    def run(self):
        # �������ݱ�
        self.initField()
        # ��������
        self.get_productList()
        # ����ajax
        self.patch_ajax()

        # ����
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
    kerastase.db.close()  # �ر����ݿ�
    print('-------������ɣ���ȡ�ɹ�------')



