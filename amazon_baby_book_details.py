#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2016-10-18 21:19:23
# Project: AmazonBabyBookDetails

from pyspider.libs.base_handler import *
import re
import mysql.connector
import time, threading
import random

PAGE_START = 1
PAGE_END = 75
BOOK_PATH = "/home/allen/Public/babybooks(3-6).txt"

# 这个不行的原因是少了div.a-fixed-left-grid-inner这一环
DOC_SELECTOR = 'div.rightContainerATF > div.rightResultsATF > div.resultsCol > div.centerMinus > div.atfResults > ul.s-results-list-atf > li > div.s-item-container > div.a-fixed-left-grid > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > a'

DOC_SELECTOR2 = 'body > div.a-page > div.searchTemplate > div.rightContainerATF > div.rightResultsATF > div.resultsCol > div.centerMinus > div.atfResults> ul.s-results-list-atf > li > div.s-item-container > div.a-fixed-left-grid > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > a'

DOC_SELECTOR3 = '#div.resultsCol > div.centerMinus > div.atfResults > ul > li'

DOC_SELECTOR4 = 'body > div.a-page > div.searchTemplate > div.rightContainerATF > div.rightResultsATF > div.resultsCol > div.centerMinus > div.atfResults> ul.s-results-list-atf > li'

DOC_SELECTOR5 = '#body > div.page'

DOC_SELECTOR6 = 'div.rightContainerATF > div.rightResultsATF > div.resultsCol > div.centerMinus > div.atfResults > ul.s-results-list-atf > li > div.s-item-container > div.a-fixed-left-grid > div.a-fixed-left-grid-inner > div.a-col-right > div.a-spacing-small > a'

# DOC_SELECTOR7被证明是有效的
DOC_SELECTOR7 = 'a.a-link-normal.s-access-detail-page.a-text-normal'
# DOC_SELECTOR8也被证明有效
DOC_SELECTOR8 = 'div.a-row.a-spacing-small > a.a-link-normal.s-access-detail-page.a-text-normal'
# DOC_SELECTOR9也被证明有效
DOC_SELECTOR9 = 'div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > a'
# DOC_SELECTOR10有效
DOC_SELECTOR10 = 'div.a-fixed-left-grid-inner > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > a'
# DOC_SELECTOR11有效
DOC_SELECTOR11 = 'div.a-fixed-left-grid > div.a-fixed-left-grid-inner > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > a'
# DOC_SELECTOR12有效
DOC_SELECTOR12 = 'div.s-item-container > div.a-fixed-left-grid > div.a-fixed-left-grid-inner > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > a'

# DOC_SELECTOR13有效
DOC_SELECTOR13 = 'li > %s' % DOC_SELECTOR12

# DOC_SELECTOR14无效,原因未知
DOC_SELECTOR14 = 'ul.s-results-list-atf > %s' % DOC_SELECTOR13

# DOC_SELECTOR15有效
DOC_SELECTOR15 = 'ul > %s' % DOC_SELECTOR13

# DOC_SELECTOR16无效,原因未知,不过应该是div#atfResults写成了div.atfResults导致的,到底应该写成div.还是div#,在chrome右下方的标签可以看出来
DOC_SELECTOR16 = 'div.atfResults > %s' % DOC_SELECTOR15

# DOC_SELECTOR17有效
DOC_SELECTOR17 = 'div#atfResults.a-row.s-result-list-parent-container > %s' % DOC_SELECTOR15
# DOC_SELECTOR18有效,说明#是关键
# 比较发现,.对应class如div class="table"要写成div.table;而#对应id之类,如<div id="detail_bullets_id"要写成div#detail_bullets_id
DOC_SELECTOR18 = 'div#atfResults > %s' % DOC_SELECTOR15
# DOC_SELECTOR19有效
DOC_SELECTOR19 = 'div#centerMinus > %s' % DOC_SELECTOR18
# DOC_SELECTOR20有效
DOC_SELECTOR20 = 'div#resultsCol > %s' % DOC_SELECTOR19
# DOC_SELECTOR21有效
DOC_SELECTOR21 = 'div#rightResultsATF > %s' % DOC_SELECTOR20
# DOC_SELECTOR22有效
DOC_SELECTOR22 = 'div#rightContainerATF > %s' % DOC_SELECTOR21

# DOC_SELECTOR23有效
DOC_SELECTOR23 = 'div#searchTemplate > %s' % DOC_SELECTOR22

# DOC_SELECTOR24有效
DOC_SELECTOR24 = 'div#main > %s' % DOC_SELECTOR23

# DOC_SELECTOR25有效
DOC_SELECTOR25 = 'div#a-page > %s' % DOC_SELECTOR24

# DOC_SELECTOR26有效
DOC_SELECTOR26 = 'body > %s' % DOC_SELECTOR25

DETAIL_BASE_SELECTOR = 'body > div#a-page > div#dp-container > div#descriptionAndDetails > div#productDescription_feature_div > div#productDescription_feature_div > div#s_wrap > div#s_center'

DETAIL_MENU_SELECTOR = '%s > div#s_menu >a' % DETAIL_BASE_SELECTOR

# DETAIL_CONTENT_KEY_SELECTOR='%s > div#s_contents > div > h3' % DETAIL_BASE_SELECTOR

# DETAIL_CONTENT_VALUE_SELECTOR='%s > div#s_contents > div > p' % DETAIL_BASE_SELECTOR

# 上面那两个也可以，不过这个是更简洁的形式
DETAIL_CONTENT_KEY_SELECTOR = 'div#s_contents > div > h3'

DETAIL_CONTENT_VALUE_SELECTOR = 'div#s_contents > div > p'

# 这个无效,原因是chrome对tbody的渲染支持不好
BASIC_INFO_SELECTOR = 'body > div#a-page > div#dp-container > div#detail_bullets_id > table > tbody > tr > td.bucket > div.content > ul > li'

BASIC_INFO_KEY_SELECTOR = '%s > b' % BASIC_INFO_SELECTOR

# 这个有效
BASIC_INFO_SELECTOR2 = 'td.bucket > div.content > ul > li'

# 这个有效
BASIC_INFO_SELECTOR3 = 'tr > %s' % BASIC_INFO_SELECTOR2

# 这个无效,原因是在chrome 中 对tbody 的渲染支持不好， 因而css 选择器参数中 请不要使用 tbody 标签进行筛选
BASIC_INFO_SELECTOR4 = 'tbody > %s' % BASIC_INFO_SELECTOR3
# 这个有效
BASIC_INFO_SELECTOR5 = 'table > %s' % BASIC_INFO_SELECTOR3
# 这个无效,原因未知
BASIC_INFO_SELECTOR6 = 'div#detail_bullets_id > %s' % BASIC_INFO_SELECTOR5

BASE_TITLE_SELECTOR = 'div#booksTitle > div.a-section.a-spacing-none > h1#title'
TITLE_SELECTOR = '%s > span#productTitle' % BASE_TITLE_SELECTOR
# BOOK_SIZE_SELECTOR='%s > span.a-size-medium' % BASE_TITLE_SELECTOR


BASE_LAYOUT_SELECTOR = 'div#MediaMatrix.feature > div#formats > div#tmmSwatches > ul.a-nostyle > li.swatchElement > span.a-list-item > span#a-autoid-5 > span.a-button-inner > a#a-autoid-5-announce.a-button-text'

LAYOUT_SELECTOR = '%s > span' % BASE_LAYOUT_SELECTOR

# PRICE_SELECTOR不行,原因未知
PRICE_SELECTOR = '%s > span.a-color-base > span.a-color-price' % BASE_LAYOUT_SELECTOR

# PRICE_SELECTOR2有效
PRICE_SELECTOR2 = 'span.a-color-base > span.a-color-price'

# 既有class又有id的话,需要用class!所以这里需要使用a.a-button-text而不是a#a-autoid-5-announce.a-button-text
PRICE_SELECTOR3 = 'a.a-button-text > %s' % PRICE_SELECTOR2

BASE_AUTHOR_SELECTOR = 'div#a-page > div#dp-container > div#centerCol > div#booksTitle.feature > div#byline > span.author'

AUTHOR_SELECTOR = '%s > a.a-link-normal' % BASE_AUTHOR_SELECTOR
CONTRIBUTION_SELECTOR = '%s > span.contribution' % BASE_AUTHOR_SELECTOR

# 这个无效，这个是促销信息
BOOK_DESC_SELECTOR = 'div#a-page > div#dp-container > div#centerCol > div#heroQuickPromo_feature_div > div#hero-quick-promo'

BOOK_DESC_SELECTOR2 = 'div.maxReadableWidth > div.maxReadableWidth > iframe.ap_never_hide > html > body > div#iframeContent'

BOOK_DESC_SELECTOR3 = 'html > body > div#iframeContent'


class Handler(BaseHandler):
    crawl_config = {
    }

    def __init__(self):
        self.base_url = 'https://www.amazon.cn/s/ref=lp_2111930051_pg_2?rh=n%3A658390051%2Cn%3A%21658391051%2Cn%3A658409051%2Cp_n_age_range%3A2111930051&page={0}&ie=UTF8&qid=1476796480'
        self.page_num = PAGE_START
        self.total_num = PAGE_END
        self.bookList = []

    @every(minutes=24 * 60)
    def on_start(self):
        while self.page_num <= self.total_num:
            url = self.base_url.format(self.page_num)
            print url
            self.crawl(url, callback=self.index_page)
            self.page_num += 1

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        print "current page_num:%d" % self.page_num
        print "get response now"
        print DOC_SELECTOR7
        for each in response.doc(DOC_SELECTOR26).items():
            url = each.attr.href
            print url
            self.crawl(url, callback=self.detail_page)
            # for each in response.doc(DOC_SELECTOR3).items():
            # for each in response.doc('a[href^="http"]').items():
            # print "found one"
            # title=each.attr.title
            # link=each.attr.href
            # print "title:%s" % title
            # print "link:%s" % link
            # self.crawl(link,callback=self.deail_page)

    @config(priority=2)
    def detail_page(self, response):
        title = response.doc(TITLE_SELECTOR).text()
        link = response.url
        print "title:%s" % title
        print "link:%s" % link
        book = AmazonBook(title, link)

        # book.layout=response.doc(LAYOUT_SELECTOR).text()
        # print "book.layout:%s" % book.layout
        book.price = get_price(response)
        print "book price:%.2f" % book.price

        book.book_desc = response.doc(BOOK_DESC_SELECTOR3).text()
        print "book.desc:%s" % book.book_desc

        book.min_age = 3
        book.max_age = 6

        # fetch book details from detail page
        for menu in response.doc(DETAIL_MENU_SELECTOR).items():
            print 'menu:%s' % menu.text()
        for content_key in response.doc(DETAIL_CONTENT_KEY_SELECTOR).items():
            print 'content_key:%s' % content_key.text()
        for content_value in response.doc(DETAIL_CONTENT_VALUE_SELECTOR).items():
            print 'content_value:%s' % content_value.text()
            book.recommand_desc = content_value.text()
            break

        print "book.recommand_desc:%s" % book.recommand_desc

        authors = []
        for author in response.doc(AUTHOR_SELECTOR).items():
            authors.append(author.text())
            print 'author:%s' % author.text()

        contris = []
        for contribution in response.doc(CONTRIBUTION_SELECTOR).items():
            contris.append(contribution.text().encode('utf-8'))
            print 'contribution:%s' % contribution.text()

        book.author, book.translator = get_author_and_translator(authors, contris)

        print "book.author:%s" % book.author
        print "book.translator:%s" % book.translator

        print "now start to search basic info"

        for basic_info in response.doc(BASIC_INFO_SELECTOR5).items():
            # 注意此时content类型是unicode
            content = basic_info.text()
            # print type(content)
            content = content.encode('utf-8')
            print 'basic info:%s' % content
            if content.startswith('出版社'):
                # 将utf-8编码的str再转化为unicode
                content = content.decode('utf-8')
                book.press = content[4:]
                print "book press:%s" % book.press
            elif content.startswith('丛书名'):
                content = content.decode('utf-8')
                book.serial_name = content[4:]
                print "serial name:%s" % book.serial_name
            elif content.startswith('平装') or content.startswith('精装'):
                book.page_num = get_page_num(content)
                print book.page_num
                if content.startswith('平装'):
                    book.layout = '平装'
                else:
                    book.layout = '精装'
                print book.layout

            elif content.startswith('读者对象'):
                content = content.decode('utf-8')
                book.suit_reader = content[5:]
                print "suit reader:%s" % book.suit_reader
            elif content.startswith('开本'):
                book.book_size = get_book_size(content)
                print book.book_size
            elif content.startswith('ISBN'):
                book.isbn = content[5:]
                print "isbn:%s" % book.isbn
            elif content.startswith('商品尺寸'):
                book.length, book.width, book.thickness = get_book_dimension(content)
                print book.length, book.width, book.thickness
            elif content.startswith('商品重量'):
                book.weight = get_weight(content)
                print "book weight:%.2f" % book.weight
            elif content.startswith('品牌'):
                content = content.decode('utf-8')
                book.brand = content[4:]
                print "book brand:%s" % book.brand
            elif content.startswith('用户评分'):
                book.average_star = 4.2
                print "average star:%.1f" % book.average_star
            elif content.find('亚马逊热销商品排名') >= 0:

                book.rank = get_book_rank(content)
                print "book rank:%d" % book.rank

        insert_into_db(book)

        print "before sleep"
        time.sleep(random.randint(200, 500))
        print "after sleep"

        return {
            "url": response.url,
            "title": response.doc('title').text(),
        }


def insert_into_db(book):
    try:
        #注意将password的值替换为你自己的mysql密码
        conn = mysql.connector.connect(user='root', password='password', database='amazon_book')
        cursor = conn.cursor()
        cursor.execute(
            'insert into baby_book_info(isbn,title,link,author,translator,price,layout,recommand_desc,min_age,max_age,press,serial_name,page_num,suit_reader,book_size,length,width,thickness,weight,brand,average_star,book_rank) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
            [book.isbn, book.title, book.link, book.author, book.translator, str(book.price), book.layout,
             book.recommand_desc, str(book.min_age), str(book.max_age), book.press, book.serial_name,
             str(book.page_num), book.suit_reader, str(book.book_size), str(book.length), str(book.width),
             str(book.thickness), str(book.weight), book.brand, str(book.average_star), str(book.book_rank)])

        conn.commit()
        cursor.close()
    except:
        print "%s inseted failed." % book.title


def get_book_rank(content):
    return int(get_first_num_str(content))


def get_weight(content):
    r = re.compile('\d+[.]{0,1}\d{0,10}')
    m = r.findall(content)
    result = float(m[0])
    if (result > 100.0):
        return result / 100.0
    else:
        return result


def get_book_dimension(content):
    r = re.compile('\d+[.]{0,1}\d{0,10}')
    m = r.findall(content)
    return float(m[0]), float(m[1]), float(m[2])


def get_book_size(content):
    return int(get_first_num_str(content))


def get_page_num(content):
    return int(get_first_num_str(content))


def get_first_num_str(content):
    r = re.compile(r'\d+', re.S)
    m = r.search(content)
    try:
        return m.group(0)
    except:
        return '0'


def get_price(response):
    try:
        price_str = response.doc(PRICE_SELECTOR3).text()
        print price_str
        price_str = price_str[1:]
        return float(price_str)
    except:
        print 'price exception'
        return 0


def get_author_and_translator(authors, contris):
    index = 0
    author_result = ''
    translator_result = ''
    while index < len(contris):
        # contris[index]=contris[index].encode('utf-8')
        if (contris[index].find('作者') >= 0):
            if len(author_result) > 0:
                author_result += ','
            author_result += authors[index]
        else:
            if len(translator_result) > 0:
                translator_result += ','
            translator_result += authors[index]
        index += 1
    print author_result
    print translator_result
    return author_result, translator_result


class AmazonBook(object):
    def __init__(self, title, link):
        self.title = title
        self.link = link
        self.author = ''
        self.isbn = ''
        self.translator = ''
        self.price = 0
        # 精装/平装
        self.layout = ''
        # 介绍
        self.desc = ''
        # 推荐
        self.recommand_desc = ''
        self.min_age = 3
        self.max_age = 6
        # 主题
        self.theme = ''
        # 出版社
        self.press = ''
        # 丛书名
        self.serial_name = ''
        # 页数
        self.page_num = 0
        # 读者对象
        self.suit_reader = ''
        # 开本
        self.book_size = 20
        # 长
        self.length = 0
        # 宽
        self.width = 0
        # 厚
        self.thickness = 0
        # 重量（如果超过100则要进行转换,将g转换为kg)
        self.weight = 0
        # 品牌
        self.brand = ''
        # 平均多少星
        self.average_star = 0
        # 在图书商品中的排行
        self.book_rank = 0



























