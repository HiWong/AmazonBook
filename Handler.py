#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2016-10-18 09:35:17
# Project: AmazonChildBook2

from pyspider.libs.base_handler import *
import re

PAGE_START=1
PAGE_END=75
BOOK_PATH='/home/allen/Public/childbooks.txt'

class Handler(BaseHandler):
    crawl_config = {
    }

    def __init__(self):
        self.base_url="https://www.amazon.cn/s/ref=lp_658409051_pg_2?rh=n%3A658390051%2Cn%3A%21658391051%2Cn%3A658409051&page={0}&ie=UTF8&qid=1476538037"
        self.page_num=PAGE_START
        self.total_num=PAGE_END
        self.bookList=[]
    
    
    @every(minutes=24 * 60)
    def on_start(self):
         while self.page_num<=self.total_num:
            #url=self.base_url % str(self.page_num)
            url=self.base_url.format(self.page_num)
            print url
            self.crawl(url,callback=self.index_page)
            self.page_num+=1
        
        
        
    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
         print "current page_num:%d" % self.page_num
         print "get response now"  
         result=response.text
         #用正则匹配所有html页面中的title
         pattern=re.compile('<a class="a-link-normal s-access-detail-page  a-text-normal" target="_blank" rel=".*?" title="(.*?)" href="(.*?)">.*?</a>',re.S)
         bookInfos=re.findall(pattern,result)   
         for bookInfo in bookInfos:
             title=bookInfo[0]
             href=bookInfo[1]
             title=processTitle(title)
             saveTitle(title)   
             print title

    @config(priority=2)
    def detail_page(self, response):
        return {
            "url": response.url,
            "title": response.doc('title').text(),
        }

    
def saveTitle(title):
    with open(BOOK_PATH,'a') as f:
        f.write(title+'\n')    

def processTitle(title):
    #利用正则切分出我们需要的utf-8编码，ASCII码，特殊字符(:,()!+-_.)或者空格
    #一个示例就是title="&#32599;&#20271;&#29305;&#183;&#32599;&#32032;&#20316;&#21697;&#38598;(&#32445;&#20271;&#29790;&#20799;&#31461;&#25991;&#23398;&#22870;&#20316;&#21697;)(&#25554;&#22270;&#29256;)(&#22871;&#35013;&#20849;8&#20876;)"
    pattern = r"&#\d+;|\w+|[:,()!+-_.]{1,6}|\s{1,6}"
    p = re.compile(pattern)
    word_list = p.findall(title)
    result=''
    for single_word in word_list:
        if single_word=='':
            continue
        try:
            escapeFlag,src=processSingleWord(single_word)
            if(escapeFlag):
                src=int(src,10)
                #ascii码
                if src<0xff:
                    result = result + chr(src).decode('unicode-escape').encode('utf-8')
                else:
                    src=hex(src)
                    # 注意:此时src类型为str
                    src = src.replace('0x', '\u')
                    src = src.decode('unicode-escape')
                    result = result + src.encode('utf-8')
            else:
                result=result+src.decode('unicode-escape').encode('utf-8')
        except:
            result = result + single_word.decode('unicode-escape').encode('utf-8')
    return result

def processSingleWord(singleWord):
    p=re.compile(r"\d+")
    if singleWord.startswith("&#") and singleWord.endswith(";"):
        return True,p.findall(singleWord)[0]
    else:
        return False,singleWord    
    
from bs4 import BeautifulSoup

class AmazonBook(object):
    def __init__(self,title,link):
        self.title=title
        self.link=link    
