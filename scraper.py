# -*- coding: utf-8 -*-
#a quick and dirty script to scrape/harvest resource-level metadata records from bjdata.gov.cn
#the original purpose of this work is to support the ongoing international city open data index project led by SASS

#beijing portal is needed to be parsed at html level


import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import re
import scraperwiki

import sys
reload(sys)  
sys.setdefaultencoding('utf8')


#we need random ua to bypass website security check
ua = UserAgent()
headers = {'User-Agent':ua.random}

#bjdata has a weired structure. it constructs a new list url for each category based upon category's short or its id
category_list = ["jjjs","xyfw","csjr","lyzs","jtfw","cyms",
                "yljk","wtyy","xfgw","shaq","zjxy","jyky","skbz",
                "ldjy","shfw","fwzz","zfjgysktt","hjyzybh","qyfw","nync"]

#the jsp outputs requires category id to construct url that supports pagination
category_id_list = ["1042","1073","1072","13","14","15",
                "16","17","18","19","20","21","22",
                "23","24","25","26","27","28","29"]

category = {
            "1042":u"经济建设",
            "1073":u"信用服务",
            "1072":u"财税金融",
            "13":u"旅游住宿",
            "14":u"交通服务",
            "15":u"餐饮美食",
            "16":u"医疗健康",
            "17":u"文体娱乐",
            "18":u"消费购物",
            "19":u"生活安全",
            "20":u"宗教信仰",
            "21":u"教育科研",
            "22":u"社会保障",
            "23":u"劳动就业",
            "24":u"生活服务",
            "25":u"房屋住宅",
            "26":u"政府机构与社会团体",
            "27":u"环境与资源保护",
            "28":u"企业服务",
            "29":u"农业农村"
}

#is use to auto map the input element's id value to corresponding metadata
meta_dict = {
            'solr-index-content':'desc',
            'solr-index-publishDate':'created',
            'solr-index-viewCount':'viewcount',
            'solr-index-downloadCount':'downloadcount',
            'solr-index-callCount':'callcount',
            'solr-index-unitName':'org',
            }

for c in category_id_list:
    url = "http://www.bjdata.gov.cn/cms/web/templateIndexList/indexList.jsp?channelID=" + c
    print url
    result = requests.get(url,headers=headers)
    soup = BeautifulSoup(result.content,features='html.parser')
    #first get back the page length
    #we taget the "最后页" link and parse its url to get the page length
    try:
        page_length = soup.find("a",string='最后页')['href'].split('&')[0].split('=')[1]
    except Exception as ex:
        print ex
        page_length = 1
    #iterate each pages
    for i in range(1,int(page_length)+1):
        page_url =  url + '&currPage='+str(i)
        print page_url
        result = requests.get(page_url,headers=headers)
        soup = BeautifulSoup(result.content,features='html.parser')
        print soup
        #on each page, fetch all package blocks
        package_blocks = soup.find_all(attrs={"class":"ztrit_box fn-clear"})
        print len(package_blocks)
        #iterate each blocks
        for p in package_blocks:
            package_dict = {'url':'',
                            'name':'',
                            'desc':'',
                            'org':'',
                            'topics':'',
                            'tags':'',
                            'created':'',
                            'updated':'',
                            'frequency':'MISSING',
                            'format':'',
                            'viewcount':'',
                            'downloadcount':'',
                            'callcount':'',
                            #beijing by default has a api to return data file download link, but for some data it has a true api.
                            #the second api on the webpage shall be the true api
                            'column_count':'',
                            'row_count':'',
                            'trueapi':'',
            }
            package_dict['url'] = p.a['href']
            print package_dict['url']
            package_dict['name'] = p.a.text.strip()
            package_dict['updated'] = p.find_all("span")[1].string[:-2]
            package_dict['topics'] = category[c]
            result = requests.get(package_dict['url'],headers=headers)
            soup = BeautifulSoup(result.content,features='html.parser')
            package_details = soup.find(id="solr-Index").find_all('input')
            #iterate the details block to map value into package_dict
            for d in package_details:
                try:
                    package_dict[meta_dict[d['id']]] = d['value']
                except:
                    continue
            #the download count parsed from above is the url to the js script outputing the count
            #we need further parse that url
            
            viewcount_url = package_dict['viewcount'].split('*')[1]
            package_dict['viewcount'] = re.compile('[0-9]+').findall(requests.get(viewcount_url).text)[0]
            #next we evaluate the trueapi
            api_blocks = soup.find(attrs={'class':'zt_details_jiekou fn-clear'}).\
                        find(attrs={'class':'sjdetails_boxinfo fn-clear'}).\
                        find_all(attrs={'class':'fn-clear'})
            package_dict['trueapi'] = 'True' if len(api_blocks)>1 else 'False'
            #next we try to get format which is marked as img icon
            data_blocks = soup.find(attrs={'class':'zt_details_shuju fn-clear'}).\
                        find(attrs={'class':'sjdetails_boxinfo fn-clear'}).\
                        find_all('img')
            format = []
            pattern=re.compile("_([a-z]+)\.")
            for d in data_blocks:
                format.append(pattern.findall(d['src'])[0])
            package_dict['format'] = '|'.join(format)
            #next we further parse the metadata tab on the page to get tags
            meta_txt_url = "http://www.bjdata.gov.cn/cms/web/dataDetail/sjxx/"+package_dict['org']+"/"+package_dict['name']+".txt"
            print meta_txt_url
            result = requests.get(meta_txt_url,headers=headers)
            soup = BeautifulSoup(result.content,features='html.parser')
            #iterate rows in the text to find the tags
            for item in soup.p.text.split('\r\n'):
                try:
                    key,value = item.split('\t')
                    if key.encode('utf-8') == '关键字说明'.encode('utf-8'):
                        package_dict['tags'] = value.replace("；","|")
                except Exception as ex:
                    print ex
            #next we need to parse the data preview to check column num and row num
            #but first let's know whether it is necessary to do it based upon format
            if 'csv' in format or 'xls' in format:
                preview_url = "http://www.bjdata.gov.cn/cms/web/dataDetail/sjyl/"+package_dict['org']+"/"+package_dict['name']+"/"+package_dict['name']+".html"
                print preview_url
                result = requests.get(preview_url, headers=headers)
                soup = BeautifulSoup(result.content, features='html.parser')
                #first use re to parse the row_count from the text
                pattern = re.compile('[0-9]')
                try:
                    package_dict['row_count'] = pattern.findall(soup.strong.string)[0]
                    #next count table columns
                    package_dict['column_count'] = str(len(soup.find(attrs={'class':'tableizer-firstrow'}).find_all('th')))
                except Exception as ex:
                    print ex
                    package_dict['row_count'] = 'MISSING'
                    package_dict['column_count'] = 'MISSING'
            #output the result
            scraperwiki.sqlite.save(unique_keys=['url'],data=package_dict)
            print '****************end------end****************'

