import requests
from bs4 import BeautifulSoup
import re
import random
import time
import psycopg2

conn = psycopg2.connect(host="127.0.0.1",user="postgres",password="zhangyu",database="test")
cur = conn.cursor()

province_url_list = []
province_list = []

city_list = []
city_url_list = []

county_list = []
county_url_list = []

town_list = []
town_url_list = []

village_list = []

agents = [
    'User-Agent:Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36',
    'User-Agent:Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.5 (KHTML, like Gecko) Chrome/4.0.249.0 Safari/532.5',
    'User-Agent:Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/532.9 (KHTML, like Gecko) Chrome/5.0.310.0 Safari/532.9',
    'User-Agent:Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/534.7 (KHTML, like Gecko) Chrome/7.0.514.0 Safari/534.7',
    'User-Agent:Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/534.14 (KHTML, like Gecko) Chrome/9.0.601.0 Safari/534.14',
    'User-Agent:Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.14 (KHTML, like Gecko) Chrome/10.0.601.0 Safari/534.14',
    'User-Agent:Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.20 (KHTML, like Gecko) Chrome/11.0.672.2 Safari/534.20',
    'User-Agent:Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.27 (KHTML, like Gecko) Chrome/12.0.712.0 Safari/534.27',
    'User-Agent:Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.24 Safari/535.1',
    'User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
    'User-Agent:Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'User-Agent:Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E) QQBrowser/6.9.11079.201',
    'User-Agent:Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.41 Safari/535.1 QQBrowser/6.9.11079.201',
    'User-Agent:Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E)',
    'User-Agent:Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.472.33 Safari/534.3 SE 2.X MetaSr 1.0',
    'User-Agent:Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E; SE 2.X MetaSr 1.0)',
    'User-Agent:Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E)',
    'User-Agent:Mozilla/5.0 (Windows; U; Windows NT 6.1; ) AppleWebKit/534.12 (KHTML, like Gecko) Maxthon/3.0 Safari/534.12',
    'User-Agent:Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)',
    'User-Agent:Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
    'User-Agent:Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; GTB7.0)',
    'User-Agent:Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; InfoPath.3)',
    'User-Agent:Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; Tablet PC 2.0; .NET4.0E)',
    'User-Agent:Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.9.168 Version/11.50',
    'User-Agent:Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
    'User-Agent:Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0',
    'User-Agent:Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1'
]

proxy_pool = [
    '125.40.109.154:44641',
    '111.177.171.169:9999',
    '111.177.191.199:9999',
    '111.177.183.47:9999',
    '111.177.185.91:9999',
    '125.26.7.116:57059',
    '111.177.161.86:9999',
    '111.177.170.93:9999',
    '111.177.166.204:9999',
    '121.40.78.138:3128',
    '60.13.42.18:9999',
    '183.148.136.202:9999',
    '217.78.5.57:3128',
    '111.177.183.42:9999',
    '118.173.233.39:48988',
    '124.93.201.59:42672',
    '125.126.219.26:9999',
    '111.177.185.249:9999',
    '117.91.251.82:9999',
    '111.177.186.107:9999',
    '111.177.162.208:9999',
    '111.177.177.216:9999',
    '163.204.243.194:9999',
    '128.199.170.87:31330',
    '125.27.10.209:59790',
    '1.197.203.112:9999',
    '111.177.189.198:9999',
    '114.55.236.62:3128',
    '1.202.245.84:8080',
    '111.177.188.161:9999',
    '139.196.22.147:3128',
    '111.177.181.93:9999',
    '111.177.185.201:9999',
    '111.177.175.163:9999',
    '111.177.180.20:9999',
    '163.204.246.1:9999',
    '111.177.175.28:9999',
    '111.177.180.117:9999',
    '111.177.191.13:9999',
    '60.13.42.67:9999',
    '218.60.8.83:3129',
    '111.177.165.226:9999',
    '1.20.99.104:37003',
    '111.177.161.118:9999',
    '1.20.101.121:31946',
    '111.177.177.252:9999',
    '111.177.172.128:9999',
    '111.177.181.135:9999',
    '111.177.167.115:9999',
    '111.177.188.109:9999',
    '111.177.176.169:9999',
    '118.172.201.91:39181',
    '111.177.160.47:9999',
    '111.177.166.53:9999',
    '111.177.189.67:9999',
    '111.177.180.181:9999',
    '134.119.222.238:8080',
    '111.177.162.76:9999',
    '117.25.83.38:8118',
    '123.207.66.209:1080',
    '111.177.175.32:9999',
    '112.85.150.96:9999',
    '123.57.84.116:8118',
    '222.74.237.246:808',
    '180.183.101.59:8080',
    '111.177.168.152:9999',
    '111.177.168.78:9999',
    '111.177.185.39:9999',
    '139.196.137.255:8118',
    '47.94.135.32:8118',
    '211.147.239.101:60999',
    '111.177.173.189:9999',
    '134.119.214.198:1080',
    '116.209.52.114:9999',
    '111.177.181.4:9999',
    '163.204.246.51:9999',
    '111.177.177.3:9999',
    '111.177.163.149:9999',
    '111.177.189.91:9999',
    '221.126.249.102:8080',
    '111.177.160.30:9999',
    '106.15.42.179:33543',
    '222.173.215.170:8080',
    '111.177.185.26:9999',
    '111.177.188.35:9999',
    '163.204.245.58:9999',
    '202.128.22.72:53549',
    '114.230.69.236:9999',
    '111.177.175.178:9999',
    '180.180.218.150:38061',
    '111.177.171.30:9999',
    '111.177.164.213:9999',
    '111.177.167.232:9999',
    '183.89.144.235:8080',
    '49.86.180.163:9999',
    '111.177.174.73:9999',
    '118.172.201.60:46896',
    '103.16.168.194:45316',
    '111.177.174.115:9999',
    '120.83.107.1:9999',
    '222.189.191.92:9999',
    '111.177.165.85:9999',
    '121.61.0.165:9999',
    '222.223.115.30:31387',
    '182.34.35.76:9999',
    '1.199.30.124:9999',
    '1.10.187.128:57434',
    '96.9.69.230:53281',
    '112.85.167.228:9999',
    '116.113.27.170:53889',
    '111.177.175.214:9999',
    '182.52.51.4:40347',
    '111.177.182.246:9999',
    '112.85.170.32:9999',
    '111.72.155.80:9999',
    '117.91.254.28:9999',
    '125.24.156.252:44073',
    '101.51.141.46:37858',
    '111.177.168.80:9999',
    '52.83.253.74:3128',
    '52.83.146.11:3128',
    '111.177.166.92:9999',
    '125.126.223.86:9999',
    '183.63.101.62:55555',
    '112.85.171.117:9999',
    '111.177.191.209:9999',
    '111.177.161.28:9999',
    '111.177.178.225:9999',
    '59.45.13.220:57868',
    '111.177.165.52:9999',
    '118.190.95.43:9001',
    '111.177.181.90:9999',
    '111.177.163.140:9999',
    '113.121.44.36:9999',
    '111.177.185.169:9999',
    '111.177.163.71:9999',
    '183.89.31.205:8080',
    '111.177.176.190:9999',
    '111.177.160.31:9999',
    '58.210.133.98:38850',
    '111.177.167.104:9999',
    '111.177.172.60:9999',
    '111.177.174.189:9999',
    '111.177.191.47:9999',
    '111.177.162.111:9999',
    '183.148.143.245:9999',
    '111.177.191.247:9999',
    '222.184.7.206:45570',
    '111.177.162.207:9999',
    '47.98.174.153:8118',
    '111.177.184.65:9999',
    '47.99.113.175:8118',
    '111.177.184.217:9999',
    '115.151.2.77:9999',
    '182.52.51.47:45440',
    '111.77.196.61:9999',
    '61.131.146.245:9999',
    '111.177.187.98:9999',
    '111.177.171.100:9999',
    '111.177.167.251:9999',
    '111.177.161.205:9999',
    '111.177.174.220:9999',
    '111.177.188.23:9999',
    '111.177.186.220:9999',
    '163.204.247.235:9999',
    '111.177.175.90:9999',
    '111.177.183.61:9999',
    '163.204.244.248:9999',
    '125.26.99.84:60493',
    '111.177.181.199:9999',
    '111.177.179.213:9999',
    '111.177.176.162:9999',
    '47.107.160.99:8118',
    '111.177.164.68:9999',
    '49.86.179.30:9999',
    '111.177.177.96:9999',
    '60.13.42.176:9999',
    '180.180.156.35:37463',
    '112.85.128.189:9999',
    '183.6.130.6:8118',
    '115.239.206.246:1080',
    '1.20.97.216:39363',
    '111.177.161.227:9999',
    '111.177.188.87:9999',
    '222.252.15.114:42534',
    '111.177.164.37:9999',
    '183.88.213.25:58560',
    '182.52.74.76:34084',
    '1.10.188.5:33853',
    '49.86.178.122:9999',
    '125.26.109.114:61005',
    '111.177.170.221:9999',
    '111.177.167.158:9999',
    '163.204.247.152:9999',
    '171.97.137.21:8080',
    '111.177.160.148:9999',
    '111.177.191.5:9999',
    '183.145.51.196:8118',
    '125.26.99.131:51282',
    '111.177.180.67:9999',
    '103.239.54.3:52083',
    '125.26.109.93:57030',
    '111.177.171.154:9999',
    '123.55.114.234:9999',
    '111.177.190.153:9999',
    '111.177.161.201:9999',
    '116.209.55.83:9999',
    '111.177.161.248:9999',
    '117.91.249.222:9999',
    '183.129.207.91:22744',
    '111.177.178.145:9999',
    '120.26.127.90:8118',
    '111.177.179.253:9999',
    '111.177.170.57:9999',
    '113.130.126.2:34129',
    '183.148.136.159:9999',
    '111.177.174.22:9999',
    '111.177.174.234:9999',
    '111.177.169.98:9999',
    '1.179.147.5:39330',
    '112.27.167.74:33641',
    '111.177.162.87:9999',
    '182.53.197.142:47798'
]


def get_province():
    target = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2015/index.html'
    url = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2015/'
    headers = {'User-Agent': random.choice(agents)}
    proxy = {'https': random.choice(proxy_pool)}
    req = requests.get(url=target, headers=headers, proxies=proxy, verify=False)
    html = req.content.decode('gbk')

    bf = BeautifulSoup(str(html), "html.parser")
    texts = bf.find_all('tr', class_='provincetr')

    for text in texts:
        tr = BeautifulSoup(str(text), "html.parser")
        a = tr.find_all('a')
        for each in a:
            province = re.sub("[A-Za-z0-9\!\%\[\]\,\。\.\<\>\"/= ]", "", str(each))
            href = each.get('href')
            province_code = str(href).replace('.html', '')
            province_tuple = (province, province_code, '0', 'province')
            try :
                cur.execute("INSERT INTO region_area (id, area_name, area_type, pid) VALUES (%d,%s,%s,%d)" % {province, province_code, '0', 'province'})

            except Exception :
                print(province, "已存在！")
            province_list.append(province_tuple)
            province_url_list.append(url + each.get('href'))

def get_city(citys_url : list):
    for city_url in citys_url:
        base_url = str(city_url).replace('.html', '')
        headers = {'User-Agent': random.choice(agents)}
        proxy = {'https': random.choice(proxy_pool)}
        req = requests.get(url=city_url, headers=headers, proxies=proxy)
        html = req.content.decode('gbk')
        bf = BeautifulSoup(str(html), "html.parser")
        texts = bf.find_all('tr', class_='citytr')
        for text in texts:
            tr = BeautifulSoup(str(text), "html.parser")
            a = tr.find_all('a')
            for each in a:
                city_name = re.sub("[A-Za-z0-9\!\%\[\]\,\。\.\<\>\"/= ]", "", str(each))
                if city_name == '':
                    continue
                splits = str(each.get('href')).replace('.html', '').split('/')
                pid = splits[0]
                city_code = splits[1]
                city_tuple = (city_name, city_code, pid, 'city')
                city_list.append(city_tuple)
                url = base_url+"/"+city_code+'.html'
                city_url_list.append(url)


def get_county(countys_url : list):
    num = 0
    for county_url in countys_url :
        num = num + 1
        base_url = str(county_url).replace('.html', '')
        headers = {'User-Agent': random.choice(agents)}
        proxy = {'https': random.choice(proxy_pool)}
        if num % 5 == 0 :
            time.sleep(5)
        req = requests.get(url=county_url, headers=headers, proxies=proxy)
        html = req.content.decode('gbk')
        bf = BeautifulSoup(str(html), "html.parser")
        texts = bf.find_all('tr', class_='countytr')
        for text in texts:
            tr = BeautifulSoup(str(text), "html.parser")
            a = tr.find_all('a')
            for each in a:
                county_name = re.sub("[A-Za-z0-9\!\%\[\]\,\。\.\<\>\"/= ]", "", str(each))
                if county_name == '':
                    continue
                splits = str(each.get('href')).replace('.html', '').split('/')
                pid = splits[0]
                county_code = splits[1]
                county_tuple = (county_name, county_code, county_code[:4], 'county')
                county_list.append(county_tuple)
                base_url = base_url.replace(county_code[:4], county_code[2:4])
                url = base_url + "/" + county_code + '.html'
                county_url_list.append(url)

def get_town(towns_url : list):
    num = 0
    for town_url in towns_url :
        base_url = str(town_url).replace('.html', '')
        headers = {'User-Agent': random.choice(agents)}
        proxy = {'https': random.choice(proxy_pool)}
        num = num + 1
        if num % 5 == 0 :
            time.sleep(5)
        req = requests.get(url=town_url, headers=headers, proxies=proxy)
        html = req.content.decode('gbk')
        bf = BeautifulSoup(str(html), "html.parser")
        texts = bf.find_all('tr', class_='towntr')
        for text in texts:
            tr = BeautifulSoup(str(text), "html.parser")
            a = tr.find_all('a')
            for each in a:
                town_name = re.sub("[A-Za-z0-9\!\%\[\]\,\。\.\<\>\"/= ]", "", str(each))
                if town_name == '':
                    continue
                splits = str(each.get('href')).replace('.html', '').split('/')
                pid = splits[0]
                town_code = splits[1]
                town_tuple = (town_name, town_code, town_code[:6], 'town')
                town_list.append(town_tuple)
                # http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2015/11/01/01/110101002.html
                base_url = base_url.replace(town_code[:6], town_code[4:6])
                url = base_url + "/" + town_code + '.html'
                town_url_list.append(url)

def get_village(towns_url: list):
    num = 0
    for town_url in towns_url :
        headers = {'User-Agent': random.choice(agents)}
        proxy = {'https': random.choice(proxy_pool)}
        num = num + 1
        if num % 5 == 0:
            time.sleep(5)
        req = requests.get(url=town_url, headers=headers, proxies=proxy)

        html = req.content.decode('gbk')
        bf = BeautifulSoup(str(html), "html.parser")
        texts = bf.find_all('tr', class_='villagetr')
        for text in texts:
            tr = BeautifulSoup(str(text), "html.parser")
            td = tr.find_all('td')
            village_code = ''
            village_name = ''
            for each in td :
                code = re.findall(r'\d+', str(each))
                if code and len(code[0]) == 12 :
                    village_code = str(code[0])
                    continue
                substr = re.sub("[A-Za-z0-9\!\%\[\]\,\。\.\<\>\"/= ]", "", str(each))
                if substr=='':
                    continue
                village_name = substr
            village_touple = (village_name, village_code, village_code[:9], 'village')
            village_list.append(village_touple)



def write_file(file_name:str, items : list) :
    with open(file_name , 'w') as f :
        for item in items :
            strr = " ".join(item)
            print(strr)
            f.write(strr + '\n')


if __name__ == '__main__':
    get_province()
    write_file('province.txt', province_list)

    conn.commit()
    # get_city(province_url_list)
    # write_file('city.txt', city_list)
    #
    # get_county(city_url_list)
    # write_file('county.txt', county_list)
    #
    # # print(county_url_list)
    #
    # get_town(county_url_list)
    # write_file('town.txt', town_list)
    # write_file('village_urls.txt', town_url_list)
    # #
    # get_village(town_url_list)
    # write_file('village.txt', village_list)

    # urls = []
    # urls.append('http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2015/12/02/25/120225109.html')
    # get_village(urls)
    # print(village_list)

    # county_url_list.append("http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2015/11/01/110101.html")
    # get_town(county_url_list)
    # print(town_url_list)
