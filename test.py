import requests
from bs4 import BeautifulSoup
import re

province_url_list = []
province_list = []

city_list = []
city_url_list = []

county_list = []
county_url_list = []

town_list = []
town_url_list = []

village_list = []

headers = {'User-Agent': 'User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'}


def get_province():
    target = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2015/index.html'
    url = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2015/'
    req = requests.get(url=target, headers=headers)
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
            province_list.append(province_tuple)
            province_url_list.append(url + each.get('href'))

def get_city(citys_url : list):
    for city_url in citys_url:
        base_url = str(city_url).replace('.html', '')

        req = requests.get(url=city_url, headers=headers)
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
    for county_url in countys_url :
        base_url = str(county_url).replace('.html', '')
        req = requests.get(url=county_url, headers=headers)
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
    for town_url in towns_url :
        base_url = str(town_url).replace('.html', '')
        req = requests.get(url=town_url, headers=headers)
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

def get_village(towns_url:list) :
    for town_url in towns_url :

        req = requests.get(url=town_url, headers=headers)
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

    get_city(province_url_list)
    write_file('city.txt', city_list)

    get_county(city_url_list)
    write_file('county.txt', county_list)

    # print(county_url_list)

    get_town(county_url_list)
    write_file('town.txt', town_list)
    #
    get_village(town_url_list)
    write_file('village.txt', village_list)

    # urls = []
    # urls.append('http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2015/12/02/25/120225109.html')
    # get_village(urls)
    # print(village_list)



    # county_url_list.append("http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2015/11/01/110101.html")
    # get_town(county_url_list)
    # print(town_url_list)
