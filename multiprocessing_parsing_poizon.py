import multiprocessing
import os
import random
import time
import json
import re

import numpy as np
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent


def reads_files():
    for root, dirs, files in os.walk(r"C:\Users\valer\PycharmProjects\pythonProject\parsing_poizon"):
        if 'all_links_cards.txt' not in files:
            with open('links_poizon.txt', mode='r', encoding='utf-8') as file:
                links_categories = [row.strip().replace('\ufeff', '') for row in file.readlines()]
            split_links = np.array_split(links_categories, 8)
            multiprocessing_page(split_links)
        else:
            with open('all_links_cards_remand.txt', mode='r', encoding='utf-8') as file2:
                links_categories = [row.strip().replace('\ufeff', '') for row in file2.readlines()]
            split_links = np.array_split(links_categories, 8)
            multiprocessing_cards(split_links)


def new_browser(url='https://www.poizon.com'):
    ua = UserAgent(platforms='pc')
    options = webdriver.ChromeOptions()
    options.page_load_strategy = 'eager'
    options.add_argument('--blink-settings=imagesEnabled=false')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    for _ in range(3):
        options.add_argument(f'user-agent={ua.random}')
        browser = webdriver.Chrome(options=options)
        browser.set_page_load_timeout(60)
        browser.maximize_window()
        try:
            browser.get(url)
            button = WebDriverWait(browser, 60).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'div.ant-modal-content>button')))
            browser.execute_script("arguments[0].click();", button)
            return browser
        except:
            browser.quit()
            time.sleep(1)


def write_all_links_cards(all_links_cards):
    with open('all_links_cards.txt', 'w', encoding='utf-8') as file:
        for link in all_links_cards:
            print(link, file=file)

def multiprocessing_page(links):
    with multiprocessing.Pool(processes=8) as p:
        result = p.map(check_links_categories, links)
    all_links_cards = [num for sublist in result for num in sublist]
    write_all_links_cards(all_links_cards)


def multiprocessing_cards(links):
    with multiprocessing.Pool(processes=8) as p:
        p.map(check_all_cards, links)


def check_links_categories(links):
    time.sleep(random.uniform(0.5, 5.0))
    all_links_flow = set()
    progress_bar = tqdm(total=len(links), desc=f"Проверяю ссылки", unit=' ссылка')
    browser = new_browser()
    for n, url in enumerate(links, 1):
        try:
            browser.get(url=url)
            check_pagination(browser, all_links_flow)
        except:
            browser.quit()
            time.sleep(1)
            browser = new_browser()

        if n % 30 == 0:
            browser.quit()
            time.sleep(1)
            browser = new_browser()

        progress_bar.update()
    progress_bar.close()

    if browser:
        browser.quit()
    return all_links_flow


def check_pagination(browser, all_links_flow):
    while True:
        try:
            links_cards = WebDriverWait(browser, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.GoodsList_goodsList__hPoCW>a')))
            [all_links_flow.add(i.get_attribute('href')) for i in links_cards]
        except:
            break
        try:
            next_pagination = WebDriverWait(browser, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'li.ant-pagination-next')))
            if next_pagination.get_attribute('aria-disabled') == 'true':
                break
            else:
                browser.execute_script("return arguments[0].scrollIntoView({ block: 'center', behavior: 'auto' });",
                                       next_pagination)
                time.sleep(1)
                WebDriverWait(browser, 5).until(EC.element_to_be_clickable(next_pagination))
                browser.execute_script("arguments[0].click();", next_pagination)
                try:
                    WebDriverWait(browser, 5).until(EC.staleness_of(links_cards[0]))
                except:
                    time.sleep(1)
        except:
            break


def check_all_cards(links):
    time.sleep(random.uniform(0.5, 5.0))
    all_final_data = []
    filename = f"output_{os.getpid()}.json"
    progress_bar = tqdm(total=len(links), desc=f"Собираю данные c ссылок товара", unit=' link')
    browser = new_browser()
    for n, url in enumerate(links, 1):
        try:
            browser.get(url=url)
            check_card(browser, url, all_final_data)
            write_final_file(filename, all_final_data)
        except:
            browser.quit()
            time.sleep(1)
            browser = new_browser()

        if n % 30 == 0:
            browser.quit()
            time.sleep(1)
            browser = new_browser()

        progress_bar.update()
    progress_bar.close()

    if browser:
        browser.quit()


def search_items_details(items_details_wait):
    items_details = {i.find_element(By.CSS_SELECTOR, 'span.ProductDetails_propertyLabel__ZlSsu').text.strip():
                     i.find_element(By.CSS_SELECTOR, 'span.ProductDetails_propertyValue__Aj_Cz').text.strip() for i in
                     items_details_wait}
    final_items_details = [items_details.get('Brand', '-'), items_details.get('Style', '-')]
    return final_items_details


def check_card(browser, link, all_final_data):
    pattern = r'"categoryId":(\d+)'
    try:
        name = WebDriverWait(browser, 3).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.MainInfo_title__YSsXk'))).text.strip()
    except:
        name = '-'
    try:
        category_id_wait = WebDriverWait(browser, 3).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'script#__NEXT_DATA__'))).get_attribute('innerHTML')
        category_id = ''.join(re.findall(pattern, category_id_wait)).strip()
    except:
        category_id = ''
    try:
        details_button = WebDriverWait(browser, 5).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, 'div.ProductDetails_more__3bYAA')))
        browser.execute_script("arguments[0].click();", details_button)
        time.sleep(1)
    except:
        pass
    try:
        items_details_wait = WebDriverWait(browser, 10).until(
            EC.visibility_of_all_elements_located((By.CSS_SELECTOR, 'li.ProductDetails_propertyItem__mGdzY')))
        final_items_details = search_items_details(items_details_wait)
    except:
        final_items_details = ['-', '-']
    try:
        categories = WebDriverWait(browser, 3).until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, 'div.BreadCrumb_breadcrumb__Iy_yk>a:nth-child(3)>span'))).text.strip()
    except:
        categories = ''
    try:
        links_images_wait = {i.get_attribute('src') for i in WebDriverWait(browser, 30).until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, 'div.ProductSkuImgs_mainImg__CP_SL div.PoizonImage_imageWrap__RZTiw>img')))}
        links_images = [q for q in links_images_wait]
    except:
        links_images = ['-']
    try:
        size_button = WebDriverWait(browser, 3).until(
            EC.visibility_of_all_elements_located((By.CSS_SELECTOR, 'div.SkuPanel_tabItem__MuUkW')))
    except:
        size_button = False
    if size_button:
        check_gender(browser, size_button, name, links_images, link, categories, final_items_details, category_id, all_final_data)
    else:
        try:
            check_count_menu = WebDriverWait(browser, 10).until(EC.visibility_of_all_elements_located(
                (By.CSS_SELECTOR, 'div.SkuPanel_label__Vbp8t>span:nth-child(1)')))
            if len(check_count_menu) == 1:
                try:
                    size_wait = WebDriverWait(browser, 5).until(EC.visibility_of_all_elements_located(
                                (By.CSS_SELECTOR, 'div.SkuPanel_group__egmoX:nth-child(1) div.SkuPanel_value__BAJ1p')))
                    size = [s.get_attribute('textContent').strip() for s in size_wait]
                except:
                    size = ['-']
                try:
                    price_wait = WebDriverWait(browser, 5).until(EC.visibility_of_all_elements_located(
                                    (By.CSS_SELECTOR, 'div.SkuPanel_group__egmoX:nth-child(1) div.SkuPanel_price__KCs7G')))
                    price = [p.get_attribute('textContent').strip().replace('$', '') for p in price_wait]
                    clear_price = list(map(lambda c: int(c) if c.isdigit() else '', price))
                except:
                    clear_price = ['-']
                size_and_price = {f[0]: f[1] for f in zip(size, clear_price)}

                all_final_data.append({'Name': name,
                                       'Categories': categories,
                                       'Sizes': size_and_price,
                                       'Images': links_images,
                                       'Link': link,
                                       'SpuId': int(link.split('-')[-1]),
                                       'CategoryId': int(category_id),
                                       'Brand': final_items_details[0],
                                       'Vendor': final_items_details[1]})
            elif len(check_count_menu) == 2:
                color_wait_button = WebDriverWait(browser, 5).until(EC.visibility_of_all_elements_located(
                        (By.CSS_SELECTOR, 'div.SkuPanel_list__OUqa1.SkuPanel_col4__UYcTN.SkuPanel_imgList__7Uem4>div')))
                all_color = {}
                for color_button in color_wait_button:
                    try:
                        browser.execute_script("arguments[0].click();", color_button)
                        time.sleep(1)
                    except:
                        pass
                    try:
                        color = WebDriverWait(browser, 3).until(EC.visibility_of_element_located(
                                    (By.CSS_SELECTOR, 'span.SkuPanel_labelValue__C1VLz'))).text.strip()
                    except:
                        color = '-'
                    try:
                        size_wait = WebDriverWait(browser, 5).until(EC.visibility_of_all_elements_located(
                                    (By.CSS_SELECTOR, 'div.SkuPanel_group__egmoX:nth-child(2) div.SkuPanel_value__BAJ1p')))
                        size = [s.get_attribute('textContent').strip() for s in size_wait]
                    except:
                        size = ['-']
                    try:
                        price_wait = WebDriverWait(browser, 5).until(EC.visibility_of_all_elements_located(
                                    (By.CSS_SELECTOR, 'div.SkuPanel_group__egmoX:nth-child(2) div.SkuPanel_price__KCs7G')))
                        price = [p.get_attribute('textContent').strip().replace('$', '') for p in price_wait]
                        clear_price = list(map(lambda c: int(c) if c.isdigit() else '', price))
                    except:
                        clear_price = ['-']
                    size_and_price = {f[0]: f[1] for f in zip(size, clear_price)}
                    if color not in all_color:
                        all_color[color] = {'Sizes': size_and_price}

                all_final_data.append({'Name': name,
                                       'Categories': categories,
                                       'Color': all_color,
                                       'Images': links_images,
                                       'Link': link,
                                       'SpuId': int(link.split('-')[-1]),
                                       'CategoryId': int(category_id),
                                       'Brand': final_items_details[0],
                                       'Vendor': final_items_details[1]})
            elif len(check_count_menu) == 3:
                list_sizes = WebDriverWait(browser, 5).until(EC.visibility_of_all_elements_located(
                                (By.CSS_SELECTOR, 'div.SkuPanel_list__OUqa1.SkuPanel_col4__UYcTN.SkuPanel_imgList__7Uem4>div')))
                all_color = {}
                for sizes in list_sizes:
                    try:
                        browser.execute_script("arguments[0].click();", sizes)
                        time.sleep(1)
                    except:
                        pass
                    try:
                        color = WebDriverWait(browser, 3).until(EC.visibility_of_element_located(
                            (By.CSS_SELECTOR, 'span.SkuPanel_labelValue__C1VLz'))).text.strip()
                    except:
                        color = '-'
                    try:
                        size = WebDriverWait(browser, 5).until(EC.visibility_of_element_located(
                            (By.CSS_SELECTOR, 'div.SkuPanel_group__egmoX:nth-child(3) div.SkuPanel_value__BAJ1p'))).get_attribute(
                        'textContent').strip()
                    except:
                        size = '-'
                    try:
                        price = WebDriverWait(browser, 5).until(EC.visibility_of_element_located(
                                (By.CSS_SELECTOR, 'div.SkuPanel_group__egmoX:nth-child(3) div.SkuPanel_price__KCs7G'))).get_attribute(
                        'textContent').strip()
                        clear_price = price.replace('$', '')
                    except:
                        clear_price = '-'
                    size_and_price = {f[0]: f[1] for f in zip(size, clear_price)}
                    if color not in all_color:
                        all_color[color] = {'Sizes': size_and_price}
                all_final_data.append({'Name': name,
                                       'Categories': categories,
                                       'Color': all_color,
                                       'Images': links_images,
                                       'Link': link,
                                       'SpuId': int(link.split('-')[-1]),
                                       'CategoryId': int(category_id),
                                       'Brand': final_items_details[0],
                                       'Vendor': final_items_details[1]})
        except:
            pass


def check_gender(browser, size_button, name, links_images, link, categories, items_ditals, category_id, all_final_data):
    all_size_and_price = []
    for size in size_button:
        try:
            browser.execute_script("arguments[0].click();", size)
            time.sleep(1)
        except:
            pass
        try:
            size_wait = WebDriverWait(browser, 5).until(EC.visibility_of_all_elements_located(
                (By.CSS_SELECTOR, 'div.SkuPanel_group__egmoX:nth-child(1) div.SkuPanel_value__BAJ1p')))
            size = [s.get_attribute('textContent').strip() for s in size_wait]
        except:
            size = ['-']
        try:
            price_wait = WebDriverWait(browser, 5).until(EC.visibility_of_all_elements_located(
                (By.CSS_SELECTOR, 'div.SkuPanel_group__egmoX:nth-child(1) div.SkuPanel_price__KCs7G')))
            price = [p.get_attribute('textContent').strip().replace('$', '') for p in price_wait]
            clear_price = list(map(lambda c: int(c) if c.isdigit() else '', price))
        except:
            clear_price = ['-']
        all_size_and_price.append({f[0]: f[1] for f in zip(size, clear_price)})

    all_final_data.append({'Name': name,
                           'Categories': categories,
                           'Sizes': all_size_and_price,
                           'Images': links_images,
                           'Link': link,
                           'SpuId': int(link.split('-')[-1]),
                           'CategoryId': int(category_id),
                           'Brand': items_ditals[0],
                           'Vendor': items_ditals[1]})


def write_final_file(filename, all_final_data):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(all_final_data, file, indent=4, ensure_ascii=False)



if __name__ == '__main__':
    reads_files()
