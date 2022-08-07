import requests
from bs4 import BeautifulSoup
import logging


BRAND_LIST = requests.get('https://api.av.by/offer-types/cars/catalog/brand-items').json()
MAX_PAGES = 120
COUNT_PER_PAGE = 25


def get_count_of_brand(brand_id: int, **context):
    auto = requests.get(f'https://cars.av.by/filter?brands[0][brand]={brand_id}&page=1')
    soup = BeautifulSoup(auto.text, 'html.parser')
    count = int(str(soup.find("h3", {"class": ["listing__title"]}).text.split(" ")[1]).replace('\u2009', ''))
    context["task_instance"].xcom_push(key="count", value=count)


def get_pages(templates_dict: dict,  **context):
    logging.info(f"count is {templates_dict['count']}")
    if int(templates_dict['count']) // COUNT_PER_PAGE > MAX_PAGES:
        pages = MAX_PAGES
    else:
        pages = int(templates_dict['count']) // COUNT_PER_PAGE
    context["task_instance"].xcom_push(key="pages", value=pages)


def get_id_list_per_page(brand_id: int, page_number: int,) -> list:
    page = requests.get(f'https://cars.av.by/filter?brands[0][brand]={brand_id}&page={page_number}')
    soup = BeautifulSoup(page.text, 'html.parser')
    id_list_html = soup.find_all("a", {"class": "listing-item__link"})
    id_list = [i['href'].split('/')[3] for i in id_list_html]
    return id_list


def get_info_by_id(id: int) -> dict:
    info = requests.get(f'https://api.av.by/offers/{id}').json()
    return info