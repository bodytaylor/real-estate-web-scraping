from bs4 import BeautifulSoup
import requests
from csv import writer
import time
import random
from lxml import etree as et
import re
import os.path


# adding user agents to avoid detection
header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.66 Safari/537.36"} 
# base url for the pages
base_url= "https://www.fazwaz.com/property-for-sale/thailand/bangkok?type=condo,apartment,penthouse&order_by=verification_at|desc&page=" 
# list to store the url of every page
pages_url=[]  
# list to store the url of every apartments                          
listing_url=[]                          

# parsed HTML of a webpage, which can be used to extract information from the page using XPath expressions later on
def get_dom(the_url):
    response = requests.get(the_url, headers=header)
    soup = BeautifulSoup(response.text,'lxml')
    dom = et.HTML(str(soup))
    return dom

# get loop number range for create url list
def loop_number():
    response = requests.get(base_url, headers=header)
    soup = BeautifulSoup(response.text,'lxml')
    dom = et.HTML(str(soup))
    page_number = int(dom.xpath('//*[@id="search-result"]/div[4]/ul/li[7]/a/text()')[0].strip()) # get footer
    page_number = page_number + 1
    return page_number

for i in range(0, loop_number() - 332):                   # generate all the pages url
    page_url=base_url + str(i)
    pages_url.append(page_url)          # append the page url to the list
    print(page_url)


def get_listing_url(page_url):          # get the url of the listing
    dom = get_dom(page_url)
    page_link_list=dom.xpath('//a[contains(@class, "link-unit")]//@href')
    for page_link in page_link_list:
        listing_url.append(str(page_link))
    print("Number of Listed Properties: ",len(listing_url))


for page_url in pages_url:              # for each page url, get the listing url
    get_listing_url(page_url)
    time.sleep(random.randint(1,10))

def get_title(dom):                     # get the title of the listing
    try:                                # try to get the title
        title=dom.xpath("/html/body/div[1]/main/div[2]/div[1]/a/h1/text()")[0] #get the title using xpath
        print(title)
    except Exception as e:              # if the title is not found, print the error message
        title = "Title is not available"
        print(title)
    return title

def get_location(dom):                  # get the location of the listing
    try:                                # try to get the location
        zone=dom.xpath("/html/body/div[1]/main/div[2]/div[1]/div[2]/span/text()[2]")[0].strip() # get area
        province=dom.xpath('/html/body/div[1]/main/div[2]/div[1]/div[2]/span/a/text()')[0].strip() # get provice
        location = zone + province
        print(zone, province)           # print the location
    except Exception as e:              # if the location is not found, print the error message
        location = "Location is not available"
        print(location)
    return location

def get_price(dom):                     # get the price of the listing
    try:                                # try to get the price
        price=dom.xpath("/html/body/div[1]/main/div[2]/div[1]/div[4]/div/text()[1]")[0].replace('฿', '').replace(',', '').strip() # remove ฿, delimiter and retrive the value
        print(price)       
    except Exception as e:              # if the price is not found, print the error message
        price = "Price is not available"
        print(price)
    return price

def get_area(dom):                      # get the area of the listing
    try:                                # try to get the area
        area=dom.xpath("/html/body/div[1]/main/div[8]/div/div[1]/div[3]/div[2]/div[6]/span[2]/text()")[0].replace("SqM","").strip()
        print(area)
    except Exception as e:              # if the area is not found, print the error message
        area="Area is not available"
        print(area)
    return area

def offered_since(dom):                 # get the date of the listing was added
    try:                                # try to get listing date
        offer_since=dom.xpath("/html/body/div[1]/main/div[8]/div/div[1]/div[3]/div[2]/div[1]/span[2]/text()")[0]
        print(offer_since)
    except Exception as e:              # if the date is not found, print the error message
        offer_since="Date is not available"
        print(offer_since)
    return offer_since


def get_cam_free(dom):                  # get the CAM free per month of the listing
    try:                                # try to get the volume
        cam= dom.xpath("/html/body/div[1]/main/div[8]/div/div[1]/div[3]/div[2]/div[13]/span/text()")[0].replace('/mo', '').replace('฿', '').replace(',', '')
        print(cam)
    except Exception as e:              # if the CAM free is not found, print the error message
        cam="Volume is not available"
        print(cam)
    return cam

def get_type(dom):                      # get the type of the listing
    try:                                # try to get the type
        type=dom.xpath("/html/body/div[1]/main/div[8]/div/div[1]/div[3]/div[2]/div[3]/span[2]/a/text()")[0].strip()
        print(type)
    except Exception as e:              # if the type is not found, print the error message
        type="Type is not available"
        print(type)
    return type


def get_construction_type(dom):         # get the construction type of the listing
    try:                                # try to get the construction type
        construction_type=dom.xpath("/html/body/div[1]/main/div[2]/div[2]/div[5]/small/text()")[0].strip()
        print(construction_type)
    except Exception as e:              # if the construction type is not found, print the error message
        construction_type="Construction type is not available"
        print(construction_type)
    return construction_type

def get_constructed_year(dom):          # get the constructed year of the listing
    try:                                # try to get the constructed year
        constructed_year=dom.xpath("/html/body/div[1]/main/div[2]/div[2]/div[5]/text()[2]")[0].strip()
        print(constructed_year)
    except Exception as e:              # if the constructed year is not found, print the error message
        constructed_year="Constructed year is not available"
        print(constructed_year)
    return constructed_year


def get_bedrooms(dom):                  # get the number of bedrooms of the listing
    try:                                # try to get the number of bedrooms
        bedrooms=dom.xpath("/html/body/div[1]/main/div[8]/div/div[1]/div[3]/div[2]/div[5]/span[2]/a/text()")[0].strip()
        print(bedrooms)
    except Exception as e:              # if the number of bedrooms is not found, print the error message
        bedrooms="Number of bedrooms is not available"
        print(bedrooms)
    return bedrooms

def get_bathrooms(dom):                 # get the number of bathrooms of the listing
    try:                                # try to get the number of bathrooms
        bathrooms=dom.xpath("/html/body/div[1]/main/div[2]/div[2]/div[2]/text()[2]")[0].strip()
        print(bathrooms)
    except Exception as e:              # if the number of bathrooms is not found, print the error message
        bathrooms="Number of bathrooms is not available"
        print(bathrooms)
    return bathrooms

def get_no_floors(dom):                 # get the number of floors of the listing
    try:                                # try to get the number of floors
        no_floors=dom.xpath("/html/body/div[1]/main/div[8]/div/div[1]/div[3]/div[2]/div[4]/span[2]/text()")[0].strip()
        print(no_floors)
    except Exception as e:              # if the number of floors is not found, print the error message
        no_floors="Number of floors is not available"
        print(no_floors)
    return no_floors

def get_details_of_balcony(dom):        # get the details of the balcony of the listing
    try:                                # try to get the details of the balcony
        balcony=dom.xpath("/html/body/div[1]/main/div[8]/div/div[1]/div[3]/div[2]/div[10]/span[2]/text()")[0]
        print(balcony)
    except Exception as e:              # if the details of the balcony is not found, print the error message
        balcony="Details of balcony is not available"
        print(balcony)
    return balcony


def room_furniture(dom):                # check if the room furniture is present in the listing
    try:                                # try to check if the room furniture is present
        furnish_detial=dom.xpath("/html/body/div[1]/main/div[8]/div/div[1]/div[3]/div[2]/div[9]/span/text()")[0]
        print(furnish_detial)
    except Exception as e:              # if the room furnish is not present, print the error message
        furnish_detial="Details of room furnish is not available"
        print(furnish_detial)
    return furnish_detial


def get_est_rent(dom):                  # check if the estimate rent per month is in the listing
    try:                                # try to check if the estimate rent is present
        page_number = dom.xpath('//div[@class="new-investment-value"]/text()')
        striptext_list = [int(re.sub(r'[฿,/,%,mo]', '', i)) for i in page_number if any(c.isdigit() for c in i)]
        x = sorted(striptext_list, reverse=True)
        est_rent = (x[1])
        print(est_rent)
    except Exception as e:
        est_rent="Estimate rent is not available"
        print(est_rent)                 # if the estimate rent is not present, print the error message
    return est_rent

# write to csv file
file_exists = os.path.isfile('/code/data/bangkok_condo.csv')

with open('/code/data/bangkok_condo.csv', 'a', newline='') as f:
    thewriter = writer(f)
    if not file_exists:  # write header row only once
        heading = ['title', 'location', 'price', 'room_size_sqm', 'listing_date', 'cam_free_per_month', 'property_type', 'is_finish_construct', 'constructed_year', 'bedrooms', 'bathrooms', 'no_floors', 'balcony_view', 'room_furniture', 'estimate_rent_per_month', 'url']    
        thewriter.writerow(heading)

    # get the each link from the listing_url list
    for list_url in listing_url:
        # calling function to execute scraping data
        listing_dom = get_dom(list_url)
        title = get_title(listing_dom)  
        location = get_location(listing_dom)
        price = get_price(listing_dom)
        area = get_area(listing_dom)
        offer = offered_since(listing_dom)
        cam_free = get_cam_free(listing_dom)
        type = get_type(listing_dom)
        construction_type = get_construction_type(listing_dom)
        constructed_year = get_constructed_year(listing_dom)
        bedrooms = get_bedrooms(listing_dom)
        bathrooms = get_bathrooms(listing_dom)
        no_floors = get_no_floors(listing_dom)
        details_of_balcony = get_details_of_balcony(listing_dom)
        furniture = room_furniture(listing_dom)
        est_rent = get_est_rent(listing_dom)
        
        information = [title, location, price, area, offer, cam_free, type, construction_type, constructed_year, bedrooms, bathrooms, no_floors, details_of_balcony, furniture, est_rent, list_url]
        thewriter.writerow(information)
