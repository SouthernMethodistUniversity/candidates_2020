import sys
from bs4 import BeautifulSoup
#from scraping_tools/browserinstance import BrowserInstance

import sys, os
cdir = os.getcwd()
sys.path.insert(0, cdir+'/scraping_tools')
from browserinstance import BrowserInstance

url = sys.argv[1]
save_directory = sys.argv[2]
with BrowserInstance(tor=True, save_directory=save_directory) as browser:
    s = browser.get_url("https://www.google.com")
    s = browser.get_url(url)
    if s:
        value = browser.save_page()
    else:
        value = "fail"
    print("{},{}".format(url, value))

