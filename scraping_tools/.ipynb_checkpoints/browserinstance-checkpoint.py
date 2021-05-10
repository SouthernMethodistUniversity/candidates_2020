import sys, time, os, tempfile, pathlib, tarfile, hashlib, shutil, glob
from stem.process import launch_tor_with_config
from selenium.webdriver.common.utils import free_port
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium import webdriver
import Xlib.display
from pyvirtualdisplay.smartdisplay import SmartDisplay
import pyautogui
from bs4 import BeautifulSoup

class BrowserInstance:
    def __init__(self, tor=True, tor_tries=10, save_directory="data", url_delay=5):
        self.tor = tor
        self.tor_tries = tor_tries
        self.save_directory = save_directory
        self.url_delay = url_delay
        pathlib.Path(self.save_directory).mkdir(parents=True, exist_ok=True)
        self.display = SmartDisplay(backend='xephyr', visible = 1, size =(1920, 1080))
        self.display.start()
        self.firefox_profile_path = tempfile.mkdtemp()
        self.firefox_profile = webdriver.FirefoxProfile(self.firefox_profile_path)
        if self.tor:
            for i in range(self.tor_tries):
                try:
                    self.socks_port = free_port()
                    self.control_port = free_port()
                    self.tor_data_dir = tempfile.mkdtemp()
                    self.torrc = {'ControlPort': str(self.control_port), 'SOCKSPort': str(self.socks_port), 'DataDirectory': self.tor_data_dir}
                    self.tor_process = launch_tor_with_config(config=self.torrc, tor_cmd="/usr/sbin/tor")
                except:
                    print("Tor connection attempt {} failed.".format(i))
                    sys.stdout.flush()
                    if i == self.tor_tries - 1:
                        sys.exit()
                    continue
                break
            self.firefox_profile.set_preference("network.proxy.type", 1)
            self.firefox_profile.set_preference("network.proxy.socks", "localhost")
            self.firefox_profile.set_preference("network.proxy.socks_port", self.socks_port)
            self.firefox_profile.set_preference("network.proxy.socks_version", 5)
        self.firefox_profile.update_preferences()    
        self.driver = webdriver.Firefox(service_log_path=os.path.join(self.firefox_profile_path, "geckodriver.log"), firefox_profile=self.firefox_profile)
        self.driver.maximize_window()
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.cleanup()
    def cleanup(self):
        self.display.stop()
        shutil.rmtree(self.firefox_profile_path)
        if self.tor:
            self.tor_process.kill()
            shutil.rmtree(self.tor_data_dir)
    def print_ports(self):
        print("SOCKS Port: {}, Control Port: {}".format(self.socks_port, self.control_port))
    def save_page(self):
        pyautogui._pyautogui_x11._display = Xlib.display.Display(os.environ['DISPLAY'])
        tmp_page_directory = tempfile.mkdtemp()
        pyautogui.PAUSE = self.url_delay
        pyautogui.hotkey('alt', 'tab')
        pyautogui.hotkey('ctrl', 's')
        pyautogui.write(os.path.join(tmp_page_directory, "page"))
        pyautogui.press('enter')
        time.sleep(self.url_delay)
        tmp_tar_directory = tempfile.mkdtemp()
        archive_name = os.path.join(tmp_tar_directory, "tarball.txz")
        with tarfile.open(archive_name, "w:xz") as tar:
            tar.add(tmp_page_directory)
        h = hashlib.sha256()
        with open(archive_name, 'rb') as f:
            h.update(f.read())
        archive_hash = h.hexdigest()
        shutil.copy(archive_name, os.path.join(self.save_directory, "{}.txz".format(archive_hash)))
        shutil.rmtree(tmp_page_directory)
        shutil.rmtree(tmp_tar_directory)
        return archive_hash
    def scroll_page(self):
        height = 0
        height_increment = 500
        total_height = int(self.driver.execute_script("return document.body.scrollHeight"))
        while (height <= total_height):
            self.driver.execute_script("window.scrollTo({}, {});".format(height, height+height_increment))
            time.sleep(self.url_delay)
            height += height_increment
            total_height = int(self.driver.execute_script("return document.body.scrollHeight"))
    def get_url(self, url):
        try:
            self.driver.get(url)
            time.sleep(self.url_delay)
            self.scroll_page()
            self.url_tries_cound = 0
            return BeautifulSoup(self.driver.page_source, "html.parser")
        except:
            print("URL Connection Error: {}".format(url))
            sys.stdout.flush()
            return False
    def get_page_from_archive(self, archive_file):
        tmp_page_directory = tempfile.mkdtemp()
        os.chdir(tmp_page_directory)
        with tarfile.open(archive_file, "r:xz") as tar:
            tar.extractall(tmp_page_directory)
        url = glob.glob('**/page.html', recursive=True)
        if url:
            url = "file://" + tmp_page_directory + "/" + url[0]
            self.driver.get(url)
            s = BeautifulSoup(self.driver.page_source, "html.parser")
        else:
            s = False
        os.chdir(self.save_directory)
        shutil.rmtree(tmp_page_directory)
        return s

