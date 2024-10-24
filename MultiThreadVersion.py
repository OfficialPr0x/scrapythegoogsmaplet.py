import threading 
import streamlit as st
from dataclasses import dataclass, asdict, field
import pandas as pd
import os
import re
from typing import List, Optional, Dict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import logging
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import requests
from utils import get_proxies
from itertools import cycle
import undetected_chromedriver as uc
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from urllib.parse import urljoin
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
from threading import Thread
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
import multiprocessing

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

proxy_pool = cycle(get_proxies())

# Replace the fake-useragent import with a simple list of user agents
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
]

# Update the Business class to use proper dataclass syntax
@dataclass
class Business:
    """Business data container"""
    name: Optional[str] = None
    address: Optional[str] = None
    url: Optional[str] = None
    phone_number: Optional[str] = None
    email: Optional[str] = None
    reviews_count: Optional[int] = None
    reviews_average: Optional[float] = None
    social_media: Dict[str, str] = field(default_factory=dict)
    business_hours: Optional[str] = None
    categories: List[str] = field(default_factory=list)

class BusinessList:
    def __init__(self):
        self.business_list = []
        self.save_at = "output"

    def dataframe(self) -> pd.DataFrame:
        """Transform business_list to pandas dataframe"""
        return pd.json_normalize(
            (asdict(business) for business in self.business_list),
            sep="_",
            record_prefix="api_",
        )

    def save_to_excel(self, search_term: str) -> str:
        """Saves pandas dataframe to excel (xlsx) file"""
        if not os.path.exists(self.save_at):
            os.makedirs(self.save_at)
        filename = "{}_results".format(search_term.replace(' ', '_'))
        file_path = os.path.join(self.save_at, filename + ".xlsx")
        self.dataframe().to_excel(file_path, index=False)
        return file_path

    def save_to_csv(self, search_term: str) -> str:
        """Saves pandas dataframe to csv file"""
        if not os.path.exists(self.save_at):
            os.makedirs(self.save_at)
        filename = "{}_results".format(search_term.replace(' ', '_'))
        file_path = os.path.join(self.save_at, filename + ".csv")
        self.dataframe().to_csv(file_path, index=False)
        return file_path

# Add these constants at the top of the file
MAX_WORKERS = multiprocessing.cpu_count() * 2  # Number of worker threads
MAX_SEARCH_WORKERS = 1  # Single search thread to avoid duplicates
MAX_PROCESS_WORKERS = 3  # Three processing threads is optimal
QUEUE_TIMEOUT = 2  # Longer timeout to prevent issues

class BusinessQueue:
    def __init__(self, total_results):
        self.to_process = Queue(maxsize=total_results * 2)  # Double buffer
        self.processed = Queue(maxsize=total_results * 2)
        self.is_searching = True
        self.total_results = total_results
        self.processed_count = 0
        self.search_lock = threading.Lock()
        self.process_lock = threading.Lock()

def find_sidebar(driver):
    """Helper function to find and verify the sidebar"""
    for selector in [
        'div[role="feed"]',
        'div.m6QErb.DxyBCb.kA9KIf.dS8AEf',
        'div[aria-label*="Results"]'
    ]:
        try:
            element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
            if element.is_displayed():
                return element
        except:
            continue
    return None

def find_new_entries(driver, processed_names):
    """Find new business entries that haven't been processed yet."""
    entries = []
    entry_selectors = [
        'div[role="article"]',
        'div.Nv2PK',
        'a[href^="/maps/place"]'
    ]
    
    for selector in entry_selectors:
        try:
            new_entries = WebDriverWait(driver, 5).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
            )
            entries.extend(new_entries)
        except:
            continue
    
    new_entries_data = []
    for entry in entries:
        try:
            name = None
            for selector in [
                'span.fontHeadlineSmall',
                'div.qBF1Pd',
                'div[role="heading"]',
                'div.fontHeadlineSmall'
            ]:
                try:
                    name_element = entry.find_element(By.CSS_SELECTOR, selector)
                    name = name_element.text.strip()
                    if name:
                        break
                except:
                    continue
            
            if name and name not in processed_names:
                new_entries_data.append({
                    'name': name,
                    'index': len(processed_names)
                })
        except Exception as e:
            logging.error("Error processing entry: %s", str(e))  # Fixed logging syntax
            continue
    
    return new_entries_data

def parallel_search(driver, queue: BusinessQueue, search_id: int, search_term: str, location: str):
    """Single search thread to find and queue businesses"""
    logging.info("Search thread started")
    try:
        # Navigate to Google Maps with error handling
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                driver.get("https://www.google.com/maps")
                time.sleep(3)
                
                # Find and fill search box
                search_box = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "searchboxinput"))
                )
                search_box.clear()
                time.sleep(1)
                
                # Type search term slowly
                for char in f"{search_term} in {location}":
                    search_box.send_keys(char)
                    time.sleep(random.uniform(0.1, 0.3))
                
                time.sleep(1)
                
                # Click search button
                search_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "searchbox-searchbutton"))
                )
                driver.execute_script("arguments[0].click();", search_button)
                
                # Wait for results
                time.sleep(5)
                
                # Find results container
                sidebar = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="feed"]'))
                )
                
                processed_names = set()
                scroll_attempts = 0
                last_count = 0
                
                while queue.processed_count < queue.total_results and scroll_attempts < 5:
                    try:
                        # Scroll with dynamic wait
                        driver.execute_script(
                            'arguments[0].scrollTop = arguments[0].scrollHeight', 
                            sidebar
                        )
                        time.sleep(random.uniform(2, 3))
                        
                        # Find entries with multiple selectors
                        entries = []
                        for selector in ['div[role="article"]', 'div.Nv2PK']:
                            try:
                                new_entries = driver.find_elements(By.CSS_SELECTOR, selector)
                                entries.extend(new_entries)
                            except:
                                continue
                        
                        if not entries:
                            scroll_attempts += 1
                            continue
                        
                        new_entries_found = False
                        for entry in entries:
                            try:
                                name = None
                                for selector in [
                                    'div.fontHeadlineSmall',
                                    'span.fontHeadlineSmall',
                                    'div[role="heading"]'
                                ]:
                                    try:
                                        name_elem = entry.find_element(By.CSS_SELECTOR, selector)
                                        name = name_elem.text.strip()
                                        if name:
                                            break
                                    except:
                                        continue
                                
                                if name and name not in processed_names:
                                    queue.to_process.put({
                                        'name': name,
                                        'element': entry,
                                        'index': len(processed_names)
                                    })
                                    processed_names.add(name)
                                    new_entries_found = True
                                    logging.info("Added to queue: %s", name)
                            except Exception as e:
                                logging.error("Error processing entry: %s", str(e))
                                continue
                        
                        # Check if we're still finding new entries
                        current_count = len(processed_names)
                        if current_count == last_count:
                            scroll_attempts += 1
                        else:
                            scroll_attempts = 0
                            last_count = current_count
                            
                    except Exception as e:
                        logging.error("Error during search: %s", str(e))
                        scroll_attempts += 1
                        
                    time.sleep(random.uniform(1, 2))
                
                break  # Break out of retry loop if successful
                
            except Exception as e:
                logging.error("Search attempt %d failed: %s", attempt + 1, str(e))
                if attempt < max_attempts - 1:
                    time.sleep(random.uniform(2, 4))
                    driver.delete_all_cookies()
                    continue
                raise
                
    except Exception as e:
        logging.error("Search thread error: %s", str(e))
    finally:
        queue.is_searching = False
        logging.info("Search completed")

def parallel_process(driver, queue: BusinessQueue, session, process_id: int):
    """Parallel processing function for multiple process threads"""
    logging.info(f"Process thread {process_id} started")
    try:
        while queue.is_searching or not queue.to_process.empty():
            try:
                business_data = queue.to_process.get(timeout=QUEUE_TIMEOUT)
                if business_data:
                    business = process_business(driver, business_data, session)
                    if business:
                        queue.processed.put(business)
                        with queue.process_lock:
                            queue.processed_count += 1
                            logging.info(f"Process thread {process_id}: Processed {business.name}")
            except Empty:
                time.sleep(0.5)
                continue 
            except Exception as e:
                logging.error("Process thread %d error: %s", process_id, str(e))
                continue 
    except Exception as e:
        logging.error("Process thread %d error: %s", process_id, str(e))

# Create a session manager for requests
def create_scraper_session():
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    # Use smaller pool size to prevent connection issues
    adapter = HTTPAdapter(
        pool_connections=10,
        pool_maxsize=10,
        max_retries=retry_strategy,
        pool_block=False
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

# Update the email extraction function
def extract_email_from_website(url, max_retries=3, session=None):
    """Enhanced email extraction with connection pool management"""
    if not url:
        return None
        
    if session is None:
        session = create_scraper_session()
    
    # Rotate user agents
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    email_patterns = [
        r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}',  # Standard email
        r'mailto:[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}',  # Mailto links
        r'[A-Za-z0-9._%+-]+\s*[\[\(]\s*at\s*[\]\)]\s*[A-Za-z0-9.-]+\s*[\[\(]\s*dot\s*[\]\)]\s*[A-Z|a-z]{2,}',  # Protected emails
    ]
    
    contact_pages = [
        '/contact', '/contact-us', '/about', '/about-us', 
        '/reach-us', '/get-in-touch', '/connect'
    ]
    
    emails = set()
    
    def clean_email(email):
        """Clean and validate email address"""
        email = email.lower().strip()
        email = email.replace('mailto:', '')
        email = email.split('?')[0]  # Remove parameters
        if '@' in email and '.' in email.split('@')[1]:
            return email
        return None
    
    def is_valid_email(email):
        """Additional email validation"""
        if not email:
            return False
        
        # Check for common false positives
        invalid_domains = [
            'example.com', 'domain.com', 'email.com', 'wordpress',
            'yourdomain', 'company.com', 'website.com'
        ]
        
        return not any(domain in email.lower() for domain in invalid_domains)
    
    for attempt in range(max_retries):
        try:
            # Add random delay between requests
            time.sleep(random.uniform(1, 3))
            
            # Try main page
            response = session.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Method 1: Direct regex search
            for pattern in email_patterns:
                found_emails = re.findall(pattern, soup.text)
                emails.update(clean_email(email) for email in found_emails)
            
            # Method 2: Check mailto links
            mailto_links = soup.find_all('a', href=re.compile(r'^mailto:'))
            for link in mailto_links:
                href = link.get('href', '')
                email = clean_email(href)
                if email:
                    emails.add(email)
            
            # Method 3: Check contact pages with delay between requests
            base_url = url.rstrip('/')
            for contact_page in contact_pages:
                try:
                    time.sleep(random.uniform(0.5, 1.5))
                    contact_url = f"{base_url}{contact_page}"
                    contact_response = session.get(contact_url, headers=headers, timeout=5)
                    if contact_response.status_code == 200:
                        contact_soup = BeautifulSoup(contact_response.text, 'html.parser')
                        
                        # Search for emails in contact page
                        for pattern in email_patterns:
                            found_emails = re.findall(pattern, contact_soup.text)
                            emails.update(clean_email(email) for email in found_emails)
                except:
                    continue
            
            # Filter and validate emails
            valid_emails = {email for email in emails if is_valid_email(email)}
            
            if valid_emails:
                logging.info(f"Found {len(valid_emails)} valid emails for {url}")
                return list(valid_emails)[0]  # Return first valid email
                
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(2, 4))
            continue
    
    return None

def extract_additional_info(url):
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        social_media = {
            'facebook': soup.find('a', href=re.compile(r'facebook\.com')),
            'twitter': soup.find('a', href=re.compile(r'twitter\.com')),
            'instagram': soup.find('a', href=re.compile(r'instagram\.com')),
            'linkedin': soup.find('a', href=re.compile(r'linkedin\.com'))
        }
        social_media = {k: v['href'] if v else None for k, v in social_media.items()}
        
        business_hours = soup.find('div', class_=re.compile(r'hours|schedule|time'))
        business_hours = business_hours.text.strip() if business_hours else None
        
        categories = [tag.text for tag in soup.find_all('a', href=re.compile(r'category|tag'))]
        
        return social_media, business_hours, categories
    except Exception as e:
        logging.error(f"Error extracting additional info from {url}: {e}")
        return {}, None, []

def search_businesses(driver, queue: BusinessQueue):
    """Continuously search for new businesses until target count is reached"""
    try:
        logging.info("Waiting for results to load...")
        time.sleep(5)
        
        def find_sidebar(): 
            """Helper function to find and verify the sidebar"""
            for selector in [
                'div[role="feed"]',
                'div.m6QErb.DxyBCb.kA9KIf.dS8AEf',
                'div[aria-label*="Results"]'
            
            ]:
                try:
                    element = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    if element.is_displayed():
                        return element
                except:
                    continue
            return None

        processed_names = set()
        scroll_attempts = 0 
        max_scroll_attempts = 3
        
        while queue.processed_count < queue.total_results:
            # Refresh sidebar reference on each iteration
            sidebar = find_sidebar()
            if not sidebar:
                logging.error("Lost sidebar reference, retrying...")
                time.sleep(2) 
                continue
                
            try:
                # Scroll with retry mechanism
                for _ in range(3):  # Try scrolling up to 3 times
                    try:
                        driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', sidebar)
                        time.sleep(2)  # Wait for content to load
                        break 
                    except:
                        sidebar = find_sidebar()  # Refresh sidebar reference
                        time.sleep(1)
                
                # Find business entries with retry mechanism
                entries = []
                entry_selectors = [
                    'div[role="article"]',
                    'div.Nv2PK',
                    'a[href^="/maps/place"]'
                ]
                
                for selector in entry_selectors:
                    try:
                        new_entries = WebDriverWait(driver, 5).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                        )
                        entries.extend(new_entries) 
                    except:
                        continue
                
                if not entries:
                    scroll_attempts += 1
                    if scroll_attempts >= max_scroll_attempts:
                        logging.info("No more entries found after multiple attempts")
                        queue.is_searching = False 
                        break
                    continue
                
                # Process entries
                new_entries = 0
                for entry in entries:
                    if queue.processed_count >= queue.total_results:
                        queue.is_searching = False 
                        return
                        
                    try:
                        # Get business name with retry
                        name = None
                        for selector in [
                            'span.fontHeadlineSmall',
                            'div.qBF1Pd',
                            'div[role="heading"]',
                            'div.fontHeadlineSmall'
                        ]:
                            try:
                                name_element = WebDriverWait(driver, 2).until(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                                )
                                name = name_element.text.strip()
                                if name:
                                    break
                            except:
                                continue
                        
                        if name and name not in processed_names:
                            # Store minimal data to avoid stale references
                            queue.to_process.put({
                                'name': name,
                                'index': len(processed_names)
                            })
                            processed_names.add(name)
                            new_entries += 1 
                            logging.info(f"Added {name} to queue")
                    except Exception as e:
                        logging.error(f"Error processing entry: {str(e)}")
                        continue
                
                logging.info(f"Added {new_entries} new businesses to process (Total: {len(processed_names)})")
                
                if new_entries == 0:
                    scroll_attempts += 1
                else:
                    scroll_attempts = 0
                
                if scroll_attempts >= max_scroll_attempts:
                    logging.info("Reached end of results list") 
                    queue.is_searching = False
                    break
                
            except Exception as e:
                logging.error(f"Error during scroll iteration: {str(e)}")
                scroll_attempts += 1
                if scroll_attempts >= max_scroll_attempts:
                    break 
                time.sleep(2)
                continue 
            
    except Exception as e: 
        logging.error(f"Error in search thread: {str(e)}", exc_info=True)
        queue.is_searching = False

def verify_business_data(business: Business, existing_businesses: List[Business]) -> bool:
    """Verify business data is unique and valid"""
    if not business.name or not business.address:
        return False
    
    # Check for duplicates
    for existing in existing_businesses:
        if (business.name == existing.name and 
                business.address == existing.address):
            return False
            
        # Check for similar names to catch variations
        if (business.name and existing.name and
            (business.name in existing.name or existing.name in business.name) and
            business.address == existing.address):
            return False
    
    return True

def process_business(driver, business_data, session):
    """Process a single business entry"""
    name = business_data['name']
    
    try:
        logging.info("Processing: %s", name)
        
        # Find and click the business entry using multiple strategies
        entry = None
        try:
            # Try exact match first
            entry = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, 
                    f"//div[normalize-space(text())='{name}']"
                ))
            )
        except:
            try:
                # Try contains match
                entry = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, 
                        f"//div[contains(text(), '{name}')]"
                    ))
                )
            except:
                # Try clicking parent element
                entries = driver.find_elements(By.CSS_SELECTOR, 'div[role="article"]')
                for e in entries:
                    if name in e.text:
                        entry = e
                        break
        
        if not entry:
            logging.error("Could not find entry for: %s", name)
            return None
            
        # Click with retry mechanism
        click_successful = False
        for _ in range(3):
            try:
                driver.execute_script("arguments[0].scrollIntoView(true);", entry)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", entry)
                click_successful = True
                break
            except:
                try:
                    ActionChains(driver).move_to_element(entry).click().perform()
                    click_successful = True
                    break
                except:
                    time.sleep(1)
        
        if not click_successful:
            logging.error("Failed to click: %s", name)
            return None
            
        time.sleep(2)
        
        # Extract business details
        details = {}
        
        try:
            details['address'] = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 
                    'button[data-item-id*="address"]'
                ))
            ).text.strip()
        except:
            details['address'] = None

        try:
            details['phone'] = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 
                    'button[data-item-id*="phone"]'
                ))
            ).text.strip()
        except:
            details['phone'] = None

        try:
            website_elem = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 
                    'a[data-item-id*="authority"]'
                ))
            )
            details['website'] = website_elem.get_attribute('href')
            
            if details['website']:
                details['email'] = extract_email_from_website(
                    details['website'], 
                    session=session
                )
            else:
                details['email'] = None
        except:
            details['website'] = None
            details['email'] = None

        # Go back to results
        try:
            ActionChains(driver).send_keys(Keys.ESCAPE).perform()
        except:
            pass
        time.sleep(1)

        # Create business object
        business = Business(
            name=name,
            address=details['address'],
            url=details['website'],
            phone_number=details['phone'],
            email=details['email']
        )
        
        if business.name and business.address:
            logging.info("Successfully processed: %s", name)
            return business
            
        return None
        
    except Exception as e:
        logging.error("Error processing %s: %s", name, str(e))
        try:
            ActionChains(driver).send_keys(Keys.ESCAPE).perform()
        except:
            pass
        time.sleep(1)
        return None

def create_driver_with_options():
    """Create a new Chrome driver with optimized options"""
    options = uc.ChromeOptions()
    
    # Essential options for stability and anti-detection
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-notifications')
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--start-maximized')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-default-apps')
    options.add_argument(f'--user-agent={random.choice(USER_AGENTS)}')
    
    # Additional preferences
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    
    try:
        driver = uc.Chrome(options=options, version_main=119)  # Specify Chrome version
        driver.set_page_load_timeout(30)
        return driver
    except Exception as e:
        logging.error("Error creating driver: %s", str(e))
        raise

def get_business_data(search_query: str, location: str, total_results: int, progress_callback=None):
    """Main function to get business data"""
    drivers = []
    sessions = []
    
    # Create browser instances with retry
    for i in range(MAX_PROCESS_WORKERS):
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                driver = create_driver_with_options()
                session = create_scraper_session()
                logging.info(f"Created browser instance {i+1}")
                drivers.append(driver)
                sessions.append(session)
                time.sleep(2)  # Delay between driver creation
                break
            except Exception as e:
                logging.error(f"Error creating browser instance {i+1} (attempt {attempt+1}): {str(e)}")
                if attempt == max_attempts - 1:
                    raise
                time.sleep(random.uniform(2, 4))
    
    if not drivers:
        raise Exception("Failed to create any browser instances")
    
    try:
        # Initialize queue and results
        queue = BusinessQueue(total_results)
        business_list = BusinessList()
        processed_businesses = []
        
        # Start search thread
        search_thread = Thread(
            target=parallel_search,
            args=(drivers[0], queue, 0, search_query, location)
        )
        search_thread.daemon = True
        search_thread.start()
        
        # Start processing threads
        process_threads = []
        for i in range(len(drivers)):
            thread = Thread(
                target=parallel_process,
                args=(drivers[i], queue, sessions[i], i)
            )
            thread.daemon = True
            thread.start()
            process_threads.append(thread)
        
        # Monitor progress with timeout
        start_time = time.time()
        timeout = 300  # 5 minutes timeout
        
        while (time.time() - start_time < timeout and 
               (search_thread.is_alive() or not queue.to_process.empty() or 
                any(t.is_alive() for t in process_threads))):
            try:
                if not queue.processed.empty():
                    business = queue.processed.get_nowait()
                    if business and business not in processed_businesses:
                        business_list.business_list.append(business)
                        processed_businesses.append(business)
                        if progress_callback:
                            progress_callback({
                                'count': len(business_list.business_list),
                                'name': business.name,
                                'df': business_list.dataframe()
                            })
                
                if len(business_list.business_list) >= total_results:
                    queue.is_searching = False
                    break
                
                time.sleep(0.1)
                
            except Empty:
                continue
            except Exception as e:
                logging.error("Error in main loop: %s", str(e))
                continue
        
        return business_list
        
    finally:
        # Clean up drivers
        for driver in drivers:
            try:
                driver.quit()
                time.sleep(0.5)
            except:
                pass

def main():
    st.title("Google Maps Business Scraper")

    locations = [
        "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", 
        "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", 
        "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", 
        "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", 
        "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", 
        "New Hampshire", "New Jersey", "New Mexico", "New York",
        "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", 
        "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", 
        "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", 
        "West Virginia", "Wisconsin", "Wyoming"
    ]
    
    search_term = st.text_input("Enter search term:")
    location = st.selectbox("Select location:", locations) 
    total_results = st.number_input("Number of businesses to scrape:", min_value=1, max_value=5000, value=100) 

    results_placeholder = st.empty()
    progress_text = st.empty()
    progress_bar = st.empty()

    if st.button("Start Scraping"):
        if search_term and location:
            try:
                progress_text.text("Starting scraper...")
                progress_bar.progress(0) 

                business_list = get_business_data(
                    search_term,
                    location,
                    total_results, 
                    progress_callback=lambda x: update_progress(
                        x,
                        progress_text,
                        progress_bar,
                        results_placeholder,
                        total_results
                    )
                )
                
                # Show download buttons when done 
                if business_list.business_list:
                    st.success(f"Completed! Found {len(business_list.business_list)} businesses") 

                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("Download Excel"):
                            excel_file = business_list.save_to_excel(f"{search_term}_{location}") 
                            with open(excel_file, "rb") as file:
                                st.download_button(
                                    label="Click to Download Excel",
                                    data=file, 
                                    file_name=f"{search_term}_{location}_results.xlsx", 
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                )

                    with col2:
                        if st.button("Download CSV"):
                            csv_file = business_list.save_to_csv(f"{search_term}_{location}") 
                            with open(csv_file, "rb") as file:
                                st.download_button(
                                    label="Click to Download CSV",
                                    data=file, 
                                    file_name=f"{search_term}_{location}_results.csv", 
                                    mime="text/csv"
                                )
                            
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
        else:
            st.error("Please enter a search term and select a location.")
    
def update_progress(current_business, progress_text, progress_bar, results_placeholder, total_results):
    """Update Streamlit progress and display current results"""
    if current_business:
        count = current_business.get('count', 0) 
        # Update progress bar
        progress = min(count / total_results, 1.0) 
        progress_bar.progress(progress) 

        # Update status text
        progress_text.text(f"Processing: {current_business.get('name', 'Unknown Business')} " +
                         f"(Found {count}/{total_results} businesses)") 

        # Update results table if we have data
        if 'df' in current_business:
            results_placeholder.dataframe(current_business['df']) 

if __name__ == "__main__":
    main()
