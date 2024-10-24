import streamlit as st
from dataclasses import dataclass, asdict, field
import pandas as pd
import os
import re
from typing import List, Optional
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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

proxy_pool = cycle(get_proxies())

# Fix the Business dataclass by using proper Python type hints
@dataclass
class Business:
    name: Optional[str] = field(default=None)
    address: Optional[str] = field(default=None)
    url: Optional[str] = field(default=None)
    phone_number: Optional[str] = field(default=None)
    email: Optional[str] = field(default=None)
    reviews_count: Optional[int] = field(default=None)
    reviews_average: Optional[float] = field(default=None)
    social_media: dict = field(default_factory=dict)
    business_hours: Optional[str] = field(default=None)
    categories: list = field(default_factory=list)

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

def extract_email_from_website(url):
    if not url:
        return None
        
    try:
        # Try direct website first
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for emails in various ways
        email_regex = r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}'
        emails = set()  # Use set to avoid duplicates
        
        # Method 1: Direct text search
        text_emails = re.findall(email_regex, soup.text)
        emails.update(text_emails)
        
        # Method 2: Check mailto links
        mailto_links = soup.find_all('a', href=re.compile(r'^mailto:'))
        for link in mailto_links:
            href = link.get('href', '')
            email = href.replace('mailto:', '').split('?')[0]
            if re.match(email_regex, email):
                emails.add(email)
                
        # Method 3: Check contact page if exists
        contact_links = soup.find_all('a', href=re.compile(r'contact|about', re.I))
        for link in contact_links:
            try:
                contact_url = urljoin(url, link['href'])
                contact_response = requests.get(contact_url, timeout=5)
                contact_soup = BeautifulSoup(contact_response.text, 'html.parser')
                contact_emails = re.findall(email_regex, contact_soup.text)
                emails.update(contact_emails)
            except:
                continue
                
        # Filter out common false positives
        filtered_emails = {
            email for email in emails 
            if not any(false_positive in email.lower() for false_positive in [
                'example.com', 'domain.com', 'email.com', 'wordpress'
            ])
        }
        
        return list(filtered_emails)[0] if filtered_emails else None
        
    except Exception as e:
        logging.error(f"Error extracting email from {url}: {str(e)}")
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
        logging.error(f"Error extracting additional info from {url}: {str(e)}")
        return {}, None, []

def get_business_data(search_query, location, total, progress_callback):
    options = uc.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    driver = None
    try:
        logging.info("Initializing Chrome driver...")
        driver = uc.Chrome(options=options)
        
        logging.info("Navigating to Google Maps...")
        driver.get("https://www.google.com/maps")
        time.sleep(3)

        logging.info("Looking for search box...")
        search_box = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "searchboxinput"))
        )
        search_box.clear()
        search_query_full = f"{search_query} in {location}"
        logging.info(f"Searching for: {search_query_full}")
        search_box.send_keys(search_query_full)
        time.sleep(1)
        search_box.send_keys(Keys.ENTER)

        logging.info("Waiting for results to load...")
        time.sleep(5)

        business_list = BusinessList()
        processed_names = set()

        logging.info("Looking for results panel...")
        # Wait longer for the results panel and make sure it's visible
        try:
            sidebar = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="feed"]'))
            )
        except TimeoutException:
            try:
                # Alternative selector
                sidebar = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.m6QErb'))
                )
            except TimeoutException:
                logging.error("Could not find results panel")
                return BusinessList()
        
        while len(business_list.business_list) < total:
            # Find all business entries in the sidebar
            try:
                entries = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.Nv2PK, div[role="article"]'))
                )
                logging.info(f"Found {len(entries)} business entries")
            except TimeoutException:
                logging.warning("No entries found, trying alternative selector...")
                try:
                    entries = driver.find_elements(By.CSS_SELECTOR, 'a[href^="/maps/place"]')
                    logging.info(f"Found {len(entries)} business entries with alternative selector")
                except:
                    entries = []

            if not entries:
                logging.info("No entries found, waiting and trying again...")
                time.sleep(2)
                continue

            for entry in entries:
                if len(business_list.business_list) >= total:
                    break

                try:
                    # Scroll the entry into view
                    driver.execute_script("arguments[0].scrollIntoView(true);", entry)
                    time.sleep(1)

                    # Get business name before clicking
                    try:
                        name = entry.find_element(By.CSS_SELECTOR, 'span.fontHeadlineSmall').text.strip()
                    except:
                        try:
                            name = entry.find_element(By.CSS_SELECTOR, 'div.qBF1Pd').text.strip()
                        except:
                            continue

                    if not name or name in processed_names:
                        continue

                    logging.info(f"Processing: {name}")

                    # Click the entry
                    try:
                        ActionChains(driver).move_to_element(entry).click().perform()
                    except:
                        try:
                            entry.click()
                        except:
                            driver.execute_script("arguments[0].click();", entry)
                    time.sleep(3)

                    # Extract business details
                    try:
                        address = WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, 'button[data-item-id*="address"]'))
                        ).text
                    except:
                        address = None

                    try:
                        phone = driver.find_element(By.CSS_SELECTOR, 'button[data-item-id*="phone"]').text
                    except:
                        phone = None

                    try:
                        website = driver.find_element(By.CSS_SELECTOR, 'a[data-item-id*="authority"]').get_attribute('href')
                        # Extract email if we have a website
                        if website:
                            logging.info(f"Extracting email from website: {website}")
                            email = extract_email_from_website(website)
                            if email:
                                logging.info(f"Found email: {email}")
                        else:
                            email = None
                    except:
                        website = None
                        email = None

                    try:
                        rating = driver.find_element(By.CSS_SELECTOR, 'span.fontDisplayLarge').text
                        rating = float(rating) if rating else None
                    except:
                        rating = None

                    try:
                        reviews_text = driver.find_element(By.CSS_SELECTOR, 'span.fontBodyMedium span').text
                        reviews_count = int(''.join(filter(str.isdigit, reviews_text)))
                    except:
                        reviews_count = None

                    try:
                        hours_element = driver.find_element(By.CSS_SELECTOR, 'div[aria-label*="Hours"]')
                        business_hours = hours_element.text
                    except:
                        business_hours = None

                    try:
                        categories_elements = driver.find_elements(By.CSS_SELECTOR, 'button[jsaction*="category"]')
                        categories = [cat.text for cat in categories_elements]
                    except:
                        categories = []

                    # Create business object with email
                    business = Business(
                        name=name,
                        address=address,
                        url=website,
                        phone_number=phone,
                        email=email,  # Add email here
                        reviews_count=reviews_count,
                        reviews_average=rating,
                        business_hours=business_hours,
                        categories=categories
                    )

                    # Add to our list if we got meaningful data
                    if name and (address or phone or website):
                        business_list.business_list.append(business)
                        processed_names.add(name)
                        logging.info(f"Added business: {name}")
                        logging.info(f"Details: Address={address}, Phone={phone}, Rating={rating}")

                    # Go back to results list
                    try:
                        back_button = WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, 'button[jsaction*="back"]'))
                        )
                        back_button.click()
                    except:
                        # Alternative: press ESC key
                        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                    time.sleep(2)

                except Exception as e:
                    logging.error(f"Error processing listing: {str(e)}")
                    continue

            # Only try to scroll if we found entries and need more
            if len(business_list.business_list) < total and entries:
                try:
                    # Scroll the last entry into view
                    last_entry = entries[-1]
                    driver.execute_script("arguments[0].scrollIntoView(true);", last_entry)
                    time.sleep(2)
                except Exception as e:
                    logging.error(f"Error scrolling: {str(e)}")
                    break

            # Break if we're not finding any new entries
            if not entries:
                logging.info("No more entries found")
                break

        logging.info(f"Scraping completed. Found {len(business_list.business_list)} businesses")
        return business_list

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)
        return BusinessList()
    finally:
        if driver:
            try:
                driver.quit()
                time.sleep(1)  # Give it time to clean up
                driver = None  # Clear the reference
            except Exception as e:
                logging.error(f"Error closing driver: {str(e)}")
                try:
                    # Force kill the driver if normal quit fails
                    driver.close()
                    driver = None
                except:
                    pass

def main():
    st.title("Google Maps Business Scraper")

    # List of US states and Canadian provinces
    locations = [
        "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida",
        "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine",
        "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska",
        "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
        "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas",
        "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming",
        "Alberta", "British Columbia", "Manitoba", "New Brunswick", "Newfoundland and Labrador", "Nova Scotia",
        "Ontario", "Prince Edward Island", "Quebec", "Saskatchewan"
    ]

    search_term = st.text_input("Enter search term:")
    location = st.selectbox("Select location:", locations)
    total = st.number_input("Total results to scrape:", min_value=1, value=100, max_value=5000)

    # Create placeholder for live results
    if 'results_df' not in st.session_state:
        st.session_state.results_df = None
    
    results_placeholder = st.empty()
    progress_bar = st.empty()
    status_text = st.empty()

    if st.button("Scrape"):
        if search_term and location:
            try:
                # Initialize progress
                progress = progress_bar.progress(0)
                status_text.text("Starting scraper...")
                
                # Create a queue for live updates
                business_list = get_business_data(
                    search_term, 
                    location, 
                    total,
                    progress_callback=lambda x: update_progress(
                        x, 
                        progress, 
                        status_text, 
                        results_placeholder,
                        total
                    )
                )
                
                # Final results
                if business_list.business_list:
                    df = business_list.dataframe()
                    st.session_state.results_df = df
                    results_placeholder.dataframe(df)
                    
                    # Download buttons
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

def update_progress(current_business, progress_bar, status_text, results_placeholder, total):
    """Update Streamlit progress and display current results"""
    if current_business:
        # Update progress
        progress = min(current_business.get('count', 0) / total, 1.0)
        progress_bar.progress(progress)
        
        # Update status
        status_text.text(f"Processing: {current_business.get('name', 'Unknown Business')}")
        
        # Update results table if we have data
        if 'df' in current_business:
            results_placeholder.dataframe(current_business['df'])

if __name__ == "__main__":
    main()
