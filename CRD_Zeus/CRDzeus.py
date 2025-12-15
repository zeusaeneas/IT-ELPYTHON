import json
import time
import random
import re
import html
import warnings
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Suppress warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

class KMTScraper:
    def __init__(self):
        self.base_url = "https://kmt.vander-lingen.nl"
        self.archive_url = f"{self.base_url}/archive"
        self.output_file = "kmtOutput_Zeus.json"
        self.max_papers = 4  # Set to 0 to scrape ALL papers
        self.driver = self.setup_chrome_driver()
        self.session = self.configure_requests_session()

    def setup_chrome_driver(self):
        """Initializes the Chrome driver with performance optimizations."""
        opts = webdriver.ChromeOptions()
        # Disable images for speed
        opts.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
        opts.page_load_strategy = 'eager'
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        return webdriver.Chrome(options=opts)

    def configure_requests_session(self):
        """Initializes requests session with Retry logic for stability."""
        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def transfer_selenium_cookies(self):
        """Transfers cookies from Selenium to the Requests session."""
        self.session.cookies.clear()
        for cookie in self.driver.get_cookies():
            self.session.cookies.set(cookie['name'], cookie['value'])
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": self.driver.current_url
        })

    def parse_chemical_data(self, raw_content):
        """Extracts chemical data using Regex."""
        data = {'reaction_smiles': None, 'molecules': []}
        try:
            # Extract Reaction SMILES
            rxn_match = re.search(r'<reactionSmiles>(.*?)</reactionSmiles>', raw_content, re.DOTALL)
            if rxn_match:
                # CHANGED: Renamed key here as well for internal consistency
                data['reaction_smiles'] = html.unescape(rxn_match.group(1).strip())

            # Extract Molecules
            molecules = re.findall(r'<molecule>(.*?)</molecule>', raw_content, re.DOTALL)
            
            def extract_tag(tag, text):
                m = re.search(f'<{tag}>(.*?)</{tag}>', text, re.DOTALL)
                return html.unescape(m.group(1).strip()) if m else None

            for mol_block in molecules:
                data['molecules'].append({
                    'role': extract_tag('role', mol_block),
                    'smiles': extract_tag('smiles', mol_block),
                    'name': extract_tag('name', mol_block),
                })
        except Exception as e:
            print(f"    [Data Parse Error] {e}")
        return data

    def collect_archive_urls(self):
        """Scrapes the main archive page for paper links."""
        print(f"Loading Archive: {self.archive_url}")
        self.driver.get(self.archive_url)
        WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        papers = []
        links = self.driver.find_elements(By.PARTIAL_LINK_TEXT, "reaction data")
        
        for link in links:
            try:
                href = link.get_attribute("href")
                title = link.find_element(By.XPATH, "./..").text
                papers.append({"url": href, "title": title})
            except:
                continue
                
        print(f"Found {len(papers)} papers.")
        return papers[:self.max_papers] if self.max_papers > 0 else papers

    def mine_paper_data(self, paper, idx, total):
        """Handles the pagination and details fetching for a specific paper."""
        current_url = paper['url']
        
        # We only return the list of reactions now
        extracted_reactions = []

        # CHANGED: Now prints the URL instead of the title
        print("-"*60)
        print(f"\n[{idx}/{total}] Processing: {current_url}")

        previous_detail_links = [] 
        visited_pages = set()
        scanned_count = 0

        try:
            page_count = 1
            while current_url:
                if current_url in visited_pages:
                    print(f"   [Stop] Loop detected. Already visited {current_url}")
                    break
                visited_pages.add(current_url)

                self.driver.get(current_url)
                time.sleep(random.uniform(0.5, 1.5))

                try:
                    WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                except: 
                    print("   [Error] Page load timeout.")
                    break

                # 3. Get all 'Details' links using the robust CSS Selector
                detail_elements = self.driver.find_elements(By.CSS_SELECTOR, "a.btn.btn-outline-info[id^='title-']")
                
                detail_links = []
                for elem in detail_elements:
                    href = elem.get_attribute('href')
                    if href and "Details" in elem.text:
                        detail_links.append(href)

                if detail_links == previous_detail_links:
                    print("   [Stop] Page content is identical to the previous page. Scanning finished.")
                    break
                
                previous_detail_links = detail_links[:] 

                if not detail_links:
                    print("   [Stop] No reactions found on this page.")
                    break

                # 4. Check for Next Page
                next_page = None
                try:
                    nav_btns = self.driver.find_elements(By.XPATH, "//a[contains(text(), 'Next') or contains(text(), '>')]")
                    for btn in nav_btns:
                        href = btn.get_attribute('href')
                        if href and "start" in href and href not in visited_pages:
                            next_page = href
                            break
                except: pass

                # 5. Process Details
                self.transfer_selenium_cookies() 
                print(f"   Page {page_count}: Found {len(detail_links)} reactions. Processing...")

                for d_url in detail_links:
                    rxn_data = self.fetch_reaction_data(d_url)
                    if rxn_data:
                        extracted_reactions.append(rxn_data)
                        scanned_count += 1

                if scanned_count > 200:
                    print("   [Limit Reached] Stopping at 200 entries for this paper.")
                    break

                if not next_page:
                    print("   [Stop] No valid 'Next' button found.")
                    break
                    
                current_url = next_page
                page_count += 1

        except Exception as e:
            print(f"   [Error] {str(e)}")
            
        return extracted_reactions

    def fetch_reaction_data(self, detail_url):
        """Fetches HTML via Requests, finds data link, and parses it."""
        try:
            resp = self.session.get(detail_url, timeout=15)
            if resp.status_code != 200: return None

            soup = BeautifulSoup(resp.text, 'html.parser')
            # Searching for the anchor text that leads to the raw data file
            source_anchor = soup.find('a', string="XML")

            if source_anchor and source_anchor.get('href'):
                source_url = source_anchor.get('href')
                if source_url.startswith("/"):
                    source_url = self.base_url + source_url

                source_resp = self.session.get(source_url, timeout=15)
                if source_resp.status_code == 200:
                    parsed = self.parse_chemical_data(source_resp.text)
                    return {
                        'reaction_page_link': detail_url,
                        # REMOVED: data_source_link
                        # CHANGED: reaction_output_string -> reaction_smiles
                        'reaction_smiles': parsed['reaction_smiles'],
                        'molecules': parsed['molecules']
                    }
        except Exception as e:
            pass
        return None

    def initiate_extraction(self):
        """Main execution flow."""
        try:
            print("="*60 + "\nDEVELOPED BY: Zeus Aeneas Laporre\n" + "="*60)
            target_papers = self.collect_archive_urls()
            
            if not target_papers:
                print("No papers found.")
                return

            # Dictionary to hold results based on Link
            formatted_results = {}

            for i, paper in enumerate(target_papers, 1):
                reactions = self.mine_paper_data(paper, i, len(target_papers))
                # KEY: The paper URL
                # VALUE: The list of reactions
                formatted_results[paper['url']] = reactions

            # Save JSON
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(formatted_results, f, indent=2, ensure_ascii=False)
            
            print(f"\n[SUCCESS] Saved data for {len(formatted_results)} papers to {self.output_file}")

        finally:
            self.driver.quit()
            print("Driver closed.")

if __name__ == "__main__":
    scraper = KMTScraper()
    scraper.initiate_extraction()