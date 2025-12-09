"""
Open Reaction Database (ORD) Comprehensive Scraper
Automated extraction and transformation of chemical reaction data
Complete system with browser setup, data extraction, and processing
"""

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION & MAPPINGS
# ═══════════════════════════════════════════════════════════════════════════

TIMEOUT_GLOBAL = 45
MAX_RETRIES = 3
WAIT_SHORT = 10
WAIT_LONG = 15
WAIT_JSON = 8
DELAY_ACTION = 2
DELAY_RETRY = 3

COMPONENT_ROLES = {
    0: "UNSPECIFIED", 1: "REACTANT", 2: "REAGENT", 3: "SOLVENT",
    4: "CATALYST", 5: "WORKUP", 6: "INTERNAL_STANDARD",
    7: "AUTHENTIC_STANDARD", 8: "PRODUCT", 9: "BYPRODUCT", 10: "SIDE_PRODUCT"
}

IDENTIFIER_TYPES = {
    0: "UNSPECIFIED", 1: "CUSTOM", 2: "SMILES", 3: "INCHI",
    4: "MOLBLOCK", 5: "FINGERPRINT", 6: "NAME",
    7: "IUPAC_NAME", 8: "CAS_NUMBER"
}

MEASUREMENT_UNITS = {
    'mass': {0: "UNSPECIFIED", 1: "KILOGRAM", 2: "GRAM", 3: "MILLIGRAM", 4: "MICROGRAM"},
    'volume': {0: "UNSPECIFIED", 1: "LITER", 2: "MILLILITER", 3: "MICROLITER", 4: "NANOLITER"},
    'moles': {0: "UNSPECIFIED", 1: "MOLE", 2: "MILLIMOLE", 3: "MICROMOLE", 4: "NANOMOLE"}
}


# ═══════════════════════════════════════════════════════════════════════════
# BROWSER DRIVER SETUP
# ═══════════════════════════════════════════════════════════════════════════

def get_driver():
    """Create optimized Chrome driver with stealth configuration"""
    options = Options()
    
    # Stealth settings
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Disable caching
    options.add_argument("--disable-application-cache")
    options.add_argument("--disk-cache-size=0")
    
    # Performance settings
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    
    # Suppress logging
    options.add_argument("--log-level=3")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    
    # Download settings
    options.add_experimental_option("prefs", {
        "download.default_directory": "./downloads",
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    
    driver.set_page_load_timeout(30)
    driver.implicitly_wait(10)
    
    return driver


def wait_page_ready(driver, timeout=TIMEOUT_GLOBAL):
    """Wait for complete page load"""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time.sleep(1)
    except TimeoutException:
        print("  Warning: Page load timeout")


# ═══════════════════════════════════════════════════════════════════════════
# DATA EXTRACTION HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def parse_identifiers(component):
    """Extract all identifiers from component"""
    ids = []
    for ident in component.get("identifiersList", []):
        ids.append({
            "type": IDENTIFIER_TYPES.get(ident.get("type", 0), "UNKNOWN"),
            "value": ident.get("value")
        })
    return ids


def parse_amount(amount_obj):
    """Extract amount with proper unit mapping"""
    if not amount_obj:
        return {}
    
    for qty_type in ['moles', 'volume', 'mass']:
        if qty_type in amount_obj:
            measurement = amount_obj[qty_type]
            return {
                qty_type: {
                    "value": measurement.get('value'),
                    "units": MEASUREMENT_UNITS[qty_type].get(measurement.get('units', 0), "UNKNOWN")
                }
            }
    return {}


def transform_reaction_data(raw_reaction):
    """Convert raw JSON to structured format"""
    if not raw_reaction or 'data' not in raw_reaction:
        return None
    
    data = raw_reaction['data']
    result = {
        'reaction_id': data.get('reactionId'),
        'success': raw_reaction.get('success', True),
        'inputsMap': [],
        'outcomes': []
    }
    
    # Process inputs
    for input_pair in data.get('inputsMap', []):
        category, input_data = input_pair[0], input_pair[1]
        components = []
        
        for comp in input_data.get("componentsList", []):
            components.append({
                "identifiers": parse_identifiers(comp),
                "amount": parse_amount(comp.get('amount')),
                "reaction_role": COMPONENT_ROLES.get(comp.get("reactionRole"), "UNKNOWN")
            })
        
        result['inputsMap'].append([category, {"components": components}])
    
    # Process outcomes
    for outcome in data.get('outcomesList', []):
        for product in outcome.get('productsList', []):
            measurements = []
            for meas in product.get('measurementsList', []):
                meas_data = {"type": meas.get("type"), "details": meas.get("details")}
                if 'amount' in meas and 'mass' in meas['amount']:
                    mass = meas['amount']['mass']
                    meas_data['mass'] = {
                        "value": mass.get('value'),
                        "units": MEASUREMENT_UNITS['mass'].get(mass.get('units', 0), "UNKNOWN")
                    }
                measurements.append(meas_data)
            
            result['outcomes'].append({
                "identifiers": parse_identifiers(product),
                "reaction_role": "PRODUCT",
                "is_desired_product": product.get('isDesiredProduct', False),
                "measurements": measurements
            })
    
    return result


def simplify_reaction_data(raw_reaction):
    """Convert raw JSON to simplified format (alternative transformation)"""
    if not raw_reaction or 'data' not in raw_reaction:
        return None
    
    data = raw_reaction['data']
    result = {
        'reaction_id': data.get('reactionId'),
        'inputs': [],
        'outcomes': []
    }
    
    # Process inputs - extract SMILES only
    for input_pair in data.get("inputsMap", []):
        category = input_pair[0]
        components = []
        
        for comp in input_pair[1].get("componentsList", []):
            # Extract SMILES (type 2)
            smiles = next(
                (id['value'] for id in comp.get("identifiersList", []) if id.get("type") == 2),
                None
            )
            
            # Extract amount
            amount_info = {}
            if 'amount' in comp:
                amt = comp['amount']
                if 'moles' in amt:
                    amount_info = {'type': 'moles', 'value': amt['moles']['value']}
                elif 'volume' in amt:
                    amount_info = {'type': 'volume', 'value': amt['volume']['value'], 'units': 'LITER'}
            
            components.append({
                "smiles": smiles,
                "role": COMPONENT_ROLES.get(comp.get("reactionRole"), "UNKNOWN"),
                "amount": amount_info
            })
        
        result['inputs'].append({"tab": category, "components": components})
    
    # Process outcomes
    for outcome in data.get('outcomesList', []):
        for product in outcome.get('productsList', []):
            smiles = next(
                (id['value'] for id in product.get("identifiersList", []) if id.get("type") == 2),
                None
            )
            result['outcomes'].append({
                "smiles": smiles,
                "is_desired": product.get('isDesiredProduct', False)
            })
    
    return result


# ═══════════════════════════════════════════════════════════════════════════
# DATASET DISCOVERY
# ═══════════════════════════════════════════════════════════════════════════

def get_all_dataset_ids(start_idx=None, end_idx=None):
    """Retrieve dataset IDs from browse page"""
    driver = get_driver()
    try:
        driver.get("https://open-reaction-database.org/browse")
        wait_page_ready(driver)
        wait = WebDriverWait(driver, TIMEOUT_GLOBAL)
        
        # Set pagination to 100
        try:
            print("Setting pagination to 100...")
            select_elem = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "select#pagination"))
            )
            Select(select_elem).select_by_value('100')
            time.sleep(5)
        except Exception as e:
            print(f"Pagination warning: {e}")
        
        # Calculate total pages
        total_pages = None
        try:
            pagination_info = wait.until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "div.pagination div.select"))
            )
            match = re.search(r'of (\d+) entries', pagination_info.text)
            if match:
                total_entries = int(match.group(1))
                if end_idx and end_idx > total_entries:
                    end_idx = total_entries
                total_pages = (total_entries + 99) // 100
                print(f"Total entries: {total_entries}")
        except Exception as e:
            print(f"Page calculation warning: {e}")
        
        # Collect dataset IDs
        dataset_ids = []
        page = 1
        should_stop = False
        
        while True:
            print(f"Scanning page {page}...")
            try:
                links = wait.until(
                    EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, "a[href*='/dataset/ord_dataset-']")
                    )
                )
                
                for link in links:
                    dataset_id = link.get_attribute('href').split('/')[-1]
                    if dataset_id not in dataset_ids:
                        dataset_ids.append(dataset_id)
                        if end_idx and len(dataset_ids) >= end_idx:
                            should_stop = True
                            break
            except Exception as e:
                print(f"Link extraction error: {e}")
                break
            
            if should_stop or (total_pages and page >= total_pages):
                break
            
            # Next page
            try:
                next_btn = driver.find_element(By.CSS_SELECTOR, "div.next.paginav")
                if "no-click" in next_btn.get_attribute("class"):
                    break
                driver.execute_script("arguments[0].click();", next_btn)
                time.sleep(5)
                page += 1
            except:
                break
        
        start = max(0, (start_idx - 1) if start_idx else 0)
        return dataset_ids[start:]
        
    finally:
        driver.quit()


# ═══════════════════════════════════════════════════════════════════════════
# REACTION EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════

def get_reaction_ids(driver, dataset_id, start_idx=None, end_idx=None):
    """Get all reaction IDs from dataset"""
    try:
        driver.get(f"https://open-reaction-database.org/dataset/{dataset_id}")
        wait_page_ready(driver)
        wait = WebDriverWait(driver, TIMEOUT_GLOBAL)
        
        # Optimize pagination
        try:
            target_size = '100'
            if end_idx:
                if end_idx <= 10: target_size = '10'
                elif end_idx <= 25: target_size = '25'
                elif end_idx <= 50: target_size = '50'
            
            if target_size != '10':
                select_elem = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "select#pagination"))
                )
                selector = Select(select_elem)
                if selector.first_selected_option.get_attribute("value") != target_size:
                    selector.select_by_value(target_size)
                    time.sleep(5)
        except Exception as e:
            print(f"  Pagination warning: {e}")
        
        # Wait for links
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/id/ord-')]")))
        except TimeoutException:
            return []
        
        # Collect IDs
        links = driver.find_elements(By.XPATH, "//a[contains(@href, '/id/ord-')]")
        reaction_ids = []
        for link in links:
            href = link.get_attribute('href')
            if href:
                rid = href.split('/')[-1]
                if rid.startswith('ord-') and rid not in reaction_ids:
                    reaction_ids.append(rid)
        
        # Apply range
        start = max(0, (start_idx - 1) if start_idx else 0)
        end = min(len(reaction_ids), end_idx if end_idx else len(reaction_ids))
        
        return reaction_ids[start:end]
        
    except Exception as e:
        print(f"Error getting reactions from {dataset_id}: {e}")
        return []


def scrape_reaction(driver, reaction_id, max_retries=MAX_RETRIES):
    """Extract JSON data from single reaction"""
    for attempt in range(max_retries):
        try:
            print(f"  Loading {reaction_id}...")
            driver.get(f"https://open-reaction-database.org/id/{reaction_id}")
            wait_page_ready(driver)
            wait = WebDriverWait(driver, TIMEOUT_GLOBAL)
            
            # Click full record button
            button = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'View Full Record')]"))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", button)
            time.sleep(1)
            driver.execute_script("arguments[0].click();", button)
            
            # Extract JSON
            print("    Extracting JSON...")
            json_elem = wait.until(
                EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'data')]//pre | //pre"))
            )
            
            json_text = json_elem.text
            if not json_text.strip().startswith('{'):
                time.sleep(2)
                json_text = json_elem.text
            
            if not json_text.strip().startswith('{'):
                raise Exception("Invalid JSON format")
            
            reaction_data = json.loads(json_text)
            
            # Close modal
            try:
                driver.find_element(By.CSS_SELECTOR, ".close").click()
            except:
                pass
            
            print(f"✓ Extracted: {reaction_id}")
            return {'reaction_id': reaction_id, 'data': reaction_data, 'success': True}
            
        except Exception as e:
            print(f"⚠ Attempt {attempt + 1} failed: {str(e)[:100]}")
            time.sleep(DELAY_RETRY)
    
    return {'reaction_id': reaction_id, 'data': None, 'success': False, 'error': 'Max retries exceeded'}


# ═══════════════════════════════════════════════════════════════════════════
# DATASET PROCESSING
# ═══════════════════════════════════════════════════════════════════════════

def process_dataset(dataset_id, start_idx=None, end_idx=None, use_simple_format=False):
    """Process complete dataset"""
    driver = get_driver()
    try:
        print(f"\n{'='*60}\nDataset: {dataset_id}\n{'='*60}")
        reaction_ids = get_reaction_ids(driver, dataset_id, start_idx, end_idx)
        
        if not reaction_ids:
            return {
                'dataset_id': dataset_id,
                'reactions': [],
                'total_reactions': 0,
                'successful_scrapes': 0
            }
        
        reactions = []
        for idx, rid in enumerate(reaction_ids, 1):
            print(f"  [{idx}/{len(reaction_ids)}] Processing {rid}...")
            result = scrape_reaction(driver, rid)
            
            if result['success']:
                try:
                    # Choose transformation format
                    if use_simple_format:
                        result['formatted_data'] = simplify_reaction_data(result)
                    else:
                        result['formatted_data'] = transform_reaction_data(result)
                    print(f"    ✓ Transformed")
                except Exception as e:
                    print(f"    ⚠ Transform error: {e}")
            
            reactions.append(result)
            time.sleep(1)
        
        success_count = sum(1 for r in reactions if r['success'])
        return {
            'dataset_id': dataset_id,
            'reactions': reactions,
            'total_reactions': len(reactions),
            'successful_scrapes': success_count
        }
        
    except Exception as e:
        print(f"✗ Dataset error: {e}")
        return {
            'dataset_id': dataset_id,
            'reactions': [],
            'total_reactions': 0,
            'successful_scrapes': 0,
            'error': str(e)
        }
    finally:
        driver.quit()


# ═══════════════════════════════════════════════════════════════════════════
# PARALLEL ORCHESTRATION
# ═══════════════════════════════════════════════════════════════════════════

def run_parallel_scraping(max_workers=3, dataset_ranges=None, specific_datasets=None,
                         dataset_start=None, dataset_end=None,
                         reaction_start=None, reaction_end=None,
                         use_simple_format=False):
    """Coordinate parallel extraction"""
    print("="*60 + "\nPARALLEL SCRAPING INITIATED\n" + "="*60)
    
    if specific_datasets:
        dataset_list = specific_datasets
    else:
        dataset_list = get_all_dataset_ids(dataset_start, dataset_end)
    
    if not dataset_list:
        print("✗ No datasets found!")
        return []
    
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {}
        for dataset_id in dataset_list:
            if dataset_ranges and dataset_id in dataset_ranges:
                start, end = dataset_ranges[dataset_id]
                future = executor.submit(process_dataset, dataset_id, start, end, use_simple_format)
            elif reaction_start is not None or reaction_end is not None:
                future = executor.submit(process_dataset, dataset_id, reaction_start, reaction_end, use_simple_format)
            else:
                future = executor.submit(process_dataset, dataset_id, None, None, use_simple_format)
            future_map[future] = dataset_id
        
        for idx, future in enumerate(as_completed(future_map), 1):
            dataset_id = future_map[future]
            try:
                result = future.result()
                results.append(result)
                print(f"✓ Completed {idx}/{len(dataset_list)}: {dataset_id}")
            except Exception as e:
                print(f"✗ Failed {dataset_id}: {e}")
                results.append({'dataset_id': dataset_id, 'error': str(e)})
    
    return results


# ═══════════════════════════════════════════════════════════════════════════
# USER INTERFACE
# ═══════════════════════════════════════════════════════════════════════════

def get_user_config():
    """Interactive configuration menu"""
    print("\n" + "="*60)
    print("ORD SCRAPER - CONFIGURATION")
    print("="*60)
    print("1. Scrape ALL datasets")
    print("2. Scrape SPECIFIC datasets by ID")
    print("3. Scrape UNIFORM range")
    print("4. Scrape CUSTOM ranges")
    print("5. Scrape SINGLE specific reaction")
    
    mode = input("\nSelect mode (1-5): ").strip()
    
    if mode == "1":
        d_start = input("Start dataset index (1-based, blank for 1): ").strip()
        d_end = input("End dataset index (1-based, blank for all): ").strip()
        return {
            'mode': 'all',
            'max_workers': 3,
            'dataset_start': int(d_start) if d_start else None,
            'dataset_end': int(d_end) if d_end else None
        }
    elif mode == "2":
        ids = input("Enter dataset IDs (comma-separated): ").strip().split(',')
        return {
            'mode': 'specific_datasets',
            'dataset_ids': [i.strip() for i in ids if i.strip()],
            'max_workers': 3
        }
    elif mode == "3":
        return {
            'mode': 'uniform_range',
            'dataset_start': int(input("Dataset start: ").strip() or "1"),
            'dataset_end': int(input("Dataset end: ").strip() or "1"),
            'reaction_start': int(input("Reaction start: ").strip() or "1"),
            'reaction_end': int(input("Reaction end: ").strip() or "10"),
            'max_workers': 3
        }
    elif mode == "4":
        ranges = {}
        while True:
            did = input("Dataset ID (blank to finish): ").strip()
            if not did: break
            s = input(f"  Start for {did}: ").strip()
            e = input(f"  End for {did}: ").strip()
            ranges[did] = (int(s) if s else None, int(e) if e else None)
        return {'mode': 'custom_ranges', 'dataset_ranges': ranges, 'max_workers': 3}
    elif mode == "5":
        d = input("Dataset index: ").strip()
        if not d: return get_user_config()
        r = input("Reaction index (default 1): ").strip() or "1"
        return {
            'mode': 'single_target',
            'dataset_target': int(d),
            'reaction_target': int(r),
            'max_workers': 1
        }
    else:
        return {'mode': 'all', 'max_workers': 3, 'dataset_start': None, 'dataset_end': None}


# ═══════════════════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════════════════

def main():
    """Main entry point"""
    print("\n" + "="*60)
    print("OPEN REACTION DATABASE SCRAPER")
    print("Zeus Aeneas Laporre")
    print("="*60)
    
    config = get_user_config()
    print(f"\nMode: {config['mode']}\n")
    
    # Ask for format preference
    format_choice = input("Use simple format? (y/n, default=n): ").strip().lower()
    use_simple = format_choice == 'y'
    
    # Execute based on configuration
    results = []
    if config['mode'] == 'all':
        results = run_parallel_scraping(
            max_workers=config['max_workers'],
            dataset_start=config.get('dataset_start'),
            dataset_end=config.get('dataset_end'),
            use_simple_format=use_simple
        )
    elif config['mode'] == 'specific_datasets':
        results = run_parallel_scraping(
            max_workers=config['max_workers'],
            specific_datasets=config['dataset_ids'],
            use_simple_format=use_simple
        )
    elif config['mode'] == 'uniform_range':
        results = run_parallel_scraping(
            max_workers=config['max_workers'],
            dataset_start=config.get('dataset_start'),
            dataset_end=config.get('dataset_end'),
            reaction_start=config.get('reaction_start'),
            reaction_end=config.get('reaction_end'),
            use_simple_format=use_simple
        )
    elif config['mode'] == 'custom_ranges':
        results = run_parallel_scraping(
            max_workers=config['max_workers'],
            dataset_ranges=config['dataset_ranges'],
            use_simple_format=use_simple
        )
    elif config['mode'] == 'single_target':
        results = run_parallel_scraping(
            max_workers=1,
            dataset_start=config['dataset_target'],
            dataset_end=config['dataset_target'],
            reaction_start=config['reaction_target'],
            reaction_end=config['reaction_target'],
            use_simple_format=use_simple
        )
    
    # Save formatted output
    formatted_output = {}
    for dataset in results:
        dataset_id = dataset.get('dataset_id')
        if dataset_id:
            formatted_output[dataset_id] = {
                'dataset_id': dataset_id,
                'total_reactions_scraped': dataset.get('total_reactions', 0),
                'reactions': [
                    r['formatted_data']
                    for r in dataset.get('reactions', [])
                    if r.get('success') and 'formatted_data' in r
                ]
            }
    
    output_file = 'ord_formatted_data.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(formatted_output, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ Results saved to {output_file}")


if __name__ == "__main__":
    main()