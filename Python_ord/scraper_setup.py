"""
Browser Driver Configuration Module
Provides optimized Chrome and Firefox WebDriver initialization
with stealth mode and performance enhancements
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════

PAGE_LOAD_TIMEOUT = 30
IMPLICIT_WAIT_TIME = 10
DOWNLOAD_DIRECTORY = "./downloads"


# ═══════════════════════════════════════════════════════════════════════════
# CHROME DRIVER INITIALIZATION
# ═══════════════════════════════════════════════════════════════════════════

def initialize_chrome_browser():
    """
    Create optimized Chrome WebDriver with stealth and performance settings
    
    Returns:
        webdriver.Chrome: Configured Chrome browser instance
    """
    config = ChromeOptions()
    
    # Optional headless mode (uncomment to run without visible browser)
    # config.add_argument("--headless")
    
    # Anti-detection measures
    config.add_argument("--disable-blink-features=AutomationControlled")
    config.add_experimental_option("excludeSwitches", ["enable-automation"])
    config.add_experimental_option('useAutomationExtension', False)
    
    # Cache disabling for real-time data retrieval
    config.add_argument("--disable-application-cache")
    config.add_argument("--disk-cache-size=0")
    
    # System resource optimization
    config.add_argument("--disable-dev-shm-usage")
    config.add_argument("--no-sandbox")
    config.add_argument("--disable-gpu")
    
    # Logging suppression
    config.add_argument("--log-level=3")
    config.add_experimental_option("excludeSwitches", ["enable-logging"])
    
    # Download behavior configuration
    config.add_experimental_option("prefs", {
        "download.default_directory": DOWNLOAD_DIRECTORY,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    
    # Initialize browser with auto-managed driver
    browser = webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()),
        options=config
    )
    
    # Configure timeout behaviors
    browser.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    browser.implicitly_wait(IMPLICIT_WAIT_TIME)
    
    return browser


# ═══════════════════════════════════════════════════════════════════════════
# FIREFOX DRIVER INITIALIZATION
# ═══════════════════════════════════════════════════════════════════════════

def initialize_firefox_browser():
    """
    Create optimized Firefox WebDriver with stealth and performance settings
    
    Returns:
        webdriver.Firefox: Configured Firefox browser instance
    """
    config = FirefoxOptions()
    
    # Optional headless mode (uncomment to run without visible browser)
    # config.add_argument("--headless")
    
    # Anti-detection preferences
    config.set_preference("dom.webdriver.enabled", False)
    config.set_preference('useAutomationExtension', False)
    
    # Complete cache disabling
    config.set_preference("browser.cache.disk.enable", False)
    config.set_preference("browser.cache.memory.enable", False)
    config.set_preference("browser.cache.offline.enable", False)
    config.set_preference("network.http.use-cache", False)
    
    # Download handling configuration
    config.set_preference("browser.download.folderList", 2)
    config.set_preference("browser.download.dir", DOWNLOAD_DIRECTORY)
    config.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/octet-stream")
    
    # Console output suppression
    config.set_preference("devtools.console.stdout.content", False)
    
    # Initialize browser with auto-managed driver
    browser = webdriver.Firefox(
        service=FirefoxService(GeckoDriverManager().install()),
        options=config
    )
    
    # Configure timeout behaviors
    browser.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    browser.implicitly_wait(IMPLICIT_WAIT_TIME)
    
    return browser


# ═══════════════════════════════════════════════════════════════════════════
# UNIFIED DRIVER FACTORY
# ═══════════════════════════════════════════════════════════════════════════

def create_browser_session(browser_type='chrome'):
    """
    Factory function to create browser driver based on preference
    
    Args:
        browser_type (str): Browser to initialize ('chrome' or 'firefox')
    
    Returns:
        webdriver: Configured browser instance
    
    Raises:
        ValueError: If unsupported browser type specified
    """
    browser_type = browser_type.lower()
    
    if browser_type == 'chrome':
        return initialize_chrome_browser()
    elif browser_type == 'firefox':
        return initialize_firefox_browser()
    else:
        raise ValueError(f"Unsupported browser type: {browser_type}. Use 'chrome' or 'firefox'.")


# ═══════════════════════════════════════════════════════════════════════════
# LEGACY COMPATIBILITY ALIAS
# ═══════════════════════════════════════════════════════════════════════════

def get_driver():
    """
    Legacy function name for backward compatibility
    Defaults to Chrome browser
    
    Returns:
        webdriver.Chrome: Configured Chrome browser instance
    """
    return initialize_chrome_browser()


# ═══════════════════════════════════════════════════════════════════════════
# USAGE EXAMPLES
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Example 1: Using the new unified factory
    print("Initializing Chrome browser...")
    driver = create_browser_session('chrome')
    print(f"Chrome browser ready: {type(driver)}")
    driver.quit()
    
    # Example 2: Using Firefox
    print("\nInitializing Firefox browser...")
    driver = create_browser_session('firefox')
    print(f"Firefox browser ready: {type(driver)}")
    driver.quit()
    
    # Example 3: Using legacy function (Chrome)
    print("\nUsing legacy get_driver() function...")
    driver = get_driver()
    print(f"Browser ready: {type(driver)}")
    driver.quit()
    
    print("\n✓ All browser configurations tested successfully")