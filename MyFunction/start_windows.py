from selenium import webdriver
from datetime import datetime, timedelta
import seleniumrequests,json,time
from webdriver_manager.chrome import ChromeDriverManager

def type_login(driver):
    driver.find_element_by_css_selector('#pLoginName').send_keys('********') ## <- 帳號：已打碼
    time.sleep(2)
    driver.find_element_by_css_selector('#pLoginPassword').send_keys('************') ## <- 密碼：已打碼
    time.sleep(2)
    driver.find_element_by_css_selector('.k-switch-handle').click()
    time.sleep(2)
    driver.find_element_by_css_selector('#LoginSubmitBtn').click()
    time.sleep(2)
    assert  'Account'  not in driver.current_url

def start_new_windows():
    #參數設定區
    options = webdriver.ChromeOptions()
    prefs = {'profile.default_content_settings.popups': 0}
    options.add_experimental_option('prefs', prefs)
    options.add_experimental_option("excludeSwitches", ["enable-automation"]) #停用被自動控制
    
    options.add_argument('--headless') ## 如果要不開視窗加這行
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    driver = seleniumrequests.Chrome(ChromeDriverManager().install(),options=options)
    driver.implicitly_wait(5)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", { ## 會使selenium的特徵被隱藏起來，看起來更像一般CHROME
  "source": """
    Object.defineProperty(navigator, 'webdriver', {
      get: () => undefined
    })
  """
    })
    
    driver.get('https://eclass.yuntech.edu.tw/yuntech/sso-login')
    time.sleep(5)
    try:
        type_login(driver) ## 目前看起來似乎沒有檔機器人的驗證碼，可以用打字登入
    except Exception:
        raise BaseException("瀏覽器開啟失敗")
    
    time.sleep(1)
    driver.get('https://eclass.yuntech.edu.tw/yuntech/sso-login')
    time.sleep(1)
    return(driver)

