import re
import pyttsx3
from datetime import datetime
from selenium import webdriver

from job_history_summary import JobHistorySummary


class HumanCheckException(Exception):
    """Human Check from Linkedin"""
    pass


class CannotProceedScrapingException(Exception):
    """Human Check from Linkedin during an headless mode execution"""
    pass

class Location:
    def __init__(self, city='N/A', country='N/A', location='N/A'):
        self.full_string = location
        self.city = city
        self.country = country

    def parse_string(self, location):
        self.full_string = location
        if ',' in location:
            # TODO: Probably useless try - except. To be checked.
            try:
                self.city = location.split(',')[0]
                self.country = location.split(',')[-1]
            except:
                pass

    def __str__(self):
        if (self.city == 'N/A') and (self.country == 'N/A'):
            return '{}'.format(self.full_string)
        else:
            return '{}, {}'.format(self.city,self.country)

class Company:
    def __init__(self, name='N/A', industry='N/A'):
        self.name = name
        self.industry = industry

    def __str__(self):
        return '{}, {}'.format(self.name,self.industry)


class Job:
    def __init__(self, company=Company(), position='N/A',
                 location=Location(), daterange="N/A"):
        self.company = company
        self.position = position
        self.location = location
        self.daterange = daterange

    def __set__(self, instance, value):
        self.instance = value

    def __str__(self):
        return '''
                  company: {0}
                  pos: {1}
                  loc: {2}
                  daterange: {3}'''.format(self.company,
                                           self.position,
                                           self.location,
                                           self.daterange)


class Education:
    def __init__(self, institution="N/A", degreename='N/A',
                 field="N/A", start_year="N/A", end_year="N/A"):
        self.institution = institution
        self.degreename = degreename
        self.field = field
        self.start_year = start_year
        self.end_year = end_year

    def __set__(self, instance, value):
        self.instance = value

    def __str__(self):
        return '''
                institution: {0}
                degree: {1}, {2}
                dates: {3} - {4}
                '''.format(self.institution,
                           self.degreename,
                           self.field,
                           self.start_year,
                           self.end_year)


class Profile:
    def __init__(self, profile_name, email, skills, last_job=Job(), job_history_summary=JobHistorySummary(),
                 job_list = [], edu_list=[]):
        self.profile_name = profile_name
        self.email = email
        self.skills = skills
        self.current_job = last_job if not job_history_summary.is_currently_unemployed else Job()
        self.jobs_history = job_history_summary
        if len(job_list) == 0:
            self.job_list = [Job()]
        else:
            self.job_list = job_list

        if len(edu_list) == 0:
            self.edu_list = [Education()]
        else:
            self.edu_list = edu_list



def linkedin_logout(browser):
    browser.get('https://www.linkedin.com/m/logout')


def linkedin_login(browser, username, password):
    browser.get('https://www.linkedin.com/uas/login')

    username_input = browser.find_element_by_id('username')
    username_input.send_keys(username)

    password_input = browser.find_element_by_id('password')
    password_input.send_keys(password)
    try:
        password_input.submit()
    except:
        pass


def chunks(lst, n):
    if n == 0:
        return [lst]
    """Yield successive n-sized chunks from lst."""
    return [lst[i:i + n] for i in range(0, len(lst), n)]


def is_url_valid(url):
    regex = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(regex, url) is not None


def get_months_between_dates(date1, date2):

    if date1 < date2:
        diff = date2 - date1
    elif date1 > date2:
        diff = date1 - date2
    else:
        return 0

    return diff.days // 30


def boolean_to_string_xls(boolean_value):
    if boolean_value is None:
        return 'N/A'

    return 'X' if boolean_value else ''


def date_to_string_xls(date):
    if date is None:
        return 'N/A'

    return datetime.strftime(date, "%b-%y")


def message_to_user(message, config):
    print(message)

    if config.get('system', 'speak') == 'Y':
        engine = pyttsx3.init()
        engine.say(message)
        engine.runAndWait()


def get_browser_options(headless_option, config):

    options = webdriver.ChromeOptions()

    options.add_argument('--no-sandbox')

    if headless_option:
        options.add_argument('--headless')

    options.add_argument('--disable-dev-shm-usage')

    if not config.get('system', 'chrome_path') == '':
        options.binary_location = r"" + config.get('system', 'chrome_path')

    return options
