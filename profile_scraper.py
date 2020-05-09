import traceback
from threading import Thread
import sys, traceback

from pyvirtualdisplay import Display

from job_history_summary import JobHistorySummary
from utils import Profile, Location, Job, Education, Company, CannotProceedScrapingException
from datetime import datetime
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from utils import linkedin_login, is_url_valid, HumanCheckException, message_to_user, get_browser_options, linkedin_logout


class ScrapingResult:
    def __init__(self, arg):
        if isinstance(arg, Profile):
            self.profile = arg
            self.message = None
        else:
            self.profile = None
            self.message = arg

    def is_error(self):
        return self.profile is None


class ProfileScraper(Thread):

    def __init__(self, identifier, entries, config, headless_option):

        Thread.__init__(self)

        self._id = identifier

        print(f"Scraper #{self._id}: Setting up the browser environment...")

        self.entries = entries

        self.results = []

        # Linux-specific code needed to open a new window of Chrome
        if config.get('system', 'os') == 'linux':
            self.display = Display(visible=0, size=(800, 800))
            self.display.start()

        # Creation of a new instance of Chrome
        self.browser = webdriver.Chrome(executable_path=config.get('system', 'driver'),
                                        options=get_browser_options(headless_option, config))

        self.industries_dict = {}
        self.companies_dict = {}
        self.locations_dict = {}

        self.config = config

        self.headless_option = headless_option

        self.interrupted = False

    def parse_entry(self, entry, delimiter: str):
        # This function supports data as:
        #
        #   https://www.linkedin.com/in/federicohaag ==> parse name, email, last job
        #
        #   https://www.linkedin.com/in/federicohaag:::01/01/1730 ==> parse name, email, last job
        #   and also produces a "job history summary" returning if the person was working while studying,
        #   and how fast she/he got a job after the graduation.
        #   As graduation date is used the one passed as parameter, NOT the date it could be on LinkedIn

        if delimiter in entry:
            profile_data = entry.split(delimiter)
            profile_linkedin_url = profile_data[0]
            profile_known_graduation_date = datetime.strptime(profile_data[1].strip(), '%d/%m/%y')
        else:
            profile_linkedin_url = entry
            profile_known_graduation_date = None

        if not profile_linkedin_url[-1] == '/':
            profile_linkedin_url += '/'

        return profile_linkedin_url, profile_known_graduation_date

    def scrap_profile(self, profile_linkedin_url, profile_known_graduation_date):

        if not is_url_valid(profile_linkedin_url):
            return ScrapingResult('BadFormattedLink')

        # Scraping of the profile may fail due to human check forced by LinkedIn
        try:

            # Setting of the delay (seconds) between operations that need to be sure loading of page is ended
            loading_pause_time = 2
            loading_scroll_time = 1

            # Opening of the profile page
            self.browser.get(profile_linkedin_url)

            if not str(self.browser.current_url).strip() == profile_linkedin_url.strip():
                if self.browser.current_url == 'https://www.linkedin.com/in/unavailable/':
                    return ScrapingResult('ProfileUnavailable')
                else:
                    raise HumanCheckException

            # Scraping the Email Address from Contact Info (email)

            # > click on 'Contact info' link on the page
            self.browser.execute_script(
                "(function(){try{for(i in document.getElementsByTagName('a')){let el = document.getElementsByTagName('a')[i]; "
                "if(el.innerHTML.includes('Contact info')){el.click();}}}catch(e){}})()")
            time.sleep(loading_pause_time)

            # > gets email from the 'Contact info' popup
            try:
                email = self.browser.execute_script(
                    "return (function(){try{for (i in document.getElementsByClassName('pv-contact-info__contact-type')){ let "
                    "el = "
                    "document.getElementsByClassName('pv-contact-info__contact-type')[i]; if(el.className.includes("
                    "'ci-email')){ "
                    "return el.children[2].children[0].innerText; } }} catch(e){return '';}})()")

                self.browser.execute_script("document.getElementsByClassName('artdeco-modal__dismiss')[0].click()")
            except:
                email = 'N/A'

            # Loading the entire page (LinkedIn loads content asynchronously based on your scrolling)
            window_height = self.browser.execute_script("return window.innerHeight")
            scrolls = 1
            while scrolls * window_height < self.browser.execute_script("return document.body.offsetHeight"):
                self.browser.execute_script(f"window.scrollTo(0, {window_height * scrolls});")
                time.sleep(loading_scroll_time)
                scrolls += 1

            try:
                self.browser.execute_script(
                    "document.getElementsByClassName('pv-profile-section__see-more-inline')[0].click()")
                time.sleep(loading_pause_time)
            except:
                pass

            # Get all the job positions
            try:
                job_positions = self.browser\
                    .find_element_by_id('experience-section')\
                    .find_elements_by_tag_name('li')
            except NoSuchElementException:
                print("job_positions is null")
                job_positions = []

            # Get all the education positions
            try:
                education_positions = self.browser\
                    .find_element_by_id('education-section')\
                    .find_elements_by_tag_name('li')
            except NoSuchElementException:
                print("job_positions is null")
                education_positions = []

            # Parsing of the page html structure
            soup = BeautifulSoup(self.browser.page_source, 'lxml')

            # Scraping the Name (using soup)
            try:
                name_div = soup.find('div', {'class': 'flex-1 mr5'})
                name_loc = name_div.find_all('ul')
                profile_name = name_loc[0].find('li').get_text().strip()
            except:
                return ScrapingResult('ERROR IN SCRAPING NAME')

            # Parsing skills
            try:
                self.browser.execute_script(
                    "document.getElementsByClassName('pv-skills-section__additional-skills')[0].click()")
                time.sleep(loading_pause_time)
            except:
                pass

            try:
                skills = self.browser.execute_script(
                    "return (function(){els = document.getElementsByClassName('pv-skill-category-entity');results = [];for (var i=0; i < els.length; i++){results.push(els[i].getElementsByClassName('pv-skill-category-entity__name-text')[0].innerText);}return results;})()")
            except:
                skills = []

            # Parsing the job positions

            if len(job_positions) > 0:
                # Parse job positions to extract relative the data ranges
                js = self.parsing_jobs(job_positions)
                job_positions_data_ranges = js['job_positions_data_ranges']
                Jobs_array = js['Jobs_array']
                last_job = Jobs_array[0]

                if len(education_positions) > 0:
                    eds = self.parsing_educations(education_positions)

                    return ScrapingResult(
                        Profile(
                            profile_name,
                            email,
                            skills,
                            last_job,
                            JobHistorySummary(
                                profile_known_graduation_date,
                                job_positions_data_ranges
                            ),
                            Jobs_array,
                            eds
                        )
                    )

                else:
                    return ScrapingResult(
                        Profile(
                            profile_name,
                            email,
                            skills,
                            last_job,
                            JobHistorySummary(
                                profile_known_graduation_date,
                                job_positions_data_ranges
                            ),
                            Jobs_array
                        )
                    )

            else:
                return ScrapingResult(
                    Profile(profile_name, email, skills)
                )

        except HumanCheckException:

            if self.headless_option:
                raise CannotProceedScrapingException

            linkedin_logout(self.browser)

            linkedin_login(self.browser, self.config.get('linkedin', 'username'),
                           self.config.get('linkedin', 'password'))

            while self.browser.current_url != 'https://www.linkedin.com/feed/':
                message_to_user('Please execute manual check', self.config)
                time.sleep(30)

            return self.scrap_profile(profile_linkedin_url, profile_known_graduation_date)

    def parsing_educations(self, education_positions):
        education_array = []

        for education_position in education_positions:
            try:
                # get the institution
                try:
                    institution_element = education_position.find_element_by_tag_name('h3')
                    institution = institution_element.text
                except NoSuchElementException:
                    institution = "N/A"

                try:
                    degreename_range_element = education_position\
                        .find_element_by_class_name('pv-entity__degree-info')
                    degreename_range_spans = degreename_range_element\
                        .find_elements_by_tag_name('span')
                    is_degreename = False
                    is_field = False
                    degreename = "N/A"
                    field = "N/A"
                    for span in degreename_range_spans:
                        if not is_degreename:
                            if span.text == "Degree Name":
                                is_degreename = True
                                pass
                        else:
                            degreename = span.text
                            is_degreename = False
                        if not is_field:
                            if span.text == "Field Of Study":
                                is_field = True
                        else:
                            field = span.text
                            is_field = False
                except NoSuchElementException:
                    degreename = "N/A"
                    field = "N/A"

                try:
                    start_year = "N/A"
                    end_year = "N/A"
                    dates_range_element = education_position\
                        .find_element_by_class_name("pv-entity__dates")
                    dates_spans = dates_range_element\
                        .find_elements_by_tag_name("span")
                    years_range = dates_spans[1]\
                        .find_elements_by_tag_name("time")
                    start_year = years_range[0].text
                    end_year = years_range[1].text
                except NoSuchElementException as ens:
                    print("Oops!", ens, "occured.")

                # class Education
                educacion_oo = Education(institution,
                                         degreename,
                                         field,
                                         start_year,
                                         end_year)

                # print(educacion_oo)

                education_array.append(educacion_oo)

            except:
                print("Oops!, \n{}\n{}\n{}\n."
                      .format(sys.exc_info()[0],
                              sys.exc_info()[1],
                              traceback.print_tb(sys.exc_info()[2],
                                                 limit=1,
                                                 file=sys.stdout)
                              )
                      )
                print("Edu untacking error")
                pass

        return education_array


    def parsing_jobs(self, job_positions):
        job_positions_data_ranges = []
        #array of Jobs
        Jobs_array = []

        for job_position in job_positions:
            #print('job_pos.text: {0}\n--'.format(job_position.text))
            try:
                # Get the date range of the job position
                # get the date_range
                try:                       
                    date_range_element = job_position.find_element_by_class_name('pv-entity__date-range')
                    date_range_spans = date_range_element.find_elements_by_tag_name('span')
                    date_range = date_range_spans[1].text
                    # print('date_range: {0}'.format(date_range))
                except NoSuchElementException:
                    date_range = "N/A"

                try:
                    # get the title
                    title_range_element = job_position.find_element_by_tag_name('h3')
                    title = title_range_element.text
                    # print('title: {0}'.format(title))
                except NoSuchElementException:
                    title = "N/A"

                try:
                    # get the companyname
                    companyname_range_element = job_position.find_element_by_class_name('pv-entity__secondary-title')
                    companyname = companyname_range_element
                    companyname = companyname.text.replace('Full-time', '').replace('Part-time', '').strip()
                    # print('companyname: {0}'.format(companyname))
                except NoSuchElementException:
                    companyname = "N/A"

                try:
                    # get the company info using bautifulsoup
                    company_url_link = job_position.find_element_by_tag_name('a').get_attribute('href')
                except NoSuchElementException:
                    company_url_link = "N/A"

                try: 
                    companylocation_range_element = job_position.find_element_by_class_name('pv-entity__location')
                    companylocation_spans = companylocation_range_element.find_elements_by_tag_name('span')
                    companylocation = companylocation_spans[1].text
                except NoSuchElementException:    
                    companylocation = "N/A"
                # print('companylocation: {0}'.format(companylocation))

                job_positions_data_ranges.append(date_range)
                info_company = self.get_company_data(company_url_link)
                try:                   
                    if info_company['companyname'] == "N/A":
                        info_company['companyname'] = companyname
                    if info_company['location'].full_string == "N/A":
                        loc = Location()
                        loc.parse_string(companylocation)
                        info_company['location'] = loc
                except: 
                    print("Oops!", sys.exc_info()[0], "occured.")
                    print(info_company['industry'])
                    print(info_company['companyname'])
                    print(info_company['location'])

                trabajo_oo = Job(
                    position=title.strip(),
                    company=Company(
                        name=info_company['companyname'].strip(),
                        industry=info_company['industry'].strip()
                    ),
                    location=info_company['location'],
                    daterange=date_range.strip()
                )
                Jobs_array.append(trabajo_oo)
                # print(trabajo_oo)

            except:
                print("Oops!, \n{}\n{}\n{}\noccured.".format(sys.exc_info()[0],
                                                                      sys.exc_info()[1],
                                                                      sys.exc_info()[2]))
                print("Job untacking error")
                pass

        return {'Jobs_array':Jobs_array,
                "job_positions_data_ranges":job_positions_data_ranges}


    def get_company_data(self, url):
        #print(url)
        no_industry = False
        if url.split("/")[3] != "company":
            print("no company page")
            return {'industry':'N/A',
                    'companyname':'N/A',
                    'location':Location('N/A','N/A','N/A')}

        if url not in self.industries_dict:
            try:
                self.browser.execute_script("window.open('');")
                self.browser.switch_to.window(self.browser.window_handles[1])
                self.browser.get(url)
            except:
                print("error opening company page")
                return {'industry':'N/A',
                        'companyname':'N/A',
                        'location':Location('N/A','N/A','N/A')}
            try:
                card_summary_divs = self.browser\
                    .find_element_by_class_name('org-top-card-summary-info-list')\
                    .find_elements_by_class_name('org-top-card-summary-info-list__info-item')
                inline_divs = self.browser\
                    .find_element_by_class_name('org-top-card-summary-info-list')\
                    .find_element_by_class_name('inline-block')\
                    .find_elements_by_class_name('org-top-card-summary-info-list__info-item')
                if len(card_summary_divs) == len(inline_divs):
                    no_industry = True
                #print("card_summary_divs {}, inline_divs {}".format(len(card_summary_divs),
                #                                                    len(inline_divs)))
            except:
                print("error getting company data 3")
            #industry
            try:
                if no_industry:
                    self.industries_dict[url] = "N/A"
                else:
                    self.industries_dict[url] = self.browser.execute_script(
                        "return document.getElementsByClassName("
                        "'org-top-card-summary-info-list__info-item')["
                        "0].innerText")
            except:
                #print("industry wasnt scrapped")
                self.industries_dict[url] = 'N/A'
            #companyname
            try:
                self.companies_dict[url] = self.browser.execute_script(
                    "return document.getElementsByClassName("
                    "'org-top-card-summary__title')["
                    "0].title")
            except:
                print("company name wasnt scrapped")
                self.companies_dict[url] = 'N/A'
            #locations
            try:
                if no_industry:
                    self.locations_dict[url] = self.browser.execute_script(
                        "return document.getElementsByClassName("
                        "'org-top-card-summary-info-list__info-item')["
                        "0].innerText")
                else:
                    self.locations_dict[url] = self.browser.execute_script(
                        "return document.getElementsByClassName("
                        "'org-top-card-summary-info-list__info-item')["
                        "1].innerText")
            except:
                print("location name wasnt scrapped")
                self.locations_dict[url] = 'N/A'

            try:
                self.browser.close()
                self.browser.switch_to.window(self.browser.window_handles[0])
            except:
                print("tab did not close")



        industry = self.industries_dict[url]
        companyname = self.companies_dict[url]
        location = Location()
        location.parse_string(self.locations_dict[url])

        return {'industry':industry,
                'companyname':companyname,
                'location':location}

    def run(self):

        delimiter = self.config.get('profiles_data', 'delimiter')

        print(f"Scraper #{self._id}: Executing LinkedIn login...")

        # Doing login on LinkedIn
        linkedin_login(self.browser, self.config.get('linkedin', 'username'), self.config.get('linkedin', 'password'))

        start_time = time.time()

        count = 0

        for entry in self.entries:

            count += 1

            # Print statistics about ending time of the script
            if count > 1:
                time_left = ((time.time() - start_time) / count) * (len(self.entries) - count + 1)
                ending_in = time.strftime("%H:%M:%S", time.gmtime(time_left))
            else:
                ending_in = "Unknown time"

            print(f"Scraper #{self._id}: Scraping profile {count} / {len(self.entries)} - {ending_in} left")

            try:
                linkedin_url, known_graduation_date = self.parse_entry(entry, delimiter)
                scraping_result = self.scrap_profile(linkedin_url, known_graduation_date)
                self.results.append(scraping_result)

            except CannotProceedScrapingException:
                self.results.append(ScrapingResult('TerminatedDueToHumanCheckError'))
                self.interrupted = True
                break

            except:
                with open("errlog.txt", "a") as errlog:
                    traceback.print_exc(file=errlog)
                self.results.append(ScrapingResult('GenericError'))

        # Closing the Chrome instance
        self.browser.quit()

        end_time = time.time()
        elapsed_time = time.strftime('%H:%M:%S', time.gmtime(end_time - start_time))

        print(f"Scraper #{self._id}: Parsed {count} / {len(self.entries)} profiles in {elapsed_time}")
