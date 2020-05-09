import sys
import time
import xlsxwriter
from configparser import ConfigParser

from profile_scraper import ProfileScraper
from utils import boolean_to_string_xls, date_to_string_xls, message_to_user, chunks

# Loading of configurations
config = ConfigParser()
config.read('config.ini')

headless_option = len(sys.argv) >= 2 and sys.argv[1] == 'HEADLESS'

entries = []
for entry in open(config.get('profiles_data', 'input_file_name'), "r"):
    entries.append(entry.strip())

if len(entries) == 0:
    print("Please provide an input.")
    sys.exit(0)

if headless_option:
    grouped_entries = chunks(entries, len(entries) // int(config.get('system', 'max_threads')))
else:
    grouped_entries = [entries]

if len(grouped_entries) > 1:
    print(f"Starting {len(grouped_entries)} parallel scrapers.")
else:
    print("Starting scraping...")

scrapers = []
for entries_group in grouped_entries:
    scrapers.append(ProfileScraper(len(scrapers)+1, entries_group, config, headless_option))

for scraper in scrapers:
    scraper.start()

for scraper in scrapers:
    scraper.join()

scraping_results = []
for scraper in scrapers:
    scraping_results.extend(scraper.results)

# Generation of XLS file with profiles data
output_file_name = config.get('profiles_data', 'output_file_name')
if config.get('profiles_data', 'append_timestamp') == 'Y':
    output_file_name_splitted = output_file_name.split('.')
    output_file_name = "".join(output_file_name_splitted[0:-1]) + "_" + str(int(time.time())) + "." + \
                       output_file_name_splitted[-1]

workbook = xlsxwriter.Workbook(output_file_name)
worksheet = workbook.add_worksheet()

headers = ['Name', 'Email', 'Skills', 'Company', 'Industry', 'Job Title', 'Location',
           'DATE FIRST JOB', 'DATE LAST JOB', 'DATE FIRST GRADUATION', 'education', 'institution']

# Set the headers of xls file
for h in range(len(headers)):
    worksheet.write(0, h, headers[h])

for i in range(len(scraping_results)):

    scraping_result = scraping_results[i]

    if scraping_result.is_error():
        data = ['Error_' + scraping_result.message] * len(headers)
    else:
        p = scraping_result.profile
        data = [
            p.profile_name,
            p.email,
            ",".join(p.skills),
            p.current_job.company.name,
            p.current_job.company.industry,
            p.current_job.position,
            p.current_job.location.full_string,
            p.job_list[-1].daterange,
            p.job_list[0].daterange,
            p.edu_list[-1].end_year,
            "{}, {}".format(p.edu_list[-1].degreename, p.edu_list[-1].field),
            p.edu_list[-1].institution
        ]

    for j in range(len(data)):
        worksheet.write(i + 1, j, data[j])

workbook.close()

if any(scraper.interrupted for scraper in scrapers):
    message_to_user("The scraping didnt end correctly due to Human Check. The excel file was generated but it will "
                    "contain some entries reporting an error string.", config)
else:
    message_to_user('Scraping successfully ended.', config)
