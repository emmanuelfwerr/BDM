import pandas as pd
from datetime import date
import os
from bs4 import BeautifulSoup
import requests
import sys


def get_filepaths(directory):
    '''
    This function will generate the file names in a directory 
    tree by walking the tree either top-down or bottom-up. For each 
    directory in the tree rooted at directory top (including top itself), 
    it yields a 3-tuple (dirpath, dirnames, filenames).
    '''
    file_names = []  # List which will store all of the full filepaths.

    # Walk the tree.
    for root, directories, files in os.walk(directory):
        for filename in files:
            file_names.append(filename)  # Add it to the list.

    return file_names  # Output


def check_update_years(files, topic):
    '''
    
    '''
    current_year = date.today().year # get current year
    if topic == 'lloguer_preu':
        update_years = [str(year) for year in range(2014, current_year)] # opendatabcn-lloguer_preu available years
    elif topic == 'accidents':
        update_years = [str(year) for year in range(2016, current_year)] # opendatabcn-accidents available years
    
    # Check years present in topic's current landing zone
    present_years = [element[:4] for element in files] # returns a list of present years
    
    #
    for element in present_years:
        if element in update_years:
            update_years.remove(element)
    
    return update_years


def collect_data(update_years, topic):
    '''
    
    '''
    # Input page link
    if topic == 'lloguer_preu':
        link = 'https://opendata-ajuntament.barcelona.cat/data/es/dataset/est-mercat-immobiliari-lloguer-mitja-mensual'
    elif topic == 'accidents':
        link = 'https://opendata-ajuntament.barcelona.cat/data/es/dataset/accidents-gu-bcn'

    # Request HTML for the link
    html_text = requests.get(link).text
    soup = BeautifulSoup(html_text, features="html.parser")
    # Parse link HTML
    yearly_reports = soup.find('ul', {'class': 'resource-list'})
    report_links = yearly_reports.findAll('a', {'class':'heading'}, href=True)

    # extracting report urls from dataset page
    yearly_reports = {}
    for i in report_links:
        yearly_reports[i.text.strip()[:-3].lower()] = 'https://opendata-ajuntament.barcelona.cat{}'.format(i['href'].strip())

    # removing 'xml' datasets and previously-extracted yearly reports
    yearly_reports = {key[:4]:val for key, val in yearly_reports.items() if key[-3:] != 'xml' and key[:4] in update_years}

    # extract pending yearly reports
    for key, value in yearly_reports.items():
        # Request HTML for the link
        html_text = requests.get(value).text
        soup = BeautifulSoup(html_text, features="html.parser")
        # Parse link HTML
        download_section = soup.find('div', {'class': 'actions resource-actions'})
        reports = download_section.findAll('a', {'class':'resource-type-None'}, href=True)
        # progress-tracker(UI)
        print('\nExtracting report for {}...'.format(key))

        for i in reports:
            try: # more recent reports dont need to specify encoding
                df = pd.read_csv(str(i['href']))
            except: # I know this is ugly but it is necessary
                try: # older reports require specified enconding
                    df = pd.read_csv(str(i['href']), encoding='unicode_escape')
                    print('*** This report is older and requires specified encoding *** accomodating...')
                except: # some reports use ';' as delimiter
                    df = pd.read_csv(str(i['href']), encoding='unicode_escape', sep=';')
                    print('*** This report uses semicolon (;) as delimiter *** accomodating...')
            # export to .csv in corresponding path
            df.to_csv('landing/temporal/opendatabcn-{}/{}_{}_bcn.csv'.format(topic, key, topic), encoding='utf-8', index=False)
            # progress-tracker(UI)
            print('{}_{}_bcn.csv exported to temporal landing zone'.format(key, topic))


if __name__ == '__main__':
    # check valid sys.argv[1] input from user
    
    
    # check if temporal landing is up to date (what years are missing?)
    opendatabcn_filenames = get_filepaths("landing/temporal/opendatabcn-{}".format(sys.argv[1]))
    update_years = check_update_years(opendatabcn_filenames, sys.argv[1])
    
    # opendatabcn data collection
    if update_years:
        print('\n{} data from opendatabcn is not up to date!'.format(sys.argv[1]))
        # collection function call...
        collect_data(update_years, sys.argv[1])
        print('\nUpdate Complete!\n')
    else:
        print('\n{} data from opendatabcn is already up to date!\n'.format(sys.argv[1]))