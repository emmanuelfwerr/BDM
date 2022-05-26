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


def check_update_years(files):
    '''
    
    '''
    current_year = date.today().year # get current year
    update_years = [str(year) for year in range(2010, current_year)] # opendatabcn-accidents available years
    
    # Check years present in topic's current landing zone
    present_years = [element[:4] for element in files] # returns a list of present years
    
    #
    for element in present_years:
        if element in update_years:
            update_years.remove(element)
    
    return update_years


def collect_data(update_years):
    '''
    
    '''
    # Input page link
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
                try: # some reports use ';' as delimiter
                    df = pd.read_csv(str(i['href']), encoding='unicode_escape')
                except:
                    print('*** This report uses semicolon (;) as delimiter *** accomodating...')
                    df = pd.read_csv(str(i['href']), encoding='unicode_escape', sep=';')
            df.to_csv('landing/temporal/opendatabcn-accidents/{}_accidents_bcn.csv'.format(key), encoding='utf-8', index=False)
            # progress-tracker(UI)
            print('{}_accidents_bcn.csv exported to temporal landing zone'.format(key))

            
if __name__ == '__main__':
    #
    opendatabcn_filenames = get_filepaths("landing/temporal/opendatabcn-accidents")
    update_years = check_update_years(opendatabcn_filenames)
    
    # opendatabcn data collection
    if update_years:
        print('\naccidents data from opendatabcn is not up to date!')
        collect_data(update_years)
        print('\nUpdate Complete!\n')
    else:
        print('\naccidents data from opendatabcn is already up to date!\n')