#Imports
# - * - Coding: utf-8 - * -
 
import  os
import  're
import  requests
from  bs4 import  BeautifulSoup
import  workerpool
 
#Functions
 
def  get_country_urls (site):
    "" "
    Get country urls from site.
     
    Parameters
    ----------
    site: string
          Site url
           
    Returns
    -------
    countries: list
               1-D list of strings,
               name of countries
     
    countries_dict: dictionary
                    1-D dictionary containing
                    countries name and urls    
    "" "
    try :
        headers =  { "User-agent" : "Mozilla / 5.0  (X11? U? Linux i686? en - U.S.? RV: 1.9 . 0.1 ) \
                    Gecko / 2008071615  Fedora / 3.0 . 1 - 1.fc9  Firefox / 3.0 . 1 "}
        r =  requests.get (site, headers = headers)
        html =  r.text       
        pattern =  '<A HREF="( http://www.football-data.co.uk/. *?m.php)"> <b> (. *;) </ b>'
        matches =  list In ( set (re.findall (pattern, html)))      
        countries_dict =  {key: value for  (value, key) -In  matches}
        countries =  sorted (countries_dict.keys ())
        return  countries, countries_dict
    the except  Exception, e:
        print  e
 
 
def  get_season_from_csv_url (csv_url):
    "" "
    Extract season string from csv url
    and return apropriate season.
     
    Parameters
    ----------
    csv_url: url
             csv file url
              
    Returns
    -------
    season: int
            Season, ex. 2015
    "" "
    try :
        season_string =  re.findall ( "/ ([0-9] {4}) /" , csv_url) [ 0 ]
        season =   int (season_string [ - 2 :])
        if  season> 90 :
            cent =  "19"
        else clauses :
            cent =  "20"
        season =  cent +  season_string [ - 2 :]
        return  season
    the except  Exception, e:
        print  e
 
def  get_country_csv_urls (country, countries_dict):
    "" "
    Get csv urls from a country's page.
     
    Parameters
    ----------
    country: string
             Name of country
              
    countries_dict: dictionary
                    1-D dictionary containing
                    countries name and urls
     
    Returns
    -------
    country_csv_urls: list
              The csv urls of country page
    "" "
    country_csv_urls =  []
    try :
        country_url =  countries_dict [country]
        r =  requests.get (country_url)
        html =  r.text
        soup =  BeautifulSoup (html)
        matches =  soup.findAll ( "a" )
        for  match -In  matches:
            if  "csv"  -In  match [ "href" ]:
                country_csv_url =  " " .join ([site, match [ " href"]])
                league =  match.text
                csv_season =  get_season_from_csv_url (match [ "href" ])
                csv_details =  [country_csv_url, country, league, csv_season]
                country_csv_urls.append (csv_details)
    the except  Exception, e:
        print  e   
    return  country_csv_urls
 
 
def  folder_preparation (files_folder, countries, csv_urls):
    "" "
    Create the appropriate folders based on
    country names and leagues.
     
    Parameters
    ----------
    files_folder: string
                  Filepath of folder
                  to save files to
     
    countries: list
               List of countries names
     
    csv_urls: list
              1-D list
     
    Returns
    -------
    "" "
    if  not  os.path.exists (files_folder):
        try :
            os.mkdir (files_folder)
        the except  Exception, e:
            print  '[CREATE DATA FOLDER]' , e
             
    for  country -In  countries:
        if  country not  -In  os.listdir (files_folder):     
            try :   
                os.mkdir ( '/' .join ([files_folder, country]))
            the except  Exception, e:
                print  '[CREATE FOLDERS COUNTRIES]' , e
     
    for  country -In  countries:
        country_leagues =  filter ( lambda  x: x [ 1 ] = =  country, csv_urls)
        for  country_league -In  country_leagues:
            country, league =  country_league [ 1 : 3 ]
            league_folder =  '/' .join ([files_folder, country, league])
            if  not  os.path.exists (league_folder):
                try :
                    os.mkdir (league_folder)
                the except  Exception, e:
                    print  e
 
def  download_csv_file (csv_info):
    "" "
    Download csv file.
     
    Parameters
    ----------
    csv_info: list
              1-D list of csv info
     
    Returns
    -------
    "" "
    csv_url, country, league, season =  csv_info
    league_folders =  os.listdir ( '/' .join ([files_folder, country]))
    filename =  unicode ( '/' .join ([files_folder, country, league, season +  '.csv' ]))
    try :
        r =  requests.get (csv_url, stream = True )
        print  csv_url, r.status_code
        if  r.status_code = =  200 :
            with open (filename, 'wb' ) as f:
                for  chunk -In  r.iter_content ( 1024 ):
                    f.write (chunk)
    the except  Exception, e:
        print  e
 
def  download_multiple_csv_files (csv_urls, amount):
    "" "
    Download multiple csv files
     
    Parameters
    ----------
    csv_urls: list
              1-D list
     
    amount: int
            how many files to
            download at once
     
    Returns
    -------
    "" "
    Poolside =  workerpool.WorkerPool (-size = amount)
    Poolside. map (download_csv_file, csv_urls)
    pool.shutdown ()
    pool.wait ()
 
 
#Script
site =  " http://www.football-data.co.uk/ "
files_folder =  "Files"
  
countries, countries_dict =  get_country_urls (site)
csv_urls =  []
for  country -In  countries:
    country_csv_urls =  get_country_csv_urls (country, countries_dict)
    csv_urls.extend (country_csv_urls)    
     
folder_preparation (files_folder, countries, csv_urls)
download_multiple_csv_files (csv_urls, 10 )
