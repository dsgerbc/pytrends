from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from functools import partial
from time import sleep

from pytrends.request import TrendReq
from pytrends.exceptions import ResponseError
import pandas as pd
from calendar import monthrange
from random import randrange
from os.path import isfile


def getTimeframe(start: date, stop: date) -> str:
    """Given two dates, returns a string representing the interval between the
    dates. This is string is used to retrieve data for a specific time frame
    from Google Trends.
    """
    return f"{start.strftime('%Y-%m-%d')} {stop.strftime('%Y-%m-%d')}"


def _fetchData(pytrends, build_payload, timeframe: str) -> pd.DataFrame:
    """Attempts to fecth data and retries in case of a ResponseError."""
    attempts, fetched = 0, False
    while not fetched:
        try:
            build_payload(timeframe=timeframe)
        except ResponseError as err:
            print(err)
            print(f'Trying again in {60 + 5*attempts} seconds.')
            sleep(60 + 5*attempts)
            attempts += 1
        else:
            fetched = True
    return pytrends.interest_over_time()

def _getRawDailyData(word: str,
                 start_year: int = 2007,
                 stop_year: int = 2018,
                 geo: str = 'US',
                 tz: int = 240,
                 cat:int = 0,
                 verbose: bool = True,
                 hiprecision: bool = False,
                 wait_time: float = 5.0) -> pd.DataFrame:
    """needs description
    Currently, pulls daily data in chunks of 8-months, then to be scaled by monthly data"""

    # Set up start and stop dates
    start_date = date(start_year, 1, 1)
    stop_date = min([date(stop_year, 12, 31),date.today()])  
    #stop date truncated to today. Otherwise google may throw an error

    # Start pytrends for US region
    pytrends = TrendReq(hl='en-US', tz=tz)
    # Initialize build_payload with the word we need data for
    build_payload = partial(pytrends.build_payload,
                            kw_list=[word], cat=cat, geo=geo, gprop='')

    # Get daily data, month by month
    results = {}
    # if a timeout or too many requests error occur we need to adjust wait time
    current = start_date
    while current < stop_date:
        lastDateOfDailyChunk = date(current.year, current.month, monthrange(current.year, current.month)[1]) 
        #if using hi precision option, this would pull one month at a time 
        #(at loss of total number of max keywords per day)
        #with a 1600 request/day limit, lowprecision method requires 25 requests per keyword
        #for the whole 2004-2019 daily sample
        #The hi precision mode would require ~190 requests per keyword
        if not hiprecision:
            lastDateOfDailyChunk= lastDateOfDailyChunk+relativedelta(months=7)
            #this pulls at most 8 months of daily data (google limit is 270 days, not enough for 9 months)
        lastDateOfDailyChunk= min([stop_date,lastDateOfDailyChunk])
        #this truncates the 8-month sample by stop_date
        timeframe = getTimeframe(current, lastDateOfDailyChunk)
        if verbose:
            print(f'Category{cat}/{word}/{geo}:{timeframe}')
        results[current] = _fetchData(pytrends, build_payload, timeframe)
        current = lastDateOfDailyChunk + timedelta(days=1)
        sleep(randrange(0,10*wait_time)/10)  # don't go too fast or Google will send 429s

    daily = pd.concat(results.values()).drop(columns=['isPartial'])

    return daily


def getDailyDataOnce(word: str,
                 start_year: int = 2007,
                 stop_year: int = 2018,
                 geo: str = 'US',
                 tz: int = 240,
                 cat: int = 0,
                 verbose: bool = True,
                 wait_time: float = 5.0) -> pd.DataFrame:
    """Given a word, fetches daily search volume data from Google Trends and
    returns results in a pandas DataFrame.
    Details: Due to the way Google Trends scales and returns data, special
    care needs to be taken to make the daily data comparable over different
    months. To do that, we download daily data on a month by month basis,
    and also monthly data. The monthly data is downloaded in one go, so that
    the monthly values are comparable amongst themselves and can be used to
    scale the daily data. The daily data is scaled by multiplying the daily
    value by the monthly search volume divided by 100.
    For a more detailed explanation see http://bit.ly/trendsscaling
    Args:
        word (str): Word to fetch daily data for.
        start_year (int): First year to fetch data for. Starts at the beginning
            of this year (1st of January).
        stop_year (int): Last year to fetch data for (inclusive). Stops at the
            end of this year (31st of December).
        geo (str): Geographical area code. Default at 'US'
        tz (int): time zone, minutes offset off GMT. 240 for US EST
        verbose (bool): If True, then prints the word and current time frame
            we are fecthing the data for.
    Returns:
        complete (pd.DataFrame): Contains 4 columns.
            The column named after the word argument contains the daily search
            volume already scaled and comparable through time.
            The column f'{word}_unscaled' is the original daily data fetched
            month by month, and it is not comparable across different months
            (but is comparable within a month).
            The column f'{word}_monthly' contains the original monthly data
            fetched at once. The values in this column have been backfilled
            so that there are no NaN present.
            The column 'scale' contains the scale used to obtain the scaled
            daily data.
    """

    # Set up start and stop dates
    start_date = date(start_year, 1, 1)
    stop_date = min([date(stop_year, 12, 31),date.today()])  
    #stop date truncated to today. Otherwise google may throw an error

    # Start pytrends for US region
    pytrends = TrendReq(hl='en-US', tz=tz)
    # Initialize build_payload with the word we need data for
    build_payload = partial(pytrends.build_payload,
                            kw_list=[word], cat=cat, geo=geo, gprop='')

    # Obtain monthly data for all months in years [2004, stop_year]
    monthly = _fetchData(pytrends, build_payload,
                         getTimeframe(date(2004, 1, 1), stop_date))[start_date: stop_date]

    # Get daily data, month by month
    results = {}
    # if a timeout or too many requests error occur we need to adjust wait time
    current = start_date
    while current < stop_date:
        lastDateOfMonth = date(current.year, current.month, monthrange(current.year, current.month)[1])
        timeframe = getTimeframe(current, lastDateOfMonth)
        if verbose:
            print(f'{word}/{geo}:{timeframe}')
        results[current] = _fetchData(pytrends, build_payload, timeframe)
        current = lastDateOfMonth + timedelta(days=1)
        sleep(randrange(10,10*wait_time)/10)  # don't go too fast or Google will send 429s

    daily = pd.concat(results.values()).drop(columns=['isPartial'])
    dailyaverage=daily.resample('M').mean() #generate monthly averages
    dailyaverage.index=dailyaverage.index.values.astype('<M8[M]') #above produces weird dates, so cut them
    daily[f'{word}_{geo}_avg']=dailyaverage
    daily[f'{word}_{geo}_avg'].ffill(inplace=True)  #fill in forward
    complete = daily.join(monthly, lsuffix=f'_{geo}_unscaled', rsuffix=f'_{geo}_monthly')

    # Scale daily data by monthly weights so the data is comparable
    complete[f'{word}_{geo}_monthly'].ffill(inplace=True)  # fill NaN values
    complete['scale'] = complete[f'{word}_{geo}_monthly']/complete[f'{word}_{geo}_avg']
    complete[f'{word}_{geo}'] = complete[f'{word}_{geo}_unscaled']*complete.scale
    complete.drop(columns=['isPartial',f'{word}_{geo}_avg'], inplace=True)
    return complete


def buildDatabase(dbpath: str,
                  word: str,
                  geo: str = 'US',
                  cat: int = 0,
                  tz: int = 240,
                  hiprecision: bool = False): 
    
    if isfile(f'{dbpath}/{cat}_{word}_{geo}.xlsx'):
        data_m=pd.read_excel(f'{dbpath}/{cat}_{word}_{geo}.xlsx',sheet_name='monthly').set_index('date')
        data_d=pd.read_excel(f'{dbpath}/{cat}_{word}_{geo}.xlsx',sheet_name='daily').set_index('date')
    else:
        data_m=pd.DataFrame()
        data_d=pd.DataFrame()
    
    if word+date.today().strftime("_%m_%d_%Y") in set(data_m.columns):
        # if today's data has been donwloaded already, google will send the same data again
        print(f"Today's data already present in the database for search term '{word}/{geo}'") 
        return 
    else:
        start_date = date(2004, 1, 1)
        stop_date = date.today()   
        pytrends = TrendReq(hl='en-US', tz=tz)
        # Initialize build_payload with the word we need data for
        build_payload = partial(pytrends.build_payload,
                                kw_list=[word], cat=cat, geo=geo, gprop='')
    
        # Obtain monthly data for all months in years [2004, stop_year]
        monthly = _fetchData(pytrends, build_payload,
                             getTimeframe(date(2004, 1, 1), stop_date)).drop(columns=['isPartial'])   
        monthly.rename(columns={word:word+date.today().strftime("_%m_%d_%Y")},inplace=True)
        daily=_getRawDailyData(word, 2004, stop_date.year, geo, tz, cat, True,hiprecision, 1 )
        daily.rename(columns={word:word+date.today().strftime("_%m_%d_%Y")},inplace=True)
        data_m=data_m.join(monthly, how='outer')
        with pd.ExcelWriter(f'{dbpath}/{cat}_{word}_{geo}.xlsx') as writer:
            data_m.to_excel(writer,'monthly')
            data_d=data_d.join(daily, how='outer')
            data_d.to_excel(writer,'daily')
        # Set up start and stop dates
    
        #stop date truncated to today. Otherwise google may throw an error
    
    
        return 
    
    
def getDailyDataFromDB(dbpath: str,
                       word: str,
                       geo: str = 'US',
                       cat: int = 0,
                       start_year: int = 2004,
                       stop_year: int = date.today().year,
                       uniformdata: bool = True
                       ): 
    
    """dbpath - path to database
    word - keyword to pull
    geo - locale
    start/stop_year - obvious
    uniformdata: True if all data was pulled using either hi or low precision
                 The code will then take medians by each day
                 False if the data contains both approaches. 
                 The code will then take monthly averages for all pulled histories
                 scale them to match monthly (median) data, and then take daily medians
                 
    """  
    # Set up start and stop dates          
    start_date = date(start_year, 1, 1)
    stop_date = min([date(stop_year, 12, 31),date.today()])   
    #stop date truncated to today. Otherwise google may throw an error
    if isfile(f'{dbpath}/{cat}_{word}_{geo}.xlsx'):
        data_m=pd.read_excel(f'{dbpath}/{cat}_{word}_{geo}.xlsx',sheet_name='monthly').set_index('date')[start_date:stop_date]
        data_d=pd.read_excel(f'{dbpath}/{cat}_{word}_{geo}.xlsx',sheet_name='daily').set_index('date')[start_date:stop_date]
    else:
        print(f"No data found for search term 'Category{cat}/{word}/{geo}'") 
        return 
    
 
    monthly_med=pd.DataFrame(data_m.T.median(),columns=[f'{word}_{geo}'])
    #takes the median of monthly values. Median instead of average to account for possible zero values

    if uniformdata:
        daily_med=pd.DataFrame(data_d[start_date:stop_date].T.median(),columns=[f'{word}_{geo}'])
        dailyaverage=daily_med.resample('M').mean() #generate monthly averages
        dailyaverage.index=dailyaverage.index.values.astype('<M8[M]') #above produces weird dates, so cut them
        daily_med[f'{word}_{geo}_avg']=dailyaverage
        daily_med[f'{word}_{geo}_avg'].ffill(inplace=True)  #fill in forward
        complete = daily_med.join(monthly_med, lsuffix=f'_dailyraw', rsuffix=f'_monthly')
    
        # Scale daily data by monthly weights so the data is comparable
        complete[f'{word}_{geo}_monthly'].ffill(inplace=True)  # fill NaN values
        complete['scale'] = complete[f'{word}_{geo}_monthly']/complete[f'{word}_{geo}_avg']
        complete[f'{word}_{geo}'] = complete[f'{word}_{geo}_dailyraw']*complete.scale
        complete.drop(columns=[f'{word}_{geo}_dailyraw',f'{word}_{geo}_avg','scale'], inplace=True)
        return complete
    else:
            
        dailyaverage=data_d.resample('M').mean() #generate monthly averages
        dailyaverage.index=dailyaverage.index.values.astype('<M8[M]') #above produces weird dates, so cut them
        scale_m=dailyaverage.div(monthly_med.iloc[:,0], axis='index') 
        #this divides monthly averages of daily data by monthly series values to produce scaling
        scale_d=data_d.iloc[:,0:1].join(scale_m,lsuffix='_d').iloc[:,1:].copy().ffill()
        #this producing daily-frequency scaling factors (by merging into the daily dataframe chunk to preserve the index)
        data_d_scaled=data_d.div(scale_d)
        #this is scaled data
        daily_scaled_med=pd.DataFrame(data_d_scaled.T.median(),columns=[f'{word}_{geo}'])
        #this takes median of scaled numbers
        
        complete = daily_scaled_med.join(monthly_med, rsuffix=f'_monthly')
        #this joins the data with monthly series to be returned
        complete[f'{word}_{geo}_monthly'].ffill(inplace=True)  # fill NaN values
        
        return complete



    return 


  def getMonthlyDataFromDB(dbpath: str,
                       word: str,
                       geo: str = 'US',
                       cat: int = 0,
                       start_year: int = 2004,
                       stop_year: int = date.today().year,
                       uniformdata: bool = True
                       ): 
    
    """dbpath - path to database
    word - keyword to pull
    geo - locale
    start/stop_year - obvious
    uniformdata: True if all data was pulled using either hi or low precision
                 The code will then take medians by each day
                 False if the data contains both approaches. 
                 The code will then take monthly averages for all pulled histories
                 scale them to match monthly (median) data, and then take daily medians
                 
    """  
    # Set up start and stop dates          
    start_date = date(start_year, 1, 1)
    stop_date = min([date(stop_year, 12, 31),date.today()])   
    #stop date truncated to today. Otherwise google may throw an error
    if isfile(f'{dbpath}/{cat}_{word}_{geo}.xlsx'):
        data_m=pd.read_excel(f'{dbpath}/{cat}_{word}_{geo}.xlsx',sheet_name='monthly').set_index('date')[start_date:stop_date]
    else:
        print(f"No data found for search term 'Category{cat}/{word}/{geo}'") 
        return 
    
 
    monthly_med=pd.DataFrame(data_m.T.median(),columns=[f'{word}_{geo}'])
    #takes the median of monthly values. Median instead of average to account for possible zero values


    return monthly_med

def buildMonthlyDatabase(dbpath: str,
                  word: str,
                  geo: str = 'US',
                  cat: int = 0,
                  tz: int = 240
                  ): 
    
    if isfile(f'{dbpath}/{cat}_{word}_{geo}.xlsx'):
        data_m=pd.read_excel(f'{dbpath}/{cat}_{word}_{geo}.xlsx',sheet_name='monthly').set_index('date')
        # pretty inefficient. Consider re-writing to just dump new columns
    else:
        data_m=pd.DataFrame()
    
    if word+date.today().strftime("_%m_%d_%Y") in set(data_m.columns):
        # if today's data has been donwloaded already, google will send the same data again
        print(f"Today's data already present in the database for search term '{word}/{geo}'") 
        return 
    else:
        start_date = date(2004, 1, 1)
        stop_date = date.today()   
        pytrends = TrendReq(hl='en-US', tz=tz)
        # Initialize build_payload with the word we need data for
        build_payload = partial(pytrends.build_payload,
                                kw_list=[word], cat=cat, geo=geo, gprop='')
    
        # Obtain monthly data for all months in years [2004, stop_year]
        monthly = _fetchData(pytrends, build_payload,
                             getTimeframe(start_date, stop_date)).drop(columns=['isPartial'])   
        monthly.rename(columns={word:word+date.today().strftime("_%m_%d_%Y")},inplace=True)

        data_m=data_m.join(monthly, how='outer')
        with pd.ExcelWriter(f'{dbpath}/{cat}_{word}_{geo}.xlsx') as writer:
            data_m.to_excel(writer,'monthly')

    
        #stop date truncated to today. Otherwise google may throw an error
    
    
        return 
