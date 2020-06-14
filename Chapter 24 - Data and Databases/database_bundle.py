import pandas as pd
from sqlalchemy import create_engine 
from tqdm import tqdm # Used for progress bar

engine = create_engine('mysql+mysqlconnector://root:root@localhost/mimisbrunnr')

def available_stocks():
    symbol_query = "select distinct ticker from equity_history order by ticker"
    symbols = pd.read_sql_query(symbol_query, engine)
    return symbols.ticker
    

"""
The ingest function needs to have this exact signature,
meaning these arguments passed, as shown below.
"""
def database_bundle(environ,
                  asset_db_writer,
                  minute_bar_writer,
                  daily_bar_writer,
                  adjustment_writer,
                  calendar,
                  start_session,
                  end_session,
                  cache,
                  show_progress,
                  output_dir):
    
    # Get list of files from path
    # Slicing off the last part
    # 'example.csv'[:-4] = 'example'
    symbols = available_stocks()        
        
    # Prepare an empty DataFrame for dividends
    divs = pd.DataFrame(columns=['sid', 
                                 'amount',
                                 'ex_date', 
                                 'record_date',
                                 'declared_date', 
                                 'pay_date']
    )
    
    # Prepare an empty DataFrame for splits
    splits = pd.DataFrame(columns=['sid',
                                   'ratio',
                                   'effective_date']
    )
    
    # Prepare an empty DataFrame for metadata
    metadata = pd.DataFrame(columns=('start_date',
                                              'end_date',
                                              'auto_close_date',
                                              'symbol',
                                              'exchange'
                                              )
                                     )


    # Check valid trading dates, according to the selected exchange calendar
    sessions = calendar.sessions_in_range(start_session, end_session)
    
    # Get data for all stocks and write to Zipline
    daily_bar_writer.write(
            process_stocks(symbols, sessions, metadata, divs)
            )

    # Write the metadata
    asset_db_writer.write(equities=metadata)
    
    # Write splits and dividends
    adjustment_writer.write(splits=splits,
                            dividends=divs)    
    
    
"""
Generator function to iterate stocks,
build historical data, metadata 
and dividend data
"""
def process_stocks(symbols, sessions, metadata, divs):
    # Loop the stocks, setting a unique Security ID (SID)
    
    sid = 0
    for symbol in tqdm(symbols):
        sid += 1
        
        
        # Make a database query
        query = """select 
                    trade_date as date, open, high, low, close, volume, dividend 
                    from equity_history where ticker='{}' order by trade_date;
            """.format(symbol)
        
        # Ask the database for the data
        df = pd.read_sql_query(query, engine, index_col='date', parse_dates=['date']) 

        # Check first and last date.
        start_date = df.index[0]
        end_date = df.index[-1]        
        
        # Synch to the official exchange calendar
        df = df.reindex(sessions.tz_localize(None))[start_date:end_date]
        
        # Forward fill missing data
        df.fillna(method='ffill', inplace=True)
        
        # Drop remaining NaN
        df.dropna(inplace=True)    
        
        # The auto_close date is the day after the last trade.
        ac_date = end_date + pd.Timedelta(days=1)
        
        # Add a row to the metadata DataFrame.
        metadata.loc[sid] = start_date, end_date, ac_date, symbol, 'NYSE'
        
        # If there's dividend data, add that to the dividend DataFrame
        if 'dividend' in df.columns:
            
            # Slice off the days with dividends
            tmp = df[df['dividend'] != 0.0]['dividend']
            div = pd.DataFrame(data=tmp.index.tolist(), columns=['ex_date'])
            
            # Provide empty columns as we don't have this data for now
            div['record_date'] = pd.NaT
            div['declared_date'] = pd.NaT
            div['pay_date'] = pd.NaT            
            
            # Store the dividends and set the Security ID
            div['amount'] = tmp.tolist()
            div['sid'] = sid
            
            # Start numbering at where we left off last time
            ind = pd.Index(range(divs.shape[0], divs.shape[0] + div.shape[0]))
            div.set_index(ind, inplace=True)
            
            # Append this stock's dividends to the list of all dividends
            divs = divs.append(div)    
            
        yield sid, df