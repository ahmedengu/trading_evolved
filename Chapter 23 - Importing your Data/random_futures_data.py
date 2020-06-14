import pandas as pd
from os import listdir
from tqdm import tqdm # Used for progress bar

# Change the path to where you have your data
base_path = "C:/Users/Andreas Clenow/BookSamples/BookModels/data/"
data_path = base_path + 'random_futures/'
meta_path = 'futures_meta/meta.csv'
futures_lookup = pd.read_csv(base_path + meta_path, index_col=0)

"""
The ingest function needs to have this exact signature,
meaning these arguments passed, as shown below.
"""
def random_futures_data(environ,
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
    symbols = [f[:-4] for f in listdir(data_path)]
    
    if not symbols:
        raise ValueError("No symbols found in folder.")
        
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
                                      'root_symbol',
                                      'expiration_date',
                                      'notice_date',
                                      'tick_size',
                                      'exchange'
                                      )
                            )

    # Check valid trading dates, according to the selected exchange calendar
    sessions = calendar.sessions_in_range(start_session, end_session)

    # Get data for all stocks and write to Zipline
    daily_bar_writer.write(
            process_futures(symbols, sessions, metadata)
            )
    
    adjustment_writer.write(splits=splits, dividends=divs)    
    
    # Prepare root level metadata
    root_symbols = futures_lookup.copy()
    root_symbols['root_symbol_id'] = root_symbols.index.values
    del root_symbols['minor_fx_adj']
    
    #write the meta data
    asset_db_writer.write(futures=metadata, root_symbols=root_symbols)
   
def process_futures(symbols, sessions, metadata):
    # Loop the stocks, setting a unique Security ID (SID)
    sid = 0
    
    # Loop the symbols with progress bar, using tqdm
    for symbol in tqdm(symbols, desc='Loading data...'):
        sid += 1
       
        # Read the stock data from csv file.
        df = pd.read_csv('{}/{}.csv'.format(data_path, symbol), index_col=[0], parse_dates=[0]) 
 
        # Check for minor currency quotes
        adjustment_factor = futures_lookup.loc[
                futures_lookup['root_symbol'] == df.iloc[0]['root_symbol']
                ]['minor_fx_adj'].iloc[0]
        
        df['open'] *= adjustment_factor
        df['high'] *= adjustment_factor
        df['low'] *= adjustment_factor
        df['close'] *= adjustment_factor

        # Avoid potential high / low data errors in data set
        # And apply minor currency adjustment for USc quotes
        df['high'] = df[['high', 'close']].max(axis=1) 
        df['low'] = df[['low', 'close']].min(axis=1) 
        df['high'] = df[['high', 'open']].max(axis=1)
        df['low'] = df[['low', 'open']].min(axis=1) 

        # Synch to the official exchange calendar
        df = df.reindex(sessions.tz_localize(None))[df.index[0]:df.index[-1] ]
        
        # Forward fill missing data
        df.fillna(method='ffill', inplace=True)
        
        # Drop remaining NaN
        df.dropna(inplace=True)     
        
        # Cut dates before 2000, avoiding Zipline issue
        df = df['2000-01-01':]
        
        # Prepare contract metadata
        make_meta(sid, metadata, df, sessions)
        
        del df['openinterest']
        del df['expiration_date']
        del df['root_symbol']
        del df['symbol']
        
        yield sid, df        
        
def make_meta(sid, metadata, df, sessions):
        # Check first and last date.
        start_date = df.index[0]
        end_date = df.index[-1]        

        # The auto_close date is the day after the last trade.
        ac_date = end_date + pd.Timedelta(days=1)
        
        symbol = df.iloc[0]['symbol']
        root_sym = df.iloc[0]['root_symbol']
        exchng = futures_lookup.loc[futures_lookup['root_symbol'] == root_sym ]['exchange'].iloc[0]
        exp_date = end_date
        
        # Add notice day if you have.
        # Tip to improve: Set notice date to one month prior to
        # expiry for commodity markets.
        notice_date = ac_date
        tick_size = 0.0001   # Placeholder

        # Add a row to the metadata DataFrame.
        metadata.loc[sid] = start_date, end_date, ac_date, symbol, \
                                root_sym, exp_date, notice_date, tick_size, exchng
    
    