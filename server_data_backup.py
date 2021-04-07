import json
import io
import requests
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st
from time import sleep
from websocket import create_connection
from tvstreamhelper import generateSession, sendMessage, generateChartSession

# Initialize the headers needed for the websocket connection
def tv_headers():
    headers = json.dumps({
        'Origin': 'https://data.tradingview.com'
    })
    return headers

# Then create a connection to the tunnel
def newSession():
    ws = create_connection(
        'wss://data.tradingview.com/socket.io/websocket',
        headers=tv_headers()
    )
    session = generateSession()
    chart_session = generateChartSession()
    return ws, session, chart_session

def messagebox(ws, session, chart_session, ticker, resolution, bars):
    sendMessage(ws, "set_auth_token", ["unauthorized_user_token"])
    sendMessage(ws, "chart_create_session", [chart_session, ""])
    sendMessage(ws, "quote_create_session", [session])
    sendMessage(ws, "quote_set_fields", [session, "ch", "chp", "current_session", "description", "local_description",
                                         "language", "exchange", "fractional", "is_tradable", "lp", "lp_time",
                                         "minmov", "minmove2", "original_name", "pricescale", "pro_name", "short_name",
                                         "type", "update_mode", "volume", "currency_code", "rchp", "rtc"])
    sendMessage(ws, "quote_add_symbols", [session, ticker, {"flags": ['force_permission']}])
    sendMessage(ws, "quote_fast_symbols", [session, ticker])
    sendMessage(ws, "resolve_symbol", [chart_session,"symbol_1", "={\"symbol\":\"" + ticker + "\",\"adjustment\":\"splits\",\"session\":\"extended\"}"])
    sendMessage(ws, "create_series", [chart_session, "s1", "s1", "symbol_1", resolution, bars])


def fetch_raw_data(ticker, resolution, bars):
    ws, session, chart_session = newSession()
    messagebox(ws, session, chart_session, ticker, resolution, bars)

    search_tu = True
    while search_tu:
        try:
            result = ws.recv()
            result = result.split('~')

            for item in result:
                if 'timescale_update' in item:
                    item = pd.DataFrame([x['v'] for x in json.loads(item)['p'][1]['s1']['s']])
                    item.columns = ['epochtime', 'open', 'high', 'low', 'close', 'volume']
                    item['datetime'] = pd.to_datetime(item.epochtime, unit='s') + timedelta(hours=5.5)
                    item['time'] = item.datetime.dt.time
                    return item
                    search_tu=False

        except Exception as e:
            item = ''
            return item


def read_csv(URL):
    download = requests.get(URL).content
    try:
        df = pd.read_csv(io.StringIO(download.decode('utf-8')))
    except:
        df = pd.DataFrame()

    return df

def backup_data(csvfile, ticker, resolution, bars):
    st.write(f'Fetching Data for {ticker}')
    data = fetch_raw_data(ticker, resolution, bars)
    st.write(data.head())
    data['date'] = pd.to_datetime(data.datetime).dt.date
    data = data[data.date == datetime.now().date]
    data = data.drop('date', axis=1)
    csvfile = csvfile.append(data, ignore_index=True)
    csvfile.to_csv(f'{csvfile}.csv', index=False)
    st.write(csvfile.tail())


NIFTY_FUT_CURR_MO = read_csv('https://raw.githubusercontent.com/sumitkant/TradingViewDataStorage/main/NIFTY_FUT_CURR_MO.csv')
NIFTY_FUT_NEXT_MO = read_csv('https://raw.githubusercontent.com/sumitkant/TradingViewDataStorage/main/NIFTY_FUT_NEXT_MO.csv')
BANKNIFTY_FUT_CURR_MO = read_csv('https://raw.githubusercontent.com/sumitkant/TradingViewDataStorage/main/BANKNIFTY_FUT_CURR_MO.csv')
BANKNIFTY_FUT_NEXT_MO = read_csv('https://raw.githubusercontent.com/sumitkant/TradingViewDataStorage/main/BANKNIFTY_FUT_CURR_MO.csv')

st.title('Trading View Futures Data backup utility')
if st.button('Backup today\'s data'):

    backup_data(NIFTY_FUT_CURR_MO, 'NSE:NIFTY1!', 1, 500)
    backup_data(NIFTY_FUT_NEXT_MO, 'NSE:NIFTY2!', 1, 500)
    backup_data(BANKNIFTY_FUT_CURR_MO, 'NSE:BANKNIFTY1!', 1, 500)
    backup_data(BANKNIFTY_FUT_NEXT_MO, 'NSE:BANKNIFTY2!', 1, 500)
    st.write('All done')
