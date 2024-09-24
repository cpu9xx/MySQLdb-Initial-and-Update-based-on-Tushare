import tushare as ts
import datetime
import pandas as pd
import time
import os
from sqlalchemy import create_engine, text, types
from email_sender import send_email
from sqlalchemy.orm import sessionmaker
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
import threading
from collections import Counter
import pickle

from functools import wraps
def timeit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()  # 记录开始时间
        result = func(*args, **kwargs)  # 执行被装饰的函数
        end_time = time.time()  # 记录结束时间
        elapsed_time = end_time - start_time  # 计算耗时
        print(f"Function '{func.__name__}' executed in {elapsed_time:.4f} seconds")
        send_email("update_db consume time：%.2f 秒" % elapsed_time, title='Update Database successfully')
        return result
    return wrapper

class Ts_gate():
    def __init__(self, token, start_date, end_date):
        ts.set_token(token)
        self.pro = ts.pro_api()
        self.start_date = start_date
        self.end_date = end_date

    def get_list(self):
        mkt_ls = ['主板', '创业板', '科创板', '北交所']
        df_ls = []
        for mkt in mkt_ls:
            df = self.pro.stock_basic(market=mkt, list_status='L', fields='ts_code,market,list_status,list_date,delist_date')
            df_d = self.pro.stock_basic(market=mkt, list_status='D', fields='ts_code,market,list_status,list_date,delist_date')
            df_d['delist_date'] = df_d['delist_date'].astype(int)
            df_d = df_d[df_d['delist_date'] >= int(self.start_date)]
            df_ls.append(df)
            df_ls.append(df_d)

        # main_df = self.pro.stock_basic(market='主板', list_status='L', fields='ts_code,market,list_status,list_date,delist_date')
        # main_df_d = self.pro.stock_basic(market='主板', list_status='D', fields='ts_code,market,list_status,list_date,delist_date')
        # df_300 = self.pro.stock_basic(market='创业板', list_status='L', fields='ts_code,market,list_status,list_date,delist_date')
        # df_300_d = self.pro.stock_basic(market='创业板', list_status='D', fields='ts_code,market,list_status,list_date,delist_date')
        # df_688 = self.pro.stock_basic(market='科创板', list_status='L', fields='ts_code,market,list_status,list_date,delist_date')
        # df_688_d = self.pro.stock_basic(market='科创板', list_status='D', fields='ts_code,market,list_status,list_date,delist_date')
        # df_48 = self.pro.stock_basic(market='北交所', list_status='L', fields='ts_code,market,list_status,list_date,delist_date')
        # df_48_d = self.pro.stock_basic(market='北交所', list_status='D', fields='ts_code,market,list_status,list_date,delist_date')
        
        # stock_df = pd.concat([main_df, main_df_d, df_300, df_300_d, df_688, df_688_d, df_48, df_48_d])

        return pd.concat(df_ls)['ts_code'].to_numpy()
    
    def get_trade_date(self):
        for i in range(30):
            try:
                df = self.pro.trade_cal(exchange='', start_date=self.start_date, end_date=self.end_date)
                df = df[df['is_open'] == 1]
                df = df.iloc[:: -1]
                break
            except:
                print(f'Retrying ({i})...')
                time.sleep(1)
        else:
            error_path = os.path.realpath(__file__) + '\n'
            error_msg = f'Error: get_trade_date overtime'
            print(error_msg)
            send_email(error_path + error_msg)
        return df['cal_date'].to_numpy()
    
    def get_data_by_date(self, date):
        for i in range(10):
            try:
                df = self.pro.stk_factor_pro(trade_date=date, fields='ts_code,trade_date,high,open,low,close,pre_close,change,vol,amount,adj_factor,open_hfq,close_hfq,high_hfq,low_hfq,turnover_rate_f,volume_ratio,pe_ttm,pb,ps_ttm,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv')
                if len(df) != 0:
                    df = df.fillna(-1)
                    return df
                else:
                    raise
            except:
                print(f'Retrying ({i})...')
                time.sleep(2)
        else:#没有break时执行
            error_path = os.path.realpath(__file__) + '\n'
            error_msg = f'Error: get_data_by_date {date} overtime'
            print(error_msg)
            send_email(error_path + error_msg)

    
    def get_data_by_name(self, name):
        current_process = multiprocessing.current_process()
        print(f"{current_process.name}: {name}")
        for i in range(30):
            try:
                df = self.pro.stk_factor_pro(ts_code=name, start_date=self.start_date, end_date=self.end_date, fields='ts_code,trade_date,high,open,low,close,pre_close,change,vol,amount,adj_factor,open_hfq,close_hfq,high_hfq,low_hfq,turnover_rate_f,volume_ratio,pe_ttm,pb,ps_ttm,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv')
                if len(df) != 0:
                    df = df.fillna(-1)
                    df = df.iloc[:: -1]
                    return df
                else:
                    raise
            except:
                print(f'Retrying ({i})...')
                time.sleep(2)
        else:#没有break时执行
            error_path = os.path.realpath(__file__) + '\n'
            error_msg = f'Error: get_data_by_name {name} overtime'
            print(error_msg)
            send_email(error_path + error_msg)
        
    
    def get_index_by_name(self, name):
        for i in range(10):
            try:
                df = self.pro.index_daily(ts_code=name, start_date=self.start_date, end_date=self.end_date)
                if len(df) != 0:
                    df = df.iloc[:: -1]
                    break
                else:
                    raise
            except:
                print(f'Retrying ({i})...')
                time.sleep(2)
        else:#没有break时执行
            error_path = os.path.realpath(__file__) + '\n'
            error_msg = f'Error: get_data_by_date {name} overtime'
            print(error_msg)
            send_email(error_path + error_msg)
        return df
    

class MySQL_gate():
    def __init__(self, start_date, end_date, **kwargs):
        self.start_date = start_date
        self.end_date = end_date
        self.db_config = kwargs
        self.StockSession = None
        self.IndexSession = None

        # stock_engine = create_engine(f"mysql+mysqldb://{kwargs['user']}:{kwargs['password']}@{kwargs['host']}/stock", pool_size=30)
        # index_engine = create_engine(f"mysql+mysqldb://{kwargs['user']}:{kwargs['password']}@{kwargs['host']}/index", pool_size=30)
        # self.StockSession = sessionmaker(bind=stock_engine)
        # self.IndexSession = sessionmaker(bind=index_engine)

    def init_session(self, db):
        stock_engine = create_engine(f"mysql+mysqldb://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}/stock", pool_size=5)
        index_engine = create_engine(f"mysql+mysqldb://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}/index", pool_size=5)
        self.StockSession = sessionmaker(bind=stock_engine)
        self.IndexSession = sessionmaker(bind=index_engine)
        current_process = multiprocessing.current_process()
        print(f"{current_process.name} init_session db: {db}")
        return self.get_session(db)

    # def get_session(self, db):
    #     try:
    #         if db == 'stock':
    #             return self.StockSession()
    #         elif db == 'index':
    #             return self.IndexSession()
    #         else:
    #             print(f"Error get_session with (db: {db})")
    #             return None
    #     except Exception as e:
    #         print(f"Error get_session('{db}'): {e}")
    #         return self.init_session(db)
        
    def get_session(self, db):
        try:
            if db == 'stock':
                if self.StockSession is not None:
                    return self.StockSession()
                else:
                    return self.init_session(db)
            elif db == 'index':
                if self.IndexSession is not None:
                    return self.IndexSession()
                else:
                    return self.init_session(db)
            else:
                print(f"Error get_session with (db: {db})")
                return None
        except Exception as e:
            print(f"Error get_session ('{db}'): {e}")
            return self.init_session(db)


    def read_data_date(self, tablename, db='stock', days=1):
        query = f"SELECT * FROM `{tablename}` ORDER BY trade_date DESC LIMIT {days}"
        df = self.execute_query(query, db, commit=False)
        return df

    def read_df(self, tablename, db='stock'):
        query = f"SELECT * FROM `{tablename}` WHERE trade_date >= '{self.start_date}' AND trade_date <= '{self.end_date}';"
        df = self.execute_query(query, db, commit=False)
        return df

    def execute_query(self, query, db, commit=False):
        session = self.get_session(db)

        try:
            result = session.execute(text(query))
            if commit:
                session.commit()
            if result.returns_rows:
                df = pd.DataFrame(result.fetchall())
                return df
            else:
                return result.rowcount
        except Exception as e:
            session.rollback()
            print(f"Error executing query('{query}'): {e}")
            return None
        finally:
            session.close()
        
    def write_line(self, df, table_name, is_new=False, db='stock'):
        session = self.get_session(db)
        table_name = table_name.lower()
        # print(table_name)
        try:
            last_data = self.read_data_date(tablename=table_name, db=db, days=3)
            if is_new or (last_data is None):
                dtype_mapping = {
                    'ts_code': types.VARCHAR(length=9),
                    'trade_date': types.TIMESTAMP(timezone=False)
                }
                for col in df.columns:
                    if col not in dtype_mapping:
                        dtype_mapping[col] = types.Double()
                df.to_sql(name=table_name, con=session.bind, index=False, if_exists='replace', dtype=dtype_mapping)
                self.execute_query(f"ALTER TABLE `{table_name}` ADD PRIMARY KEY (trade_date)", db, commit=True)
            else:
                last_amount = last_data['amount']
                print(df['amount'].values[0])
                print(last_amount.values)
                if df['amount'].values[0] not in last_amount.values:
                    df.to_sql(name=table_name, con=session.bind, index=False, if_exists='append')
                else:
                    error_path = os.path.realpath(__file__) + '\n'
                    error_msg = f"Error: write_line {table_name} amount:{df['amount'].values[0]} repeat"
                    print(error_msg)
                    send_email(error_path + error_msg)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error writing line('{table_name}'): {e}")
        finally:
            session.close()

    def write_df(self, df, table_name, is_new=False, db='stock'):
        session = self.get_session(db)
        table_name = table_name.lower()
        try:
            if is_new:
                dtype_mapping = {
                    'ts_code': types.VARCHAR(length=9),
                    'trade_date': types.TIMESTAMP(timezone=False)
                }
                for col in df.columns:
                    if col not in dtype_mapping:
                        dtype_mapping[col] = types.Double()
                df.to_sql(name=table_name, con=session.bind, index=False, if_exists='replace', dtype=dtype_mapping)
                self.execute_query(f"ALTER TABLE `{table_name}` ADD PRIMARY KEY (trade_date)", db, commit=True)
            else:
                df.to_sql(name=table_name, con=session.bind, index=False, if_exists='append')
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error writing df('{table_name}'): {e}")
        finally:
            session.close()
    
    def alter_table(self, table_name, alter_statement, db='stock'):
        session = self.get_session(db)

        try:
            self.execute_query(f"ALTER TABLE `{table_name}` {alter_statement}", db=db, commit=True)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error altering table('{table_name}'): {e}")
        finally:
            session.close()
        # if is_new:
        #     try: 
        #         self.execute_query(f"ALTER TABLE `{table_name}` ADD PRIMARY KEY (trade_date)", db)
        #     except:
        #         error_path = os.path.realpath(__file__) + '\n'
        #         error_msg = 'Error: setting primary key %s' % table_name
        #         print(error_msg)
        #         send_email(error_path + error_msg)

db_config = {
    "host": "localhost",
    "user": "root",
    "password": "137137a",
}

def write_line_wrapper(args):
    db_gate, row_df, is_new, db = args
    # current_process = multiprocessing.current_process()
    # print(f"{current_process.name}: {db_gate}")
    db_gate.write_line(row_df, row_df['ts_code'].iloc[0], is_new, db)

def write_df_wrapper(args):
    db_gate, row_df, is_new, db = args
    # current_process = multiprocessing.current_process()
    # print(f"{current_process.name}: {db_gate}")
    db_gate.write_df(row_df, row_df['ts_code'].iloc[0], is_new, db)

def get_data_by_name_wrapper(args):
    ts_gate, name = args
    print(f"get_data_by_name_wrapper {name}")
    # current_process = multiprocessing.current_process()
    # print(f"{current_process.name}: {db_gate}")
    ts_gate.get_data_by_name(name)

@timeit
def update_stock(start_date, end_date):
    ts_gate = Ts_gate(token="", start_date=start_date, end_date=end_date)
    db_gate = MySQL_gate(start_date=start_date, end_date=end_date, **db_config)
    trade_days = ts_gate.get_trade_date()
    print(trade_days)
    with multiprocessing.Pool(processes=28) as pool:
        for date in trade_days:
            df_by_date = ts_gate.get_data_by_date(date)
            pool.map(write_line_wrapper, [(db_gate, pd.DataFrame([row], columns=df_by_date.columns), False, 'stock') for row in df_by_date.itertuples(index=False)])

        pool.close()
        pool.join()

@timeit
def initial_stock(start_date, end_date):
    ts_gate = Ts_gate(token="", start_date=start_date, end_date=end_date)
    db_gate = MySQL_gate(start_date=start_date, end_date=end_date, **db_config)
    stk_ls = ts_gate.get_list()
    print(len(stk_ls))

    # df_ls = [ts_gate.get_data_by_name(stk) for stk in stk_ls]
    
    # print(len(df_ls))
    # print(df_ls[1])
    # with open('df_ls.pkl', 'wb') as f:
    #     pickle.dump(df_ls, f)
    
    with open('df_ls.pkl', 'rb') as f:
        df_ls = pickle.load(f)
    print(len(df_ls))
    df_ls = [df for df in df_ls if not df.empty]
    print(len(df_ls))

    with multiprocessing.Pool(processes=28) as pool:
        # df_ls = pool.map(get_data_by_name_wrapper, [(ts_gate, stk) for stk in stk_ls])
        # df_ls = pool.map(ts_gate.get_data_by_name, stk_ls)
        pool.map(write_df_wrapper, [(db_gate, df, True, 'stock') for df in df_ls])
        pool.close()
        pool.join()

@timeit
def initial_index(start_date, end_date, initial=False):
    ts_gate = Ts_gate(token="", start_date=start_date, end_date=end_date)
    db_gate = MySQL_gate(start_date=start_date, end_date=end_date, **db_config)

    index_list = ['399006.sz', '000300.sh', '000852.sh', '000905.sh', '000688.sh']
    is_new = initial
    
    for index in index_list:
        print(index)
        df = ts_gate.get_index_by_name(index)
        db_gate.write_line(df, df['ts_code'].iloc[0], is_new, 'index')

def test(start_date, end_date):
    db_gate = MySQL_gate(start_date=start_date, end_date=end_date, **db_config)
    df = db_gate.read_df('000001.sz')
    print(df)


if __name__ == '__main__':
    start_date = '20170101'
    # start_date = '20240716'
    end_date = (datetime.datetime.today() + datetime.timedelta(days=1)).strftime("%Y%m%d")
    test(start_date, end_date)

    # initial_stock(start_date, end_date)
    # start_date = (datetime.datetime.today()).strftime("%Y%m%d")
    # end_date = (datetime.datetime.today()).strftime("%Y%m%d")

    # initial_index(start_date, end_date, initial=True)
    # initial_index(start_date, end_date, initial=False)

