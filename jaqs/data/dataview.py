# encoding: utf-8

import os
import numpy as np
import pandas as pd

from jaqs.data.align import align
from jaqs.data.py_expression_eval import Parser
import jaqs.util as jutil


class DataView(object):
    """
    Prepare data before research / trade. Support file I/O.
    Support: add field, add formula, save / load.

    Attributes
    ----------
    symbol : list
    start_date : int
    end_date : int
    fields : list
    data_d : pd.DataFrame
        All daily frequency data will be merged and stored here.
        index is date, columns is symbol-field MultiIndex
    data_q : pd.DataFrame
        All quarterly frequency data will be merged and stored here.
        index is date, columns is symbol-field MultiIndex

    """
    #const
    ANN_DATE_FIELD_NAME = 'ann_date'
    REPORT_DATE_FIELD_NAME = 'report_date'
    TRADE_DATE_FIELD_NAME = 'trade_date'

    def __init__(self, datasrv=None, props=None, base="stock"):
        """
        """
        if base == "stock": #股票数据
            self._daily_field = ['open','close','high','low','vol','amount','trade_status','hfq_close','listday_matrix']
            self._quarterly_field = []
        elif base == "future": #期货数据
            self._daily_field = []
            self._quarterly_field = []
        else:
            raise ValueError("不支持的数据种类")

        if datasrv and props: #如果设置了数据源和配置参数
            self.datasrv = datasrv
            #保存参数
            self._benchmark = props['benchmark']  #对比基准指数
            self._universe = props['universe']    #选股空间
            self.start_date = props['start_date'] #时间范围开始
            self.end_date = props['end_date']     #时间范围结束
            self.extended_start_date = jutil.shift(self.start_date, n_weeks=-8) #为了预留更早的数据
            #查询选股空间所有符号
            self.symbol = self._query_universe_range()
            #研究的时间范围内所有交易日列表
            self.dates = self.datasrv.adjfactor.columns.tolist()
            tdays = pd.Series(self.dates, index=self.dates)
            tdays = tdays[(tdays>=self.extended_start_date)&(tdays<=self.end_date)]
            self.dates = tdays.index.tolist()
            #
            self.fields = []
            self.data_d = None
            self.data_q = None
            self._benchmark_data = None

    def _query_universe_range(self):
        """ 查询选股空间所有符号
        """
        if not self._universe:
            return self.datasrv.adjfactor.index.tolist()
        if self._universe == '000300.SH':
            df = self.datasrv.hs300_member
        elif self._universe == '000905.SH':
            df = self.datasrv.zz500_member
        else:
            raise ValueError("Index name [{:s}] not exist.".format(self._universe))
        df[df==0] = np.nan
        df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")
        return df.index.tolist()

    def prepare_data(self):
        """Prepare data for the FIRST time."""
        # prepare benchmark and group
        print("Query data...")
        data_d, data_q = self._prepare_daily_quarterly()
        self.data_d, self.data_q = data_d, data_q
        self._align_and_merge_q_into_d()
        #设置指数成员
        if self._universe:
            self.set_index_member(self._universe)
        #设置对比基准
        if self._benchmark:
            self.benchmark = self._benchmark
        print("Data has been successfully prepared.")

    def set_index_member(self, index_name):
        """ 设置指数成员字段
        """
        if 'index_member' in self.fields:
            return
        if self.datasrv is None:
            raise ValueError("Data source not exist")
        if not index_name:
            return
        if index_name == '000300.SH':
            self.add_field('index_member', self.datasrv.hs300_member.T)
        elif index_name == '000905.SH':
            self.add_field('index_member', self.datasrv.zz500_member.T)
        else:
            raise ValueError("Index name [{:s}] not exist.".format(index_name))

    @property
    def benchmark(self):
        """ 获取对比基准
        """
        return self._benchmark_data

    @benchmark.setter
    def benchmark(self, index_name):
        """ 设置对比基准
        """
        if self._benchmark_data is not None:
            return
        if not index_name:
            return
        if index_name not in ['000300.SH', '000905.SH']:
            raise ValueError("Index name [{:s}] not exist.".format(index_name))
        if self.datasrv is None:
            raise ValueError("Data source not exist")
        bm_high = self.datasrv.bm_high
        bm_open = self.datasrv.bm_open
        bm_low = self.datasrv.bm_low
        bm_close = self.datasrv.bm_close
        df = pd.DataFrame(index=bm_close.columns)
        df['high'] = bm_high.loc[index_name]
        df['open'] = bm_open.loc[index_name]
        df['low'] = bm_low.loc[index_name]
        df['close'] = bm_close.loc[index_name]
        tdays = pd.Series(self.dates, index=self.dates)
        tdays = tdays[(tdays>=self.start_date)&(tdays<=self.end_date)]
        self._benchmark_data = df.loc[tdays.index]

    @property
    def source(self):
        """ 获取数据源
        """
        return self.datasrv

    @source.setter
    def source(self, datasrv):
        """ 设置数据源
        """
        self.datasrv = datasrv

    def _prepare_daily_quarterly(self):
        """
        Query and process data from data_api.

        Parameters
        ----------
        fields : list

        Returns
        -------
        merge_d : pd.DataFrame or None
        merge_q : pd.DataFrame or None

        """
        # query data
        print("Query data - query...")
        daily_list, quarterly_list = self._init_basic_data()

        def pivot_and_sort(df, index_name):
            df = df.drop_duplicates(subset=['symbol', index_name])
            df = df.pivot(index=index_name, columns='symbol')
            df.columns = df.columns.swaplevel()
            col_names = ['symbol', 'field']
            df.columns.names = col_names
            df = df.sort_index(axis=1, level=col_names)
            df.index.name = index_name
            return df

        multi_daily = None
        multi_quarterly = None
        if daily_list:
            daily_list_pivot = [pivot_and_sort(df, self.TRADE_DATE_FIELD_NAME) for df in daily_list]
            multi_daily = self._merge_data(daily_list_pivot, self.TRADE_DATE_FIELD_NAME)
            multi_daily = self._fill_missing_idx_col(multi_daily, index=self.dates, symbols=self.symbol)
            print("Query data - daily fields prepared.")
        if quarterly_list:
            quarterly_list_pivot = [pivot_and_sort(df, self.REPORT_DATE_FIELD_NAME) for df in quarterly_list]
            multi_quarterly = self._merge_data(quarterly_list_pivot, self.REPORT_DATE_FIELD_NAME)
            multi_quarterly = self._fill_missing_idx_col(multi_quarterly, index=None, symbols=self.symbol)
            print("Query data - quarterly fields prepared.")

        return multi_daily, multi_quarterly

    def _get_df(self, name):
        """
        """
        df = getattr(self.datasrv, name, None)
        df = df.loc[self.symbol, self.dates]
        df.index.name = 'symbol'
        df.columns.name = 'trade_date'
        df = pd.DataFrame(df.stack(dropna=False))
        df.columns = [name]
        df = df.reset_index()
        return df

    def _init_basic_data(self):
        """
        Query data using different APIs, then store them in dict.
        period, start_date and end_date are fixed.
        Keys of dict are securitites.

        Returns
        -------
        daily_list : list
        quarterly_list : list

        """
        daily_list = []
        quarterly_list = []

        for name in self._daily_field:
            df = self._get_df(name)
            daily_list.append(df)
            self.fields.append(name)

        return daily_list, quarterly_list

    @staticmethod
    def _merge_data(dfs, index_name='trade_date', join='outer', keep_input=True):
        """
        Merge data from different APIs into one DataFrame.

        Parameters
        ----------
        dfs : list of pd.DataFrame

        Returns
        -------
        merge : pd.DataFrame or None
            If dfs is empty, return None

        Notes
        -----
        Align on date index, concatenate on columns (symbol and fields)

        """
        # dfs = [df for df in dfs if df is not None]
        new_dfs = []
        # column level swap: [symbol, field] => [field, symbol]
        for df in dfs:
            if keep_input:
                df_new = df.copy()
            else:
                df_new = df
            df_new.columns = df_new.columns.swaplevel()
            df_new = df_new.sort_index(axis=1)
            new_dfs.append(df_new)

        index_set = None
        for df in new_dfs:
            if index_set is None:
                index_set = set(df.index)
            else:
                if join == 'inner':
                    index_set = index_set & set(df.index)
                else:
                    index_set = index_set | set(df.index)
        index_list = list(index_set)
        index_list.sort()
            
        cols = None
        for df in new_dfs:
            if cols is None:
                cols = df.columns
            else:
                cols = cols.append(df.columns)
        
        merge = pd.DataFrame(data=np.nan, index=index_list, columns=cols)
        
        for df in new_dfs:
            for col in df.columns.levels[0]:
                if not df[col].empty:
                    merge[col] = df[col]
        
        merge.columns = merge.columns.swaplevel()
        merge = merge.sort_index(axis=1)
    
        # merge1 = pd.concat(dfs, axis=1, join='outer')
        # drop duplicated columns. ONE LINE EFFICIENT version
        mask_duplicated = merge.columns.duplicated()
        if np.any(mask_duplicated):
            # print("Duplicated columns found. Dropped.")
            merge = merge.loc[:, ~mask_duplicated]

            # if merge.isnull().sum().sum() > 0:
            # print "WARNING: nan in final merged data. NO fill"
            # merge.fillna(method='ffill', inplace=True)

        merge = merge.sort_index(axis=1, level=['symbol', 'field'])
        merge.index.name = index_name

        return merge

    def _fill_missing_idx_col(self, df, index=None, symbols=None):
        if index is None:
            index = df.index
        if symbols is None:
            symbols = df.columns.levels[0]
        fields = df.columns.levels[1]

        if len(fields) * len(symbols) != len(df.columns) or set(index) != set(df.index):
            cols_multi = pd.MultiIndex.from_product([fields, symbols], names=['field', 'symbol'])
            cols_multi = cols_multi.sort_values()

            df_final = pd.DataFrame(index=index, columns=cols_multi, data=np.nan)
            df_final.index.name = df.index.name
            
            df.columns = df.columns.swaplevel()
            df = df.sort_index(axis=1)
            
            for col in df.columns.levels[0]:
                df_final[col] = df[col]
            
            df_final.columns = df_final.columns.swaplevel()
            df_final = df_final.sort_index(axis=1)

            idx_diff = np.append(df_final.index.difference(set(df.index)), df.index.difference(set(df_final.index)))
            col_diff = np.append(
                df_final.columns.levels[0].difference(set(df.columns.levels[1].values)), 
                df.columns.levels[1].difference(set(df_final.columns.levels[0].values)))

            print("WARNING: some data is unavailable: "
                   + "\n    At index "  + np.str(idx_diff)
                   + "\n    At fields " + np.str(col_diff))
            return df_final
        else:
            return df

    def _align_and_merge_q_into_d(self):
        data_d, data_q = self.data_d, self.data_q
        if data_d is not None and data_q is not None:
            df_ref_ann = data_q.loc[:, pd.IndexSlice[:, self.ANN_DATE_FIELD_NAME]].copy()
            df_ref_ann.columns = df_ref_ann.columns.droplevel(level='field')

            dic_expanded = dict()
            for field_name, df in data_q.groupby(level=1, axis=1):  # by column multiindex fields
                df_expanded = align(df, df_ref_ann, self.dates)
                dic_expanded[field_name] = df_expanded
            df_quarterly_expanded = pd.concat(dic_expanded.values(), axis=1)
            df_quarterly_expanded.index.name = self.TRADE_DATE_FIELD_NAME

            data_d_merge = self._merge_data([data_d, df_quarterly_expanded], index_name=self.TRADE_DATE_FIELD_NAME)
            data_d = data_d_merge.loc[data_d.index, :]
        self.data_d = data_d

    # --------------------------------------------------------------------------------------------------------
    # Add/Remove Fields&Formulas
    def _add_field(self, field_name):
        if field_name in self.fields:
            return
        self.fields.append(field_name)

    def _create_parser(self, formula_func_name_style='camel', allow_future_data=False):
        parser = Parser(allow_future_data=allow_future_data)
        parser.set_capital(formula_func_name_style)
        return parser

    def add_formula(self, field_name, formula, formula_func_name_style='camel', within_index=True, is_factor=True):
        """
        Add a new field, which is calculated using existing fields.

        Parameters
        ----------
        formula : str or unicode
            A formula contains operations and function calls.
        field_name : str or unicode
            A custom name for the new field.
        formula_func_name_style : {'upper', 'lower'}, optional
        within_index : bool
            When do cross-section operatioins, whether just do within index components.
        is_factor: bool
            Whether new field is factor or label.

        Notes
        -----
        Time cost of this function:
            For a simple formula (like 'a + 1'), almost all time is consumed by add_field;
            For a complex formula (like 'GroupRank'), half of time is consumed by evaluation and half by add_field.
        """
        if field_name in self.fields:
            raise ValueError("Add formula failed: name [{:s}] exist. Try another name.".format(field_name))

        parser = self._create_parser(formula_func_name_style, allow_future_data=not is_factor)
        expr = parser.parse(formula)

        var_df_dic = dict()
        var_list = expr.variables()
        var_list = [var for var in var_list if var not in expr.functions]

        for var in var_list:
            if var not in self.fields:
                raise ValueError("Variable [{:s}] is not recognized (it may be wrong)".format(var))

        for var in var_list:
            # must use extended date. Default is start_date
            var_df_dic[var] = self.get_ts(var, start_date=self.extended_start_date, end_date=self.end_date)

        # TODO: send ann_date into expr.evaluate. We assume that ann_date of all fields of a symbol is the same
        df_ann = self._get_ann_df()
        if within_index:
            df_index_member = self.get_ts('index_member', start_date=self.extended_start_date, end_date=self.end_date)
            df_eval = parser.evaluate(var_df_dic, ann_dts=df_ann, trade_dts=self.dates, index_member=df_index_member)
        else:
            df_eval = parser.evaluate(var_df_dic, ann_dts=df_ann, trade_dts=self.dates)

        self.add_field(field_name, df_eval)

    def _get_ann_df(self):
        """
        Query announcement date of financial statements of all securities.

        Returns
        -------
        df_ann : pd.DataFrame or None
            Index is date, column is symbol.
            If no quarterly data available, return None.

        """
        if self.data_q is None:
            return None
        df_ann = self.data_q.loc[:, pd.IndexSlice[:, self.ANN_DATE_FIELD_NAME]]
        df_ann.columns = df_ann.columns.droplevel(level='field')

        return df_ann

    def add_field(self, field_name, df=None):
        """
        Append DataFrame to existing multi-index DataFrame and add corresponding field name.

        Parameters
        ----------
        df : pd.DataFrame or pd.Series
        field_name : str or unicode
        
        """
        if field_name in self.fields:
            raise ValueError("Add field failed: name [{:s}] exist. Try another name.".format(field_name))

        if df is None and self.datasrv is None:
            raise ValueError("Must have data source if df is null")

        if df is None:
            df = getattr(self.datasrv, field_name)

        df = df.copy()
        if isinstance(df, pd.DataFrame):
            pass
        elif isinstance(df, pd.Series):
            df = pd.DataFrame(df)
        else:
            raise ValueError("Data to be appended must be pandas format. But we have {}".format(type(df)))

        the_data = self.data_d

        # if field_name in the_data.columns.levels[1]:
        #     raise ValueError("The field already exists in DataView! " + field_name)

        # Copy exists symbols and set multi index
        df = df.loc[:, the_data.columns.levels[0]]
        multi_idx = pd.MultiIndex.from_product([[field_name], df.columns])
        df.columns = multi_idx
        df = df.sort_index(axis=1)

        the_data.columns = the_data.columns.swaplevel()
        the_data = the_data.sort_index(axis=1)

        if field_name not in self.fields or field_name not in the_data.columns.levels[0]:
            new_cols = the_data.columns.append(df.columns)
            the_data = the_data.reindex(columns=new_cols)

        the_data[field_name] = df[field_name]
        the_data.columns = the_data.columns.swaplevel()
        the_data = the_data.sort_index(axis=1)

        self.data_d = the_data
        self._add_field(field_name)

    # --------------------------------------------------------------------------------------------------------
    # Get Data API
    def get(self, symbol="", start_date=0, end_date=0, fields=""):
        """
        Basic API to get arbitrary data. If nothing fetched, return None.

        Parameters
        ----------
        symbol : str, optional
            Separated by ',' default "" (all securities).
        start_date : int, optional
            Default 0 (self.start_date).
        end_date : int, optional
            Default 0 (self.start_date).
        fields : str, optional
            Separated by ',' default "" (all fields).

        Returns
        -------
        res : pd.DataFrame or None
            index is datetimeindex, columns are (symbol, fields) MultiIndex

        """
        sep = ','

        if not fields:
            fields = slice(None)  # self.fields
        else:
            fields = fields.split(sep)

        if not symbol:
            symbol = slice(None)  # this is 3X faster than symbol = self.symbol
        else:
            symbol = symbol.split(sep)

        if not start_date:
            start_date = self.start_date
        if not end_date:
            end_date = self.end_date

        res = self.data_d.loc[pd.IndexSlice[start_date: end_date], pd.IndexSlice[symbol, fields]]
        return res

    def get_snapshot(self, snapshot_date, symbol="", fields=""):
        """
        Get snapshot of given fields and symbol at snapshot_date.

        Parameters
        ----------
        snapshot_date : int
            Date of snapshot.
        symbol : str, optional
            Separated by ',' default "" (all securities).
        fields : str, optional
            Separated by ',' default "" (all fields).

        Returns
        -------
        res : pd.DataFrame
            symbol as index, field as columns

        """
        res = self.get(symbol=symbol, start_date=snapshot_date, end_date=snapshot_date, fields=fields)
        if res is None:
            raise ValueError("No data. for date={}, fields={}, symbol={}".format(snapshot_date, fields, symbol))

        res = res.stack(level='symbol', dropna=False)
        res.index = res.index.droplevel(level=self.TRADE_DATE_FIELD_NAME)

        return res

    def get_symbol(self, symbol, start_date=0, end_date=0, fields=""):
        """
        """
        res = self.get(symbol, start_date=start_date, end_date=end_date, fields=fields)
        if res is None:
            raise ValueError("No data. for start_date={}, end_date={}, field={}, symbol={}".format(start_date, end_date, fields, symbol))

        res.columns = res.columns.droplevel(level='symbol')
        return res

    def get_ts(self, field, symbol="", start_date=0, end_date=0, keep_level=False):
        """
        Get time series data of single field.

        Parameters
        ----------
        field : str or unicode
            Single field.
        symbol : str, optional
            Separated by ',' default "" (all securities).
        start_date : int, optional
            Default 0 (self.start_date).
        end_date : int, optional
            Default 0 (self.start_date).

        Returns
        -------
        res : pd.DataFrame
            Index is int date, column is symbol.

        """
        res = self.get(symbol, start_date=start_date, end_date=end_date, fields=field)
        if res is None:
            raise ValueError("No data. for start_date={}, end_date={}, field={}, symbol={}".format(start_date, end_date, field, symbol))

        #if not keep_level and len(res.columns) and len(field.split(',')) == 1:
        if not keep_level and len(field.split(',')) == 1:
            res.columns = res.columns.droplevel(level='field')
            # XXX Save field name for ResReturnFunc
            res.columns.name = field

        return res

    def save_dataview(self, folder_path):
        """
        Save data and meta_data_to_store to a single hd5 file.
        Store at output/sub_folder

        Parameters
        ----------
        folder_path : str or unicode
            Path to store your data.

        """
        folder_path = os.path.abspath(folder_path)
        meta_path = os.path.join(folder_path, 'meta_data.json')
        data_path = os.path.join(folder_path, 'data.hd5')
        data_to_store = {'data_d': self.data_d,
                         'data_q': self.data_q,
                         '_benchmark_data': self._benchmark_data}
        data_to_store = {k: v for k, v in data_to_store.items() if v is not None}
        meta_data_list = ['start_date', 'end_date', 'extended_start_date', 'dates', 'fields', 'symbol', '_universe', '_benchmark']
        meta_data_to_store = {key: self.__dict__[key] for key in meta_data_list}
        print("\nStore data...")
        jutil.save_json(meta_data_to_store, meta_path)
        self._save_h5(data_path, data_to_store)
        print("Dataview has been successfully saved to:\n"
              + folder_path + "\n\n"
              + "You can load it with load_dataview('{:s}')".format(folder_path))

    @staticmethod
    def _save_h5(fp, dic):
        """
        Save data in dic to a hd5 file.

        Parameters
        ----------
        fp : str
            File path.
        dic : dict

        """
        import warnings
        warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)
        jutil.create_dir(fp)
        h5 = pd.HDFStore(fp, complevel=9, complib='blosc')
        for key, value in dic.items():
            h5[key] = value
        h5.close()

    @staticmethod
    def _load_h5(fp):
        """Load data and meta_data from hd5 file.

        Parameters
        ----------
        fp : str, optional
            File path of pre-stored hd5 file.

        """
        h5 = pd.HDFStore(fp)
        res = dict()
        for key in h5.keys():
            res[key] = h5.get(key)
        h5.close()
        return res

    def load_dataview(self, folder_path='.'):
        """
        Load data from local file.

        Parameters
        ----------
        folder_path : str, optional
            Folder path to store hd5 file and meta data.

        """
        folder_path = os.path.abspath(folder_path)
        path_meta_data = os.path.join(folder_path, 'meta_data.json')
        path_data = os.path.join(folder_path, 'data.hd5')
        if not (os.path.exists(path_meta_data) and os.path.exists(path_data)):
            raise IOError("There is no data file under directory {}".format(folder_path))
        meta_data = jutil.read_json(path_meta_data)
        dic = self._load_h5(path_data)
        self.data_d = dic.get('/data_d', None)
        self.data_q = dic.get('/data_q', None)
        self._benchmark_data = dic.get('/_benchmark_data', None)
        self.__dict__.update(meta_data)
        print("Dataview loaded successfully.")