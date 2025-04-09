import pandas as pd
from taipy.gui import Gui
from datetime import datetime
import taipy.gui.builder as tgb
from math import cos, exp

from jaref_bot.db.postgres_manager import DBManager
from jaref_bot.config.credentials import host, user, password, db_name
postgre_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
postgre_manager = DBManager(postgre_params)

def load_data(token: str) -> pd.DataFrame:
    df = postgre_manager.get_token_history(token)
    df = df[df['exchange'] == 'bybit'][['bucket', 'avg_bid', 'avg_ask']]
    df['avg_bid'] = df['avg_bid'].astype(float)
    df['avg_ask'] = df['avg_ask'].astype(float)
    return df.tail(200)

# ct = datetime.now().strftime('%H:%M')

token_1 = 'token_1'
token_2 = 'token_2'
df_1 = pd.DataFrame(columns=['bucket', 'avg_bid', 'avg_ask'])
df_2 = pd.DataFrame(columns=['bucket', 'avg_bid', 'avg_ask'])
token_list = postgre_manager.get_unique_tokens()

# main_df =

layout = {
    'xaxis': {'title': ''},
    'yaxis': {'title': ''},
    'margin': {
        'l': 50,  # Левый отступ
        'r': 10,  # Правый отступ
        't': 5,  # Верхний отступ
        'b': 15   # Нижний отступ
    }}

def update_token_1(state):
    state.df_1 = load_data(state.token_1)
    update_main_df(state)

def update_token_2(state):
    state.df_2 = load_data(state.token_2)
    update_main_df(state)

def update_main_df(state):
    if not state.df_1.empty and not state.df_2.empty:
        coef = (state.df_1['avg_bid'].mean() / state.df_2['avg_bid'].mean())
        print(coef)

with tgb.Page() as page:
    with tgb.part(class_name='container'):
        with tgb.layout(columns="3 2 15"):
            with tgb.part():
                tgb.html('br')
                tgb.selector(value="{token_1}",
                             lov=token_list,
                             on_change=update_token_1,
                             dropdown=True,
                             # width='150px',
                             label='bid price')
                tgb.html('br')
                tgb.selector(value="{token_2}",
                             lov=token_list,
                             on_change=update_token_2,
                             dropdown=True,
                             # width='150px',
                             label='ask price')
            with tgb.part(class_name='text-center'):
                tgb.text('Diff')
                tgb.text('0.18', class_name='positive')
                tgb.html('br')
                tgb.text('Mean')
                tgb.text('0.72', class_name='negative')
                tgb.html('br')
                tgb.text('Std')
                tgb.text('0.12')
            with tgb.part():
                tgb.chart("{df_1}", mode="lines", x="bucket", y__1="avg_bid", y__2='avg_ask',
                          layout="{layout}",
                        active=False, height='200px')
        tgb.html('br')
        with tgb.part(class_name='card'):
            tgb.button(label='ADD')
            # tgb.table(data=df_1, auto_loading=True, )


Gui(page=page).run(title="Pair Trading", use_reloader=True)
