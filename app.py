from tracemalloc import start
from dash import Dash, html, Input, Output, callback, dash_table, dcc, ctx
import pandas as pd
import dash_bootstrap_components as dbc
from sqlalchemy.pool import NullPool
import pandas as pd
from sqlalchemy import text, create_engine
from tenacity import retry, wait_exponential, stop_after_attempt
import yaml
from datetime import date, datetime
from dash.dash_table.Format import Format, Group
from dash.dash_table import FormatTemplate
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

cred_stream = open("conf/local/credentials.yml", "r")
app_stream = open("conf/base/parameters/app.yml", "r")
cred_dict = yaml.safe_load(cred_stream)
app_config = yaml.safe_load(app_stream)

engine = create_engine(cred_dict['db_credentials']['con'])

stock_list_query = """select * from stock_list where stock_code in (select distinct stock_code from stock_participants)"""
stock_top10_participants_query = """select * from stock_top10_participants"""
stock_participants_hist_query = """select * from stock_participants where stock_code='{0}'""".format("00001")
#stock_participants_hist_query = """select * from stock_participants"""

def get_participants_hist_by_stock(stock_code: str):
    stock_participants_hist_query = """select * from stock_participants where stock_code='{0}'""".format(stock_code)
    @retry(wait=wait_exponential(multiplier=2, min=1, max=10), stop=stop_after_attempt(5))
    def try_connection(stock_participants_hist_query):
        try:
            with engine.connect() as connection:
                stmt = text("SELECT 1")
                connection.execute(stmt)
                df_stock_participants_hist = pd.read_sql(stock_participants_hist_query, connection)
            print("Connection to database successful.")
            return df_stock_participants_hist
        except Exception as e:
            print("Connection to database failed, retrying.")
        raise Exception
    return try_connection(stock_participants_hist_query)

stock_participant_diff_query = """select * from stock_participants where stock_code in (select stock_code from stock_list where stock_code = '{0}')""".format("00001")

@retry(wait=wait_exponential(multiplier=2, min=1, max=10), stop=stop_after_attempt(5))
def try_connection():
    try:
        with engine.connect() as connection:
            stmt = text("SELECT 1")
            connection.execute(stmt)
            df_stock_list = pd.read_sql(stock_list_query, connection)
            df_stock_top10_participants = pd.read_sql(stock_top10_participants_query, connection)
        print("Connection to database successful.")
        return df_stock_list, df_stock_top10_participants
    except Exception as e:
        print("Connection to database failed, retrying.")
        raise Exception

df_stock_list, df_stock_top10_participants = try_connection()
df_stock_participants_hist_sample = get_participants_hist_by_stock('00001')
#df_stock_participants_hist
stock_list = df_stock_list['stock_code']

global_min_date, global_max_date = app_config['config']['min_date'], app_config['config']['max_date']

# df = df_stock_participants[(df_stock_participants['business_date'] >= min_date) & (df_stock_participants['business_date'] <= max_date)]
# a = list(df.columns)
# b= a.pop()
# df = df[[b] + a]

percentage = FormatTemplate.percentage(2)
table_columns = [{"name": i, "id": i} for i in df_stock_participants_hist_sample.columns]
for col in table_columns:
    if col['id'] == 'sharepercent':
        col['type'] = 'numeric'
        col['format'] = percentage
    if col['id'] == 'sharenumber':
        col['type'] = 'numeric'
        col['format'] = Format().group(True)
print(table_columns)

app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

app.layout = html.Div(children=[
    html.H1(
        children='CCASS Analysis',
        style={'textAlign': 'center'}
    ),
    html.Div(children='''
        Dash: A web application framework for your data.
    ''', style={'textAlign': 'center'}),
    html.Div([
        html.Div([
            html.Label('stock dropdown'),
            dcc.Dropdown(stock_list, id='stock_code-dropdown'),
            html.Div(id='stock_code-container')
        ], style={'padding': 10, 'flex': 1, 'width': '50vh'}),
        html.Div([
            html.Label('select date range'),
            dcc.DatePickerRange(
                id='stock-date-picker-range',
                min_date_allowed=date(2021, 7, 1),
                max_date_allowed=date(2022, 7, 15),
                initial_visible_month=date(2022, 7, 15),
                end_date=date(2022, 7, 15)
            ),
            html.Div(id='output-container-date-picker-range')
        ], style={'padding': 10, 'flex': 1, 'width': '50vh'}),
        html.Div([
            html.Button(id="search-button", children="search"),
        ], style={'padding': 5, 'flex': 1}),
        html.Div([
            html.Label('search button'),
            dcc.Input(
                id='min-share-threshold', 
                type="number", placeholder="input %",
                min=1, max=100, step=1,
            )
        ], style={'padding': 10, 'flex': 1, 'width': '50vh'}),
    ], style={'textAlign': 'center'}, className = "center"),
    html.Div([
        dcc.Tabs(
            id="tabs-with-classes",
            value='tab-2',
            parent_className='custom-tabs',
            className='custom-tabs-container',
            children=[
                dcc.Tab(
                    label='Trend plot',
                    value='tab-1',
                    className='custom-tab',
                    selected_className='custom-tab--selected',
                    children=[
                        html.Div([
                            html.H4(children='CCASS line', className='center'),
                            dcc.Graph(id='shareholder-linechart'),
                        ], style={'padding': 10, 'flex': 1, 'width': '60vh'}, className='center'),
                        html.Div([
                            html.H4(children='CCASS Table', className='center'),
                            dbc.Container([
                                dbc.Label('Click a cell in the table:'),
                                dash_table.DataTable(
                                    df_stock_participants_hist_sample.to_dict('records'),
                                    table_columns, 
                                    id='tbl',
                                    filter_action="native",
                                    sort_action="native",
                                    sort_mode="multi",
                                    export_format="csv"),
                                dbc.Alert(id='tbl_out'),
                            ])
                        ], style={'padding': 10, 'flex': 1, 'width': '90h'}, className='center')
                    ]
                ),
                dcc.Tab(
                    label='Transaction finder',
                    value='tab-2',
                    className='custom-tab',
                    selected_className='custom-tab--selected',
                    children=[
                        html.Div([
                            html.Label('As of date'),
                            dcc.DatePickerSingle(id='asofdate-picker', date=date(2022, 7, 15)),
                        ], style={'padding': 10, 'flex': 1, 'width': '60vh'}, className='center'),
                        html.Div([
                            html.H4(children='CCASS holder changes', className='center'),
                            dcc.Graph(id='shareholder-butterflychart'),
                        ], style={'padding': 10, 'flex': 1, 'width': '60vh'}, className='center'),
                    ]
                )
            ]),
        html.Div(id='tabs-content-classes')
    ]),
])

@app.callback(
    Output('stock_code-container', 'children'),
    Input('stock_code-dropdown', 'value')
)
def update_output(value):
    return f'You have selected {value}'

@callback(
    Output('stock-date-picker-range', 'start_date'), 
    Output('stock-date-picker-range', 'end_date'), 
    Output('stock-date-picker-range', 'min_date_allowed'), 
    Output('stock-date-picker-range', 'max_date_allowed'), 
    Input('stock_code-dropdown', 'value'),
    prevent_initial_call=True
)
def update_calendar(value):
    if value:
        df = get_participants_hist_by_stock(value)
        #df = df_stock_participants_hist[df_stock_participants_hist.stock_code == value]
        min_date, max_date = min(df['business_date']), max(df['business_date'])
        return min_date, max_date, min_date, max_date
    else:
        return app_config['config']['min_date'], app_config['config']['max_date'], app_config['config']['min_date'], app_config['config']['max_date']

@app.long_callback(
    Output('tbl', 'data'),
    Input('stock_code-dropdown', 'value'),
    Input('stock-date-picker-range', 'start_date'), 
    Input('stock-date-picker-range', 'end_date'), 
    Input('search-button', 'n_clicks'),
    running=[
        (Output("search-button", "disabled"), True, False),
    ],
)
def click_search(value, start_date, end_date, button_clicks):
    button_id = ctx.triggered_id if not None else 'No clicks yet'
    if button_id == 'search-button':
        print(f'You have selected {value}')
        df = df_stock_top10_participants[df_stock_top10_participants.stock_code == value]
        top10_lst = list(df[df.business_date == end_date]['participant_id'])
        print('top10_lst')
        print(top10_lst)
        df_stock_participants_hist = get_participants_hist_by_stock(value)
        df_sub = df_stock_participants_hist[(df_stock_participants_hist.business_date >= start_date) & (df_stock_participants_hist.business_date <= end_date) & (df_stock_participants_hist.participant_id.isin(top10_lst))]
        return df_sub.to_dict('records')

# @app.callback(
#     Output('tbl', 'data'),
#     Input('stock_code-dropdown', 'value'),
#     Input('stock-date-picker-range', 'start_date'), 
#     Input('stock-date-picker-range', 'end_date'), 
# )
# def update_table(value, start_date, end_date):
#     print(f'You have selected {value}')
#     df = df_stock_top10_participants[df_stock_top10_participants.stock_code == value]
#     top10_lst = list(df[df.business_date == end_date]['participant_id'])
#     print('top10_lst')
#     print(top10_lst)
#     df_stock_participants_hist = get_participants_hist_by_stock(value)
#     df_sub = df_stock_participants_hist[(df_stock_participants_hist.business_date >= start_date) & (df_stock_participants_hist.business_date <= end_date) & (df_stock_participants_hist.participant_id.isin(top10_lst))]
#     return df_sub.to_dict('records')

@callback(Output('tbl_out', 'children'), Input('tbl', 'active_cell'))
def update_graphs(active_cell):
    return str(active_cell) if active_cell else "Click the table"

@callback(
    Output('asofdate-picker', 'min_date_allowed'), 
    Output('asofdate-picker', 'max_date_allowed'), 
    Input('stock-date-picker-range', 'min_date_allowed'), 
    Input('stock-date-picker-range', 'max_date_allowed'), 
    prevent_initial_call=True
)
def update_asofdatepicker(min_date_allowed, max_date_allowed):
    return min_date_allowed, max_date_allowed


@callback(
    Output('shareholder-linechart', 'figure'), 
    Input('tbl', 'data'),
    Input('stock_code-dropdown', 'value'),
    Input('stock-date-picker-range', 'start_date'), 
    Input('stock-date-picker-range', 'end_date'), 
    prevent_initial_call=True
)
def update_shareholder_linechart(df_dict, dropdown_value, start_date, end_date):
    if dropdown_value:
        df = pd.DataFrame(df_dict)
        print('before filtering, line chart dataframe is:')
        print(df.head(5))
    else:
        df = pd.DataFrame({})
    fig = px.line(df, x= 'business_date', y= 'sharepercent', color='participant_id')
    #fig.update_traces(mode='lines+markers')
    #fig.update_xaxes(showgrid=False)
    #fig.update_yaxes(type='linear')
    #fig.update_layout(height=225, margin={'l': 20, 'b': 30, 'r': 10, 't': 10})
    #print(fig)
    return fig


# @callback(
#     Output('shareholder-butterflychart', 'figure'), 
#     Input('tbl', 'data'),
#     Input('stock_code-dropdown', 'value'),
#     Input('asofdate-picker', 'date'), 
#     Input('min-share-threshold', 'value'), 
#     prevent_initial_call=True
# )
# def update_shareholder_butterflychart(df_dict, dropdown_value, asofdate, share_threshold):
#     df = pd.DataFrame(df_dict)
#     df = df[df['stock_code'] == dropdown_value]
#     df["business_date"] = pd.to_datetime(df["business_date"])
#     df['business_date'] = df['business_date'].dt.strftime('%Y-%m-%d')
#     print(f'share threshold is {share_threshold}')
#     share_threshold = int(share_threshold)/100
#     df_positive = df[(df['business_date'] == asofdate) & (df['sharepercent']>=share_threshold*2)]
#     df_negative = df[(df['business_date'] == asofdate) & (df['sharepercent']>=share_threshold)]

#     fig = make_subplots(rows=1, cols=2, specs=[[{}, {}]], shared_xaxes=False,
#                     shared_yaxes=True, horizontal_spacing=0)
#     fig.add_trace(go.Bar(
#         x=df_positive['participant_id'],
#         y=df_positive['sharepercent'], 
#         text=df_positive["participant_id"],
#         orientation='h'), 
#         1, 1)
#     fig.add_trace(go.Bar(
#         x=df_negative['participant_id'],
#         y=df_negative['sharepercent'], 
#         text=df_negative["participant_id"],
#         orientation='h'), 
#         1, 2)
#     return fig

@callback(
    Output('shareholder-butterflychart', 'figure'), 
    Input('tbl', 'data'),
    Input('stock_code-dropdown', 'value'),
    Input('asofdate-picker', 'date'), 
    Input('min-share-threshold', 'value'), 
    prevent_initial_call=True
)
def update_shareholder_butterflychart_test(df_dict, dropdown_value, asofdate, share_threshold):
    df = pd.read_csv('data/test.csv')

    fig = make_subplots(rows=1, cols=2, specs=[[{}, {}]], shared_xaxes=False,
                        shared_yaxes=True, horizontal_spacing=0)

    fig.append_trace(go.Bar(x=df['Male'],
                        y=df['Industry'], 
                        text=df["Male"].map('{:,.0f}'.format), #Display the numbers with thousands separators in hover-over tooltip 
                        textposition='inside',
                        orientation='h', 
                        width=0.7, 
                        showlegend=False, 
                        marker_color='#4472c4'), 
                        1, 1) # 1,1 represents row 1 column 1 in the plot grid

    fig.append_trace(go.Bar(x=df['Female'],
                        y=df['Industry'], 
                        text=df["Female"].map('{:,.0f}'.format),
                        textposition='inside',
                        orientation='h', 
                        width=0.7, 
                        showlegend=False, 
                        marker_color='#ed7d31'), 
                        1, 2) # 1,2 represents row 1 column 2 in the plot grid
    fig.update_xaxes(showticklabels=False,title_text="Male", row=1, col=1, range=[16000000,0])
    fig.update_xaxes(showticklabels=False,title_text="Female", row=1, col=2)

    fig.update_layout(title_text="Industry Employment Counts by Gender (Q1 2021)", 
                    width=800, 
                    height=700,
                    title_x=0.9,
                    xaxis1={'side': 'top'},
                    xaxis2={'side': 'top'},)
    return fig

if __name__ == "__main__":
    app.run_server(debug=True)