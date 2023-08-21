#!/usr/bin/env python
# coding: utf-8

# In[3]:


get_ipython().system(' pip install GitPython')


# In[4]:


import os
os.environ['GIT_PYTHON_GIT_EXECUTABLE']= r'C:\Program Files\Git\cmd\git.exe'
from git import repo


# In[5]:


import csv
import subprocess
import pandas as pd
import requests
import git
import pandas as pd
import json
from git.repo.base import Repo


# In[6]:


os.system("git clone https://github.com/PhonePe/pulse.git")


# In[7]:


root_dir = (r'C:\Project on Phonepe Pulse\pulse\data')
data_list = []
for state_dir in os.listdir(os.path.join(root_dir,r'C:\Project on Phonepe Pulse\pulse\data\aggregated\transaction\country\india\state')):
    state_path = os.path.join(root_dir,r'C:\Project on Phonepe Pulse\pulse\data\aggregated\transaction\country\india\state', state_dir)
    if os.path.isdir(state_path):
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)
                            for transaction_data in data['data']['transactionData']:
                                row_dict = {'states': state_dir,
                                            'Transaction_Year': year_dir,
                                            'Quarters': int(json_file.split('.')[0]),
                                            'Transaction_Type': transaction_data['paymentInstruments'][0]['count'],
                                            'Transaction_Amount': transaction_data['paymentInstruments'][0]['amount']
                                           }
                                data_list.append(row_dict)


# In[8]:


df1 = pd.DataFrame(data_list)
df1


# In[9]:


root_dir = r'C:\Project on Phonepe Pulse\pulse\data\aggregated\user\country\india\state'
df2_list = []
for state_dir in os.listdir(root_dir):
    state_path = os.path.join(root_dir, state_dir)
    if os.path.isdir(state_path):
        for json_file in os.listdir(state_path):
            if json_file.endswith('.json'):
                with open(os.path.join(state_path, json_file), 'r') as f:
                    json_data = join.load(f)
                    if isinstance(json_data, list):
                        df2_list += json_data
                    else:
                        df2_list.append(json_data)
        if df2_list:
            df2 = pd.json_normalize(df2_list)
            df2['subfolder'] = state_dir
            df2['subsubfolder'] = 'state'
df2 = pd.DataFrame(data_list)


# In[10]:


df2


# In[11]:


root_dir = r'C:\Project on Phonepe Pulse\pulse\data'
data_list = []
for state_dir in os.listdir(os.path.join(root_dir,r'C:\Project on Phonepe Pulse\pulse\data\map\transaction\hover\country\india\state')):
    state_path = os.path.join(root_dir,r'C:\Project on Phonepe Pulse\pulse\data\map\transaction\hover\country\india\state', state_dir)
    if os.path.isdir(state_path):
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)
                            for hoverDataList in data['data']['hoverDataList']:
                                row_dict = {
                                    'States': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarters': int(json_file.split('.')[0]),
                                    'District': hoverDataList['name'],
                                    'Transaction_Type': hoverDataList['metric'][0]['type'],
                                    'Transaction_Count': hoverDataList['metric'][0]['amount']
                                }
                                data_list.append(row_dict)


# In[12]:


df3 = pd.DataFrame(data_list)
df3


# In[13]:


root_dir = r"C:\Project on Phonepe Pulse\pulse\data\map\user\hover\country\india\state"
data_list = []
for state_dir in os.listdir(root_dir):
    state_path = os.path.join(root_dir, state_dir)
    if os.path.isdir(state_path):
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open (os.path.join(year_path, json_file)) as f:
                            data = json.load(f)
                            for district, values in data['data']['hoverData'].items():
                                row_dict = {
                                    'states': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarter': int(json_file.split('.')[0]),
                                    'District': district,
                                    'RegisteredUsers': values['registeredUsers'],
                                }
                                data_list.append(row_dict)


# In[14]:


df4 = pd.DataFrame(data_list)
df4


# In[15]:


import os
os.environ['GIT_PYTHON_GIT_EXECUTABLE']= r'C:\Program Files\Git\cmd\git.exe'
from git import repo
import json
import pandas as pd
root_dir = r'C:\Project on Phonepe Pulse\pulse\data'
data_list = []
for state_dir in os.listdir(os.path.join(root_dir,r'C:\Project on Phonepe Pulse\pulse\data\top\transaction\country\india\state')):
    state_path = os.path.join(root_dir,r'C:\Project on Phonepe Pulse\pulse\data\top\transaction\country\india\state', state_dir)
    if os.path.isdir(state_path):
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)
                            for districts in data['data']['districts']:
                                row_dict = {
                                    'States': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarters': int(json_file.split('.')[0]),
                                    'District': districts['entityName'],
                                    'Transaction_Type': districts['metric']['type'],
                                    'Transaction_Count': districts['metric']['count'],
                                    'Transaction_Amount': districts['metric']['amount']
                                }
                                data_list.append(row_dict)


# In[16]:


df5 = pd.DataFrame(data_list)
df5


# In[17]:


import os
os.environ['GIT_PYTHON_GIT_EXECUTABLE']= r'C:\Program Files\Git\cmd\git.exe'
from git import repo
import json
import pandas as pd
root_dir = r'C:\Project on Phonepe Pulse\pulse\data\top\user\country\india\state'
data_list = []
for state_dir in os.listdir(root_dir):
    state_path = os.path.join(root_dir, state_dir)
    if os.path.isdir(state_path):
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)
                            for district in data['data']['districts']:
                                row_dict = {
                                    'state': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarters': int(json_file.split('.')[0]),
                                    'District': district['name'] if 'name' in district else district['pincode'],
                                    'RegisteredUsers': district['registeredUsers'],
                                }
                                data_list.append(row_dict)


# In[18]:


df6 = pd.DataFrame(data_list)
df6


# In[19]:


d1 = df1.drop_duplicates()
d2 = df2.drop_duplicates()
d3 = df3.drop_duplicates()
d4 = df4.drop_duplicates()
d5 = df5.drop_duplicates()
d6 = df6.drop_duplicates()


# In[20]:


null_counts = d1.isnull().sum()
print(null_counts)


# In[21]:


null_counts = d2.isnull().sum()
print(null_counts)


# In[22]:


null_counts = d3.isnull().sum()
print(null_counts)


# In[23]:


null_counts = d4.isnull().sum()
print(null_counts)


# In[24]:


null_counts = d5.isnull().sum()
print(null_counts)


# In[25]:


null_counts = d6.isnull().sum()
print(null_counts)


# In[26]:


df1.to_csv('agg_trans.csv', index=False)
df2.to_csv('agg_user.csv', index=False)
df3.to_csv('map_tran.csv', index=False)
df4.to_csv('map_user.csv', index=False)
df5.to_csv('top_tran.csv', index=False)
df6.to_csv('top_user.csv', index=False)


#     #Connecting to SQL

# In[27]:


import mysql.connector as sql
import streamlit as st
mydb = sql.connect(host='localhost',
                  user='root',
                  password='9500',
                  database='gokhil_ds_mysql'
                  )
mycursor = mydb.cursor(buffered=True)


# In[28]:


import mysql.connector as sql
import pandas as pd
mycursor.execute("create table agg_trans (States varchar(50), Transaction_Year varchar(50), Quarters int, Transaction_Type varchar(50), Transaction_Amount double)")
for i,row in df1.iterrows():
    sql = "INSERT INTO agg_trans VALUES (%$,%$,%$,%$,%$,%$)"
    mycursor.execute(sql,tuple(row))
    mydb.commit()


# In[ ]:


mycursor.execute("create table agg_user (States varchar(50), Transaction_Year varchar(50), Quarters int, District varchar(50), RegisteredUsers varchar(50))")
for i,row in df2.iterrows():
    sql = "INSERT INTO agg_user VALUES (%$,%$,%$,%$,%$,%$)"
    mycursor.execute(sql,tuple(row))
    mydb.commit()


# In[ ]:


mycursor.execute("create table map_tran (States varchar(50), Transaction_Year varchar(50), Quarters int, District varchar(50), Transaction_Type varchar(50), Transaction_Count varchar(50))")
for i,row in df3.iterrows():
    sql = "INSERT INTO map_tran VALUES (%$,%$,%$,%$,%$,%$)"
    mycursor.execute(sql,tuple(row))
    mydb.commit()


# In[ ]:


mycursor.execute("create table map_user (States varchar(50), Transaction_Year varchar(50), Quarters int, District varchar(50), RegisteredUsers varchar(50))")
for i,row in df4.iterrows():
    sql = "INSERT INTO map_user VALUES (%$,%$,%$,%$,%$,%$)"
    mycursor.execute(sql,tuple(row))
    mydb.commit()


# In[ ]:


mycursor.execute("create table top_tran (States varchar(50), Transaction_Year varchar(50), Quarters int, District varchar(50), Transaction_Type varchar(50), Transaction_Count varchar(50), Transaction_Amount double)")
for i,row in df5.iterrows():
    sql = "INSERT INTO top_tran VALUES (%$,%$,%$,%$,%$,%$)"
    mycursor.execute(sql,tuple(row))
    mydb.commit()


# In[29]:


mycursor.execute("create table top_user (States varchar(50), Transaction_Year varchar(50), Quarters int, District varchar(50), RegisteredUsers varchar(50))")
for i,row in df6.iterrows():
    sql = "INSERT INTO top_user VALUES (%$,%$,%$,%$,%$,%$)"
    mycursor.execute(sql,tuple(row))
    mydb.commit()


# In[30]:


mycursor.execute("show tables")
mycursor.fetchall()


# In[31]:


import pandas as pd
import mysql.connector as sql
import streamlit as st
import plotly.express as px
import os
import json
from streamlit_option_menu import option_menu
from PIL import Image
from git.repo.base import Repo


# In[32]:


st.set_page_config(page_title = "Phonepe Pulse Data Visualization and Exploration | By Gokhil S",
                   layout = "wide",
                   initial_sidebar_state = "expanded",
                   menu_items = {'About': """"# This dashboard app is created by *Gokhil S*!
                                                              Data has been cloned from Phonepe Pulse Github Repo"""})
st.sidebar.header(":wave: :violet[**Hi Buddy! Welcome to the dashboard**]")


# In[33]:


Repo.clone_from("https://github.com/PhonePe/pulse.git", "C:\Project on Phonepe Pulse\pulse\data")


# In[ ]:


mydb = sql.connect(host="localhost",
                   user="root",
                   password="9500",
                   database= "gokhil_ds_mysql"
                  )
mycursor = mydb.cursor(buffered=True)


# In[34]:


with st.sidebar:
    selected = option_menu("Menu", ["Home", "Top Charts", "Explore Data", "About"],
                          icons = ["house", "graph-up-arrow", "bar-chart-line", "exclamation-circle"],
                          menu_icon = "menu-button-wide",
                          default_index = 0,
                          styles = {"nav-link": {"font-size": "20px", "text-align": "left", "margin": "-2px", "--hover-color": "#6F36AD"},
                        "nav-link-selected": {"background-color": "#6F36AD"}})


# In[35]:


if selected == "Home":
    st.markdown("# :violet[Data Visualization and Exploration]")
    st.markdown("# :violet[A User-Friendly Tool Using Streamlit and Plotly]")
    col1, col2 = st.columns([3,2], gap="medium")
    with col1:
        st.write(" ")
        st.write(" ")
        st.markdown("### :White[Domain :] Data Science")
        st.markdown("### :White[Technologies used :] Github cloning, Python, Pandas, MySQL, mysql-connector-python, Streamlit, and Plotly.")


# In[44]:


if selected == "Top Charts":
    st.markdown("## :White[Top Charts]")
    Type = st.sidebar.selectbox("**Type**", ("Transactions", "Users"))
    column1, column2= st.columns([1,1.5],gap="large")
    with column1:
        Year = st.slider("**Year**", min_value=2018, max_value=2022)
        Quarter = st.slider("Quarter", min_value=1, max_value=4)
    with column2:
        st.info(
                """
                #### From this menu we can get insights like :
                - Overall ranking on a particular Year and Quarter.
                - Top 10 State, District, Pincode based on Total number of transaction and Total amount spent on phonepe.
                - Top 10 State, District, Pincode based on Total phonepe users and their app opening frequency.
                - Top 10 mobile brands and its percentage based on the how many people use phonepe.
                """,icon="🔍"
                )
        if Type == "Transactions":
            col1, col2, col3 = st.columns([1,1,1], gap="small")
            with col1:
                st.markdown("### :White[State]")
                mycursor.execute(f"select state, sum(Transaction_Count) as Total_Transactions_Count, sum(Transaction_Amount) as Total from agg_trans where year = {Year} and quarter = {Quarter} group by state order by Total desc limit 10")
                df = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Transactions_Count','Total_Amount'])
                fig = px.pie(df, values='Total_Amount',
                             names='State',
                             title='Top 10',
                             color_discrete_sequence=px.colors.sequential.Agsunset,
                             hover_data=['Transactions_Count'],
                             labels={'Transactions_Count':'Transactions_Count'})
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig,use_container_width=True)
                with col2:
                    st.markdown("### :White[District]")
                    mycursor.execute(f"select district, sum(count) as Total_Count, sum(Amount) as Total from map_trans where year = {year} and quarter = {Quarter} group by district order by Total desc limit 10")
                    df1 = pd.DataFrame(mycursor.fetchall(), columns = ['State', 'Total_Transactions', 'Total_amount'])
                    fig = px.pie(df, values='Total_Amount',
                                 names='District',
                                 title='Top 10',
                                 color_discrete_sequence=px.colors.sequential.Agsunset,
                                 hover_data=['Transactions_Count'],
                                 labels={'Transactions_Count':'Transactions_Count'})
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig,use_container_width=True)
                with col3:
                    st.markdown("### :violet[Pincode]")
                    mycursor.execute(f"select pincode, sum(Transaction_Count) as Total_Transactions_Count, sum(Transaction_Amount) as Total from top_trans where year = {Year} and quarter = {Quarter} group by pincode order by Total desc limit 10")
                    df = pd.DataFrame(mycursor.fetchall(), columns=['Pincode', 'Transactions_Count','Total_Amount'])
                    fig = px.pie(df, values='Total_Amount',
                                 names='Pincode',
                                 title='Top 10',
                                 color_discrete_sequence=px.colors.sequential.Agsunset,
                                 hover_data=['Transactions_Count'],
                                 labels={'Transactions_Count':'Transactions_Count'})
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig,use_container_width=True)


# In[54]:


Type = "Users"
if Type == "Users":
    col1,col2,col3,col4 = st.columns([2,2,2,2],gap="small")
    with col1:
        st.markdown("### :violet[Brands]")
        Year = 2022
        Quarter = 4
        if Year == 2022 and Quarter in [2,3,4]:
            st.markdown("#### Sorry No Data to Display for 2022 Qtr 2,3,4")
        else:
            mycursor.execute(f"select brands, sum(count) as Total_Count, avg(percentage)*100 as Avg_Percentage from agg_user where year = {Year} and quarter = {Quarter} group by brands order by Total_Count desc limit 10")
            df = pd.DataFrame(mycursor.fetchall(), columns=['Brand', 'Total_Users','Avg_Percentage'])
            fig = px.bar(df,title='Top 10',
                         x="Total_Users",
                         y="Brand",
                         orientation='h',
                         color='Avg_Percentage',
                         color_continuous_scale=px.colors.sequential.Agsunset)
            st.plotly_chart(fig,use_container_width=True)
            with col2:
                st.markdown("### :violet[District]")
                mycursor.execute(f"select district, sum(Registered_User) as Total_Users, sum(app_opens) as Total_Appopens from map_user where year = {Year} and quarter = {Quarter} group by district order by Total_Users desc limit 10")
                df = pd.DataFrame(mycursor.fetchall(), columns=['District', 'Total_Users','Total_Appopens'])
                df.Total_Users = df.Total_Users.astype(float)
                fig = px.bar(df,
                             title='Top 10',
                             x="Total_Users",
                             y="District",
                             orientation='h',
                             color='Total_Users',
                             color_continuous_scale=px.colors.sequential.Agsunset)
                st.plotly_chart(fig,use_container_width=True)
                with col3:
                    st.markdown("### :violet[Pincode]")
                    mycursor.execute(f"select Pincode, sum(Registered_Users) as Total_Users from top_user where year = {Year} and quarter = {Quarter} group by Pincode order by Total_Users desc limit 10")
                    df = pd.DataFrame(mycursor.fetchall(), columns=['Pincode', 'Total_Users'])
                    fig = px.pie(df,
                                 values='Total_Users',
                                 names='Pincode',
                                 title='Top 10',
                                 color_discrete_sequence=px.colors.sequential.Agsunset,
                                 hover_data=['Total_Users'])
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig,use_container_width=True)
                    with col4:
                        st.markdown("### :violet[State]")
                        mycursor.execute(f"select state, sum(Registered_user) as Total_Users, sum(App_opens) as Total_Appopens from map_user where year = {Year} and quarter = {Quarter} group by state order by Total_Users desc limit 10")
                        df = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Total_Users','Total_Appopens'])
                        fig = px.pie(df, values='Total_Users',
                                     names='states',
                                     title='Top 10',
                                     color_discrete_sequence=px.colors.sequential.Agsunset,
                                     hover_data=['Total_Appopens'],
                                     labels={'Total_Appopens':'Total_Appopens'})
                        fig.update_traces(textposition='inside', textinfo='percent+label')
                        st.plotly_chart(fig,use_container_width=True)


# In[65]:


Year = st.sidebar.slider("**Year**", min_value=2018, max_value=2022)
Quarter = st.sidebar.slider("Quarter", min_value=1, max_value=4)
if selected == "Explore Data":
    Year = st.sidebar.slider("**Year**", min_value=2018, max_value=2022)
    Quarter = st.sidebar.slider("Quarter", min_value=1, max_value=4)
    Type = st.sidebar.selectbox("**Type**", ("Transactions", "Users"))
    col1,col2 = st.columns(2)
    if Type == "Transactions":
        with col1:
            st.markdown("## :violet[Overall State Data - Transactions Amount]")
            mycursor.execute(f"select state, sum(count) as Total_Transactions, sum(amount) as Total_amount from map_trans where year = {Year} and quarter = {Quarter} group by state order by state")
            df1 = pd.DataFrame(mycursor.fetchall(),columns= ['State', 'Total_Transactions', 'Total_Amount'])
            df2 = pd.read_csv('Statenames.csv')
            df1.State = df2
            fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                                featureidkey='properties.ST_NM',
                                locations='states',
                                color='Total_Amount',
                                color_continuous_scale='sunset')
            fig.update_geos(fitbounds="locations", visible=False)
            st.plotly_chart(fig,use_container_width=True)
        with col2:
            st.markdown("## :violet[Overall State Data - Transactions Count]")
            mycursor.execute(f"select state, sum(count) as Total_Transactions, sum(amount) as Total_amount from map_trans where year = {Year} and quarter = {Quarter} group by state order by state")
            df1 = pd.DataFrame(mycursor.fetchall(),columns= ['State', 'Total_Transactions', 'Total_amount'])
            df2 = pd.read_csv('Statenames.csv')
            df1.Total_Transactions = df1.Total_Transactions.astype(int)
            df1.State = df2
            fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                                featureidkey='properties.ST_NM',
                                locations='states',
                                color='Total_Transactions',
                                color_continuous_scale='sunset')
            fig.update_geos(fitbounds="locations", visible=False)
            st.plotly_chart(fig,use_container_width=True)


# In[79]:


if Type == "Users":
    st.markdown("## :violet[Overall State Data - User App opening frequency]")
    mycursor.execute(f"select States, sum(RegisteredUsers) as Total_Users from map_user where Transaction_Year = {Year} and Quarters = {Quarter} group by States order by States")
    df1 = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Total_Users','Total_Appopens'])
    df2 = pd.read_csv('Statenames.csv')
    df1.Total_Appopens = df1.Total_Appopens.astype(float)
    df1.State = df2
    fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey='properties.ST_NM',
                        locations='State',
                        color='Total_Appopens',
                        color_continuous_scale='sunset')
    fig.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig,use_container_width=True)


# In[84]:


st.markdown("## :violet[Select any State to explore more]")
selected_state = st.selectbox("",
                              ('andaman-&-nicobar-islands','andhra-pradesh','arunachal-pradesh','assam','bihar',
                               'chandigarh','chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu','delhi','goa','gujarat','haryana',
                               'himachal-pradesh','jammu-&-kashmir','jharkhand','karnataka','kerala','ladakh','lakshadweep',
                               'madhya-pradesh','maharashtra','manipur','meghalaya','mizoram',
                               'nagaland','odisha','puducherry','punjab','rajasthan','sikkim',
                               'tamil-nadu','telangana','tripura','uttar-pradesh','uttarakhand','west-bengal'),index=30)
mycursor.execute(f"select States,Transaction_Year,Quarters,District,sum(RegisteredUsers) as Total_Users from map_user where Transaction_Year = {Year} and Quarters = {Quarter} and States = '{selected_state}' group by States, District, Transaction_Year, Quarters order by States,District")
df = pd.DataFrame(mycursor.fetchall(), columns=['State','year', 'quarter', 'District', 'Total_Users','Total_Appopens'])
df.Total_Users = df.Total_Users.astype(int)
fig = px.bar(df,
                     title=selected_state,
                     x="District",
                     y="Total_Users",
                     orientation='v',
                     color='Total_Users',
                     color_continuous_scale=px.colors.sequential.Agsunset)
st.plotly_chart(fig,use_container_width=True)


# In[86]:


if selected == "About":
    col1,col2 = st.columns([3,3],gap="medium")
    with col1:
        st.write(" ")
        st.write(" ")
        st.markdown("### :violet[About PhonePe Pulse:] ")
    with col2:
        st.write(" ")
        st.write(" ")
        st.write(" ")
        st.write(" ")


# In[ ]:




