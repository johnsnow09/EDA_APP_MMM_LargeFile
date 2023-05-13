import streamlit as st
import polars as pl
import pandas as pd
import datetime as dt
import plotly.express as px


# from: https://youtu.be/lWxN-n6L7Zc
# StreamlitAPIException: set_page_config() can only be called once per app, and must be called as the first Streamlit command in your script.



############### Setting Configuration ###############

st.set_page_config(page_title="EDA of Ekart Data",
                    layout='wide', # centered
                    initial_sidebar_state="collapsed")


# Setting configuration to diable plotly zoom in plots
config = dict({'scrollZoom': False})



st.markdown("""<style>.big-font {
font-size:38px !important;}
</style>
""", unsafe_allow_html=True)

############### Setting Configuration Ends ###############





############################## GET DATA ##############################
# @st.cache
# @st.cache_data

# this is to read main data file
@st.cache_resource
def get_data():
    df = pl.scan_parquet('df_consumer_v3.parquet')
    
    return df

df_consumer = get_data()

############################## DATA DONE ##############################




############### Custom Functions ###############

# from: https://discuss.streamlit.io/t/how-to-add-extra-lines-space/2220/7
def v_spacer(height, sb=False) -> None:
    for _ in range(height):
        if sb:
            st.sidebar.write('\n')
        else:
            st.write('\n')


# Total Orders Function
@st.cache_data
def Calc_total_orders(groupof,count_of):
    return (df_consumer.groupby(groupof).agg( 
                total_orders = pl.count(count_of)
            ).sort(groupof).collect()
            )

# Total GMV Function
@st.cache_data
def Calc_total_gmv(groupof,sum_of):
    return (df_consumer.groupby(groupof).agg(
                total_gmv = pl.sum(sum_of)
            ).sort(groupof).collect()
            )

@st.cache_data
def Calc_countof_group(groupof):
    return (df_consumer.groupby(groupof).count().collect()
            )

@st.cache_data
def Calc_percof_group(groupof):
    return (df_consumer.groupby(groupof).count().with_columns(
                total_orders = pl.sum('count').over('Year') 
            ).with_columns(
                perc_sales = (pl.col('count') / pl.col('total_orders') * 100 ).round(2)
            ).collect()
                    )


# Since cust_id is not available in slim data so below code will give error if slim data is used
@st.cache_data()
def get_Repeated_customer_df():

    customer_2015_list = df_consumer.filter(pl.col('Year') == 2015).select('cust_id').unique().collect().to_series().to_list()

    return (df_consumer.select(pl.col(['Year','cust_id','order_payment_type'])).filter(pl.col('Year') > 2015).with_columns(
            Repeat_cust_over_Year = pl.when(pl.col('cust_id').is_in(customer_2015_list)
                                            ).then('Customer_Repeat_from_2015').otherwise('New_Customer')
                                            ).groupby(['Repeat_cust_over_Year','order_payment_type']).count().with_columns(
                                                total_count = pl.sum('count').over(pl.col('order_payment_type'))
                                            ).with_columns(
                                                perc_count = (pl.col('count') / pl.col('total_count') * 100 ).round(2)
                            ).collect()
)


############### Custom Functions Ends ###############



############################## CREATING HEADER ##############################

header_left,header_mid,header_right = st.columns([1,8,1],gap = "small")

with header_mid:
    # https://docs.streamlit.io/library/get-started/create-an-app
    st.title("Exploratary Data Analysis of Ecommerce ElecKart Consumer Electronics (WIP)")

############################## HEADER DONE ##############################




############################## TOP CONTENT ##############################
    # v_spacer(1)

    st.write(f"(App Work Under Progress) This is a demo webapp for **Consumer Electronics** data which was 1st built in **R** and is now replicated using **Python & Polars** in streamlit.   \
             As every Modeling Process first needs good **EDA** (Exploratary Data Analysis) to get business insights so this App is focused on EDA before the **Market Mix Modelling**.    \
             Eleckart Data used in this is from kaggle -  https://www.kaggle.com/datasets/ashydv/consumer-electronics-data or https://drive.google.com/drive/folders/1F5O1thcC43zAPxsWIw1WQMh58Qi5I5CB. \
               \n\n Note: - Lot of Data Pre processing & Cleaning has been done before creating the Plots. It has 2 years of data and {df_consumer.collect().height} number of Rows after Data Cleaning." )
############################## TOP CONTENT END ##############################




############################## FILTERED DATA ##############################

# df_selected = df.filter((pl.col('State') == State_Selected) &
#                     (pl.col('Year').is_in(Year_Selected))
#                     )
############################## FILTERED DATA DONE ##############################    


############################## USEFUL AGGREGATIONS & LIST ##############################


############################## USEFUL AGGREGATIONS & LIST DONE ##############################


# st.write(df_consumer.dtypes)



# col_01,col_02,col_03 = st.columns([4,4,2],gap = "small")
# with col_02:
#     st.markdown('<p class="big-font">Data</p>', unsafe_allow_html=True)



with st.expander("Click here to View the Demo of the Data"):

    tab11, tab21 = st.tabs(["🗃 Data Demo","📈 Stat Summary"])

    tab11.dataframe(df_consumer.head(10).collect().to_pandas())
    tab21.dataframe(df_consumer.collect().to_pandas().describe()) #head(10).collect().to_pandas()

v_spacer(4)    

col_11,col_12,col_13 = st.columns([4,4,2],gap = "small")
with col_12:
    st.markdown('<p class="big-font">About Orders</p>', unsafe_allow_html=True)




############################## FIRST PLOT ##############################
plt_box_1,plt_box_2 = st.columns([7,1],gap = "small")

@st.cache_data
def df_overall_orders(): 
    return (df_consumer.groupby(['date_only']).agg( 
                                            total_orders = pl.count('order_id')
                                        ).sort('date_only').collect().to_pandas())

@st.cache_data
def df_overall_gmv():
    return (df_consumer.groupby(['date_only']).agg( 
                                                total_gmv = pl.sum('gmv')
                                            ).sort('date_only').collect().to_pandas())

with plt_box_1:
    # if gmv_option1 == "No":
    tab12, tab22 = st.tabs(["Total Orders","Total GMV"])

    fig_overall_orders = px.line(df_overall_orders(),
                                x='date_only',y='total_orders', #line_group='total_orders',
                                # hover_name='Party',
                                labels={
                                        "date_only": "Order Date",
                                        "total_orders": "Total Orders"
                                    },
                            
                            title=f'<b>Overall Total Orders </b>'
    )


    tab12.plotly_chart(fig_overall_orders,use_container_width=True, config = config)

    fig_overall_gmv = px.line(df_overall_gmv(),
                                x='date_only',y='total_gmv', 
                                # hover_name='Party',
                                labels={
                                        "date_only": "Order Date",
                                        "total_gmv": "Total GMV"
                                    },
                            
                            title=f'<b>Overall Total GMV</b>'
    )

    tab22.plotly_chart(fig_overall_gmv,use_container_width=True, config = config)
    
with plt_box_2:
    # v_spacer(3)
    # gmv_option1 = st.radio("Total GMV instead of Total Orders?",options=['No','Yes'], horizontal=True, key=1)
    
    v_spacer(15)
    st.write(f"As we can see in the Plot there is only 2 years (2015,2016) of Data.")


# st.divider()
st.markdown("""---""") 

############################## FIRST PLOT DONE ##############################

plt_box_12,plt_box_22 = st.columns([1,1],gap = "small")

@st.cache_data
def df_monthly_orders():
    return (Calc_total_orders(groupof=['Year','Month'],count_of='order_id'
                                                            ).to_pandas())
@st.cache_data
def df_monthly_gmv():
    return (Calc_total_gmv(groupof=['Year','Month'],sum_of='gmv'
                                                        ).to_pandas())
@st.cache_data
def df_hr_total_orders():
    return (Calc_total_orders(groupof=['Hour'],count_of='order_id'
                                                            ).to_pandas())

@st.cache_data
def df_hr_total_orders_facet():
    return (Calc_total_orders(groupof=['Year','Hour'],count_of='order_id'
                                                            ).to_pandas())


with plt_box_12:
    
    # v_spacer(3)
    # gmv_option2 = st.radio("In below Plot - Use Total GMV instead of Total Orders?",options=['No','Yes'], horizontal=True, key=2)

    tab13, tab23 = st.tabs(["Total Orders","Total GMV"])

    # if gmv_option2 == "No":
    fig_monthly_overall_orders = px.line(df_monthly_orders(),
                                x='Month',y='total_orders', facet_col='Year',
                                # hover_name='Party',
                                labels={
                                        "date_only": "Order Date",
                                        "total_orders": "Total Orders"
                                    },
                            
                            title=f'<b>Overall Total Orders by Months</b>'
    )

    tab13.plotly_chart(fig_monthly_overall_orders,use_container_width=True, config = config)

    # else:
    fig_monthly_overall_gmv = px.line(df_monthly_gmv(),
                        x='Month',y='total_gmv', facet_col='Year',
                        # hover_name='Party',
                        labels={
                                "date_only": "Order Date",
                                "total_gmv": "Total GMV"
                            },
                    
                    title=f'<b>Overall Total GMV by Months</b>'
)

    tab23.plotly_chart(fig_monthly_overall_gmv,use_container_width=True, config = config)
    # v_spacer(2)
    st.write(f"In 2015 the Max value was in Month of October and after a small decline the values have been pretty much constant.")
    
    
with plt_box_22:
    
    # v_spacer(3)
    # gmv_option3 = st.radio("In below Plot - Split by Years?",options=['No','Yes'], horizontal=True, key=3)
    tab15, tab25 = st.tabs(["Orders by Hours","Orders by Hours Split by Years"])
    

    # if gmv_option3 == "No":
    fig_hourly_overall_orders = px.line(df_hr_total_orders(),
                                x='Hour',y='total_orders',
                                # hover_name='Party',
                                labels={
                                        "Hour": "Hour of Day",
                                        "total_orders": "Total Orders"
                                    },
                            
                            title=f'<b>Overall Total Orders by Hour of the Day</b>'
    )
    tab15.plotly_chart(fig_hourly_overall_orders,use_container_width=True, config = config)

    # else:
    fig_hourly_overall_orders_facet = px.line(df_hr_total_orders_facet(),
                        x='Hour',y='total_orders', facet_col='Year',
                        # hover_name='Party',
                        labels={
                                "Hour": "Hour of Day",
                                "total_gmv": "Total GMV"
                            },
                    
                    title=f'<b>Overall Total Orders by Hour of the Day</b>'
)

    tab25.plotly_chart(fig_hourly_overall_orders_facet,use_container_width=True, config = config)
    
    # v_spacer(2)
    st.write(f"As it can be seen in the Plot that the Orders decline in the early hours of the day and is min around 5 am  \
             and then pickups and reach max around 11am or 12.")

# st.divider()
st.markdown("""---""") 

##################################         ##########################






##################################         ##########################



col_31,col_32,col_33 = st.columns([3,4,2],gap = "small")
with col_32:
    st.markdown('<p class="big-font">By Payment Method Type</p>', unsafe_allow_html=True)

fig_payment_type = px.bar(
                            # Calc_countof_group(groupof=['Year','order_payment_type']).to_pandas(),
                            Calc_percof_group(groupof=['Year','order_payment_type']).to_pandas(),
                    y='count',x='order_payment_type', color='order_payment_type',facet_col= 'Year',
                    orientation='v',
                    category_orders={'Year':[2015,2016],
                                     'order_payment_type': ['COD','Prepaid']},
                    
                    labels={
                            "order_payment_type": "Payment Type",
                            "count": "Total Order Count"
                        },
                
                title=f'<b>Total Orders by Payment type</b>'
                
).update_layout(height = 550)


fig_payment_type_perc = px.bar(
                            Calc_percof_group(groupof=['Year','order_payment_type']).to_pandas(),
                    y='perc_sales',x='order_payment_type', color='order_payment_type',facet_col= 'Year',
                    orientation='v',
                    category_orders={'Year':[2015,2016],
                                     'order_payment_type': ['COD','Prepaid']},
                    
                    labels={
                            "order_payment_type": "Payment Type (%)",
                            'perc_sales': '% Orders'
                        },
                
                title=f'<b>Percntage Orders by Payment type</b>'
                
).update_layout(height = 550)

col_41,col_42 = st.columns([1,1],gap = "small")

with col_41:
    st.plotly_chart(fig_payment_type,use_container_width=True, config = config)

with col_42:
    st.plotly_chart(fig_payment_type_perc,use_container_width=True, config = config)

# st.dataframe(Calc_percof_group(groupof=['Year','order_payment_type']).to_pandas())

st.write("As we can see in Above Plots - Not only the Number of Orders have increased in 2016 from 2015 in both Prepaid & COD orders  \
         but also the Percentage of Prepaid Orders have increased and COD have decreased which shows the growing confidence of \
         customers in the organization.")
            

#################################           ##########################







################################## REPEAT / NEW CUSTOMER        ##########################


col_51,col_52,col_53 = st.columns([3,8,2],gap = "small")
with col_52:
    st.markdown('<p class="big-font">Repeat/New Customer Payment Method</p>', unsafe_allow_html=True)

fig_payment_type_repeat_cust = px.bar(get_Repeated_customer_df().to_pandas(),
                    y='count',x='Repeat_cust_over_Year', color='order_payment_type',
                    orientation='v',
                    category_orders={'Repeat_cust_over_Year': ['Customer_Repeat_2015','New_Customer']},
                    
                    # labels={
                    #         "Repeat_cust_over_Year": "Repeat Customer from 2015 to 2016"
                    #     },
                
                title=f'<b>Count of New/ Repeated Customers in 2016 coming from 2015</b>'
                
).update_layout(height = 550)


fig_payment_type_repeat_cust_type = px.bar(get_Repeated_customer_df().to_pandas(),
                    y='count',x='order_payment_type', color='Repeat_cust_over_Year',
                    orientation='v',
                    # category_orders={'Repeat_cust_over_Year': ['Customer_Repeat_2015','New_Customer']},
                    
                    # labels={
                    #         "Repeat_cust_over_Year": "Repeat Customer from 2015 to 2016"
                    #     },
                    # hover_name= 'perc_count',
                
                title=f'<b>Count of New/ Repeated Customers in 2016 coming from 2015 - by Payment type</b>'
                
).update_layout(height = 550)


col_61,col_62 = st.columns([1,1],gap = "small")

with col_61:
    st.plotly_chart(fig_payment_type_repeat_cust,use_container_width=True, config = config)

with col_62:
    st.plotly_chart(fig_payment_type_repeat_cust_type,use_container_width=True, config = config)


# st.dataframe(get_Repeated_customer_df().to_pandas())

st.markdown("""---""") 
 
#################################           ##########################








#################################           ##########################


col_21,col_22,col_23 = st.columns([3,4,2],gap = "small")
with col_22:
    st.markdown('<p class="big-font">By Product Analytics Category</p>', unsafe_allow_html=True)


plt_box_13,plt_box_23 = st.columns([1,1],gap = "small")

@st.cache_data
def df_product_analytic_category_orders():
    return (Calc_total_orders(groupof=['product_analytic_category'],count_of='order_id'
                                                              ).to_pandas())

@st.cache_data
def df_product_analytic_category_orders_facet():
    return (Calc_total_orders(groupof=['Year','product_analytic_category'],count_of='order_id'
                                                              ).to_pandas())
@st.cache_data
def df_product_analytic_category_gmv():
    return (Calc_total_gmv(groupof=['product_analytic_category'],sum_of='gmv'
                                                                ).to_pandas())
@st.cache_data
def df_product_analytic_category_gmv_facet():
    return (Calc_total_gmv(groupof=['Year','product_analytic_category'],sum_of='gmv'
                                                                ).to_pandas())


with plt_box_13:
    
    # v_spacer(3)
    # gmv_option4 = st.radio("In below Plot - Split by Years?",options=['No','Yes'], horizontal=True, key=4)
    tab16, tab26 = st.tabs(["Total Orders","Total Orders Split By Years"])

    
    # if gmv_option4 == "No":
    fig_product_analytic_category_orders = px.bar(df_product_analytic_category_orders(),
                                x='total_orders',y='product_analytic_category',
                                orientation='h',
                                # hover_name='Party',
                                labels={
                                        "total_orders": "Total Orders"
                                    },
                            
                            title=f'<b>Overall Total Orders by product_analytic_category</b>'
    ).update_yaxes(type='category', categoryorder='max ascending')

    tab16.plotly_chart(fig_product_analytic_category_orders,use_container_width=True, config = config)

    # else:
    fig_product_analytic_category_orders_facet = px.bar(df_product_analytic_category_orders_facet(),
                        x='total_orders',y='product_analytic_category', facet_col='Year',
                        orientation='h',
                        # hover_name='Party',
                        labels={
                                "total_gmv": "Total GMV"
                            },
                    
                    title=f'<b>Overall Total Orders by product_analytic_category</b>'
                    
    ).update_yaxes(type='category', categoryorder='max ascending')
    
    tab26.plotly_chart(fig_product_analytic_category_orders_facet,use_container_width=True, config = config)
    


with plt_box_23:

    # gmv_option5 = st.radio("In below Plot - Split by Years?",options=['No','Yes'], horizontal=True, key=5)
    tab17, tab27 = st.tabs(["Total GMV","Total GMV Split By Years"])

    # if gmv_option5 == "No":
    fig_product_analytic_category_gmv = px.bar(df_product_analytic_category_gmv(),
                                x='total_gmv',y='product_analytic_category',
                                orientation='h',
                                # hover_name='Party',
                                labels={
                                        "total_gmv": "Total GMV"
                                    },
                            
                            title=f'<b>Overall Total GMV by product_analytic_category</b>'
    ).update_yaxes(type='category', categoryorder='max ascending')

    tab17.plotly_chart(fig_product_analytic_category_gmv,use_container_width=True, config = config)

    # else:
    fig_product_analytic_category_gmv_facet = px.bar(df_product_analytic_category_gmv_facet(),
                                x='total_gmv',y='product_analytic_category',facet_col='Year',
                                orientation='h',
                                # hover_name='Party',
                                labels={
                                        "total_gmv": "Total GMV"
                                    },
                            
                            title=f'<b>Overall Total GMV by product_analytic_category</b>'
                    
    ).update_yaxes(type='category', categoryorder='max ascending')
    
    tab27.plotly_chart(fig_product_analytic_category_gmv_facet,use_container_width=True, config = config)
    

st.divider()

##################################           #########################







#################################           ##########################



# v_spacer(3)
# gmv_option6 = st.radio("In below Plot - Use Total GMV instead of Total Orders??",options=['No','Yes'], horizontal=True, key=6)
tab14, tab24 = st.tabs(["Total Orders","Total GMV"])

# if gmv_option6 == "No":
fig_product_analytic_category_orders_facet = px.line(Calc_total_orders(groupof=['Year','Month','product_analytic_category'],count_of='order_id'
                                                        ).to_pandas(),
                    y='total_orders',x='Month', facet_row= 'Year', facet_col= 'product_analytic_category',
                    # orientation='h',
                    # hover_name='Party',
                    labels={
                            "total_orders": "Total Orders"
                        },
                
                title=f'<b>Overall Total Orders by product_analytic_category</b>'
                
).update_layout(height = 650)
tab14.plotly_chart(fig_product_analytic_category_orders_facet,use_container_width=True, config = config)

# else:
fig_product_analytic_category_gmv_facet = px.line(Calc_total_gmv(groupof=['Year','Month','product_analytic_category'],sum_of='gmv'
                                                        ).to_pandas(),
                    y='total_gmv',x='Month', facet_row= 'Year', facet_col= 'product_analytic_category',
                    # orientation='h',
                    # hover_name='Party',
                    labels={
                            "total_gmv": "Total GMV"
                        },
                
                title=f'<b>Overall Total GMV by product_analytic_category</b>'
                
).update_layout(height = 650)

tab24.plotly_chart(fig_product_analytic_category_gmv_facet,use_container_width=True, config = config)
    

# st.divider()

st.markdown("""---""") 

##################################           #########################






