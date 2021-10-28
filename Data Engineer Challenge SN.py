#!/usr/bin/env python
# coding: utf-8

# # Data Engineer Challenge 

# # Setup
# 
# ## Library import
# We import all the required Python libraries

# In[1]:


# Data manipulation
import pandas as pd

# Options for pandas
pd.options.display.max_columns = 50
pd.options.display.max_rows = 30


# Autoreload extension
if 'autoreload' not in get_ipython().extension_manager.loaded:
    get_ipython().run_line_magic('load_ext', 'autoreload')
    
get_ipython().run_line_magic('autoreload', '2')


# ## Notebook Settings
# 

# In[2]:



from IPython.core.display import display, HTML
display(HTML("<style>.container { width:75% !important; }</style>"))


# 
# # Data import
# We retrieve all the required data for the analysis.

# In[3]:


site_0 = pd.read_csv(r'C:\Users\Sam.Winter\OneDrive - Wilmington\Documents\Physio\Springer Nature\site_0.csv')
site_1 = pd.read_csv(r'C:\Users\Sam.Winter\OneDrive - Wilmington\Documents\Physio\Springer Nature\site_1.csv')
site_2 = pd.read_csv(r'C:\Users\Sam.Winter\OneDrive - Wilmington\Documents\Physio\Springer Nature\site_2.csv')


# # Data processing
# Put here the core of the notebook. Feel free di further split this section into subsections.

# In[4]:


site_0['Site'] = 0
site_1['Site'] = 1
site_2['Site'] = 2 


# In[5]:


# I have decided against using strict camelCase, and capatalized the beginning of words too. Partly for aesthetic reasons, but also to avoid stealing conventions from javaScript 
site_0['SiteViews'] = site_0.shape[0]
site_1['SiteViews'] = site_1.shape[0]
site_2['SiteViews'] = site_2.shape[0]
allViews = pd.concat([site_0, site_1, site_2])
assert  len(allViews) > 1, 'Ingested siteVew files appear empty'

allViews.timestamp = pd.to_datetime(allViews.timestamp) 

allViews.Timestamp = pd.to_datetime(allViews.timestamp,format='%y/%m/%d %H:%M:%S')
assert (allViews.Timestamp > pd.datetime.now()).sum() < allViews.Timestamp.notna().count(), "View time appears to contain labels in the future"

if (allViews.Timestamp > pd.datetime.now()).sum() > allViews.Timestamp.notna().count():
    print("Warining! View time appears to contain labels with future dates")
elif allViews.Timestamp.dt.hour.nunique() > 1:
    print("Warining! View time contains labels stretching over multiple hours! Consider reformatting code to analyse hours")

elif allViews.Timestamp.dt.date.nunique() >1:
        print("Warining! View time contains labels stretching over multiple days! Consider reformitting code to analyse dates")

else: 
    print("View time is always labeled with historic dates, as we'd hope! All views appear to be within the same date, within the same hour")

allViews['Second'] = allViews.Timestamp.dt.second
allViews['Minute'] = allViews.Timestamp.dt.minute

#Could also use replace, someonelse decide 
allViews.columns = ['ArticleId', 'UserId', 'Timestamp', 'site', 'siteViews', 'Second', 'Minute']
allViews = allViews[['Timestamp', 'Second', 'Minute', 'ArticleId', 'UserId', 'site', 'siteViews']]
allViews.describe()


# In[6]:


# Checking for empty fields, by column 
if allViews.isna().sum().sum() > 0:
    print('Data Contains Null Values')
    display('site_0 data null values', site_0.isna().sum(), 'site_1 data null values', site_1.isna().sum(), 'site_2 data null values', site_2.isna().sum(), 'Combined data null values', allViews.isna().sum())


# In[7]:


'''Although not as relevant to the small data sets you kindly provided, streamed data can become quite large!
df.info() will give us some high level information about our dataframe, including its size, information about data types and memory usage.
By default, pandas approximates of the memory usage of the dataframe to save time. Because we’re interested in accuracy, we’ll set the memory_usage parameter to 'deep' to get an accurate number.
Pandas Int8 ranges between [-128 : 127], and Int16 between [-32768 : 32767]. With the describe method, we know 3/5 of our data types fit into int8 dtypes, 2/5 into int 36. However, other batches may produce different streaming methods.
I have therefore used the errors='raise' argument to allert felow SN collegues if data will not fit in the provided type. In such a situation, use a larger datatype. 
If this results in memory being problematc, we could load in smaller chunks, or use libaries instead of pandas that allow for lazy evaluation, where  computation is executed only when necessary '''

allViews.info(memory_usage='deep')

allViews.select_dtypes(include ='int64') 
allViews[['Second', 'Minute', 'site']] = allViews[['Second', 'Minute', 'site']].astype('int8', errors='raise')
allViews[['UserId','siteViews']] = allViews[['UserId','siteViews']].astype('int16', errors='raise')

allViews[['ArticleId']] = allViews[['ArticleId']].astype('category')

allViews.info(memory_usage='deep')


# # Producer to Batch Views in Chronological Order 

# In[8]:


def producer(Ceiling_Minute = allViews.Minute.max(), Floor_Minute =  allViews.Minute.max(), Ceiling_Second = allViews.Second.max(), Floor_Second = allViews.Second.min()):


    if Ceiling_Minute < Floor_Minute:
        raise Exception("Sorry, the lowest input minute, the Floor_ Minute, is greater than input Ceiling_ Minute, no views would be returned")
    if Ceiling_Second < Floor_Second:
        raise Exception("Sorry, the lowest input second, the Floor_ Second, is greater than input Ceiling_ Second, no views would be returned")

    print('Time Window Analysed:     ',  pd.to_datetime(allViews.Timestamp[0]).dt.date.drop_duplicates().to_string(index = False), '   ', pd.to_datetime(allViews.Timestamp[0]).dt.hour.drop_duplicates().to_string(index = False),'hour     ',
          Ceiling_Minute, 'min :', Ceiling_Second, 'sec -', Floor_Minute, 'min :', Floor_Second , 'sec')
       
    
 #   print('Time Window Analysed:     ',  pd.to_datetime(cSpeak['START DATE/TIME']).date(), Ceiling_Minute, 'min :', Ceiling_Second, 'sec -', Floor_Minute, 'min :', Floor_Second , 'sec')
    print('------------------------------------------------------------------------------------------------------------------------------------')
    
    producer.allViewsSorted = allViews.sort_values(by = 'Timestamp')


    MinSecond = producer.allViewsSorted['Second'] > Floor_Second
    MaxSecond = producer.allViewsSorted['Second'] < Ceiling_Second
    MinMinute = producer.allViewsSorted['Minute'] > Floor_Minute
    MaxMinute = producer.allViewsSorted['Minute'] < Ceiling_Minute
    TimeWindow = producer.allViewsSorted[(MinSecond) ] # & (MaxSecond) ]  # & (MinMinute) & (MaxMinute)]



    #Here, I outline a way to return our AllViews dataframe in batches. The first line splits the dataframe into rows of ten using a range loop. In conjustion with the second line, yield i, the producer function will output our AllViews dataframe in batches of ten rows at a time when called upon
    for i in range(0,TimeWindow.shape[0],10):
        yield i




# I have not used np.inf here to set default arguments in the producer function defination. If we alter the default arguments in the function call to infinite values, this will mean the function can be used on any input View data. However, would make the code run slighly slower. 
#I have used the dataset absolutes and shown how infinites can be used below to show both are possible, and defined also infinity in different ways to make the code slighly less boring! 
# Ceiling_Minute =  np.inf, Floor_Minute = float('-inf'), Ceiling_Second = float('inf'), Floor_Second = float('-inf')

next(producer(Ceiling_Minute = allViews.Minute.max(), Floor_Minute =  allViews.Minute.max(), Ceiling_Second = allViews.Second.max(), Floor_Second = allViews.Second.min()))


# # Comsumer to generate insights

# In[9]:


def consumer(Ceiling_Minute = 12, Floor_Minute = 8, Ceiling_Second = 20, Floor_Second = 2):
    
    producer.ViewsSortedBatched = next(producer(Ceiling_Minute = 12, Floor_Minute = 2, Ceiling_Second = 20, Floor_Second = 2))
      
    


    # 1. The site with the most article views

    
    # Here I create a clone of the Article Id feature of our dataset. This is not necessary for calculations, but does mean we can group one whilst aggregate on the other Id, making outputing a clean verson of our results for part two slighly easier. 
    #This is not necessary for part one, as we are aggregating on seperate features, nor part three or four, where we cam aggregate on the generic timestamp column, rather than the minute or second features  
    producer.allViewsSorted['ArticleIdForCount'] = producer.allViewsSorted['ArticleId'] 

                                                                                                                #The pandas group by function will return a multi-lolumn index. This can make calling columns, for sorting for example, a little fiddly.
                                                                                                                #For legibility, and my efficiency, I like to transpose the groupby result, the reset its index, and transpose back 
    MaxSiteViewsDF =  producer.allViewsSorted.groupby(['site'], as_index = False).agg({'siteViews': ['count']}).T.reset_index(drop=True).T
    MaxSiteViewsDF.columns = ['site', 'MeanSiteViews']

    # Here, we sort by the number of site views in each site, highest ontop with the asceding False argument, then extract just the first row. We could also extract the first row with the .head method. 
    MaxSiteViewsRow = MaxSiteViewsDF.sort_values(by = MaxSiteViewsDF.columns[-1], ascending=False).iloc[:1]
    SiteWithMaxView = MaxSiteViewsRow.iloc[:,0]

    SiteWithMaxView.columns = ['Site With Highest Views', 'Site Views']
    consumer.SingleSiteWithMaxView = SiteWithMaxView.iloc[-1]

    print('Site With Highest Views:', consumer.SingleSiteWithMaxView)
    print('------------------------------------------------------------------------------------------------------------------------------------')
    
    
    #2. The most common paper

    
    MaxArtcleViewsDF = producer.allViewsSorted.groupby(['ArticleId'], as_index = False).agg({'ArticleIdForCount': ['count']}).T.reset_index(drop=True).T
    MaxArtcleViewsDF.columns = ['ArticleId', 'ArticleIdForCount']

    ArticleIdViewsRow = MaxArtcleViewsDF.sort_values(by = MaxArtcleViewsDF.columns[-1], ascending=False).iloc[:1]
    ArticleIdwithMaxView = ArticleIdViewsRow.iloc[:,0]
    ArticleIdwithMaxView.columns = ['Article With Highest Views', 'Article Views']
    consumer.ArticleIdOnlywithMaxView = ArticleIdwithMaxView.iloc[-1]
 
    
    print('Article With Highest Views:', consumer.ArticleIdOnlywithMaxView)
    print('------------------------------------------------------------------------------------------------------------------------------------')

    
    # 3. Busiest 1 minute period
    
    MaxSecondViewsDF = producer.allViewsSorted.groupby(['Minute'], as_index = False).agg({'Timestamp': ['count']}).T.reset_index(drop=True).T
    MaxSecondViewsDF.columns = ['Most Viewed Minute:', 'Views in Busiest Minute:']

    SecondViewsRow = MaxSecondViewsDF.sort_values(by = MaxSecondViewsDF.columns[-1], ascending=False).iloc[:1]
    consumer.SecondwithMaxView = SecondViewsRow.iloc[-1]

    consumer.SecondwithMaxView.columns = ['Minutes With Highest Views', 'Minutes View Counts']

    
    # I decided to display the minute the the greatest number of views in addition to the views of that minute. SN's sights will go under significantly more stress at peak times, spotting temporal patterns could help prevent problems in the past, or identify the aetiology of known patterns. 
    # The two string method removes the index from the displayed output. The index returns the dataframe row number, and, although corresponding to time in our sorted dataframe, is unlikely to generate meaningful insights to our user. 
    print(consumer.SecondwithMaxView.to_string())
    print('------------------------------------------------------------------------------------------------------------------------------------')


    

    # 4.  Suspected Robot Users - continuous clicks on different articles by the same
    

    MaxSecondViewsDF = producer.allViewsSorted.groupby(['UserId', 'Second'], as_index = False).agg({'Timestamp': ['count']}).T.reset_index(drop=True).T
    MaxSecondViewsDF.columns = ['User Id of potential Robot(s):', 'Suspect Second:', 'Views in Suspect Second:']
    consumer.MaxSecondViewsDFSorted = MaxSecondViewsDF.sort_values(by = 'Suspect Second:', ascending=False) #could use .head(1)
    
    # I decided to display display additional information on robot users - the corresponding second(s) of activitity, and the views in that second.  
    # The minute the the greatest number of views in addition to the views of that minute. SN's sights will go under significantly more stress at peak times, spotting temporal patterns could help prevent problems in the past, or identify the aetiology of known patterns. 
    
    ''' I have also explained how I defined a robot user. 
    We could also define a robot user as having greater clicks in a time window than the user average by a certain quantity, 
    eg.consumer.MaxSecondViewsDFSorted[consumer.MaxSecondViewsDFSorted['Views in Suspect Second:'] >  (consumer.MaxSecondViewsDFSorted['Views in Suspect Second:'].mean() * 3)] will return information on users with more than three times the sample average '''
    
    print('A Robot is identified when a user is responsible for more than 12 views a second') 
    print(consumer.MaxSecondViewsDFSorted[consumer.MaxSecondViewsDFSorted['Views in Suspect Second:'] > 12].to_string(index = False))
    if consumer.MaxSecondViewsDFSorted[consumer.MaxSecondViewsDFSorted['Views in Suspect Second:'] > 12]['User Id of potential Robot(s):'].nunique() > 3:
        print('Warning - We Appear to have been affected by more than three robots!')

        

    
    
    
    
#percenage of robots
    
consumer() 


# In[10]:


# This functon is not necessary! I have stripped out the 'backend' of the producer function above, and extracted the 'output' print statements. I have also included the producer function call as and additional way to define our time constraints.
#I decided to split up the function as I felt the consumer function contained a lot of code, and havng just the outputs could make it a little easier to read the notebook. Feel free to skip this cell of course 

def consume(Ceiling_Minute = allViews.Minute.max(), Floor_Minute =  allViews.Minute.max(), Ceiling_Second = allViews.Second.max(), Floor_Second = allViews.Second.min()):
    
    producer.ViewsSortedBatched = next(producer(Ceiling_Minute = 12, Floor_Minute = 8, Ceiling_Second = 20, Floor_Second = 2))

    
    # 1. The site with the most article views

    print('Site With Highest Views:', consumer.SingleSiteWithMaxView)
    print('------------------------------------------------------------------------------------------------------------------------------------')
    
    
    #2. The most common paper
    
    print('Article With Highest Views:', consumer.ArticleIdOnlywithMaxView)
    print('------------------------------------------------------------------------------------------------------------------------------------')

    
    # 3. Busiest 1 minute period

    print(consumer.SecondwithMaxView.to_string())
    print('------------------------------------------------------------------------------------------------------------------------------------')


    

    # 4.  Suspected Robot Users - continuous clicks on different articles by the same

    print('A Robot is identified when a user viewing articals more than 12 times a second') 
    print(consumer.MaxSecondViewsDFSorted[consumer.MaxSecondViewsDFSorted['Views in Suspect Second:'] > 12].to_string(index = False))
    if consumer.MaxSecondViewsDFSorted[consumer.MaxSecondViewsDFSorted['Views in Suspect Second:'] > 12]['User Id of potential Robot(s):'].nunique() > 3:
        print('Warning - We Appear to have been affected by more than three robots!')

    

        
consume(Ceiling_Minute = allViews.Minute.max(), Floor_Minute =  allViews.Minute.max(), Ceiling_Second = allViews.Second.max(), Floor_Second = allViews.Second.min())


# In[11]:


# 5. All of the above (#1-#4) for given time windows.  
# This call is also not necessary for our code to run, or to generate output but I have included to allow easy time window definition, and fuor the purpose of abstraction. We can call the comsume or consumer function and recieve the same output 

consume(Ceiling_Minute = 12, Floor_Minute = 8, Ceiling_Second = 20, Floor_Second = 2) 


# # Additional function to remove outliers from our data
# ### Normally I would remove outliers at the top of our code, before analysis or machine learning is undertaken, but given outlier removal was not specified as a given task, I have included this little function at the end

# In[12]:


# Lists numeric columns we can pick to extract outliers 
print('possibleColsToTrim', producer.allViewsSorted.select_dtypes('number').columns) 


# In[13]:


# Pick columns to extract/view outliers  

ColsToTrim = [ 'UserId', 'siteViews']

  
def remove_outlier(ColsToTrim, floor = 0.05, ceiling = 0.95):
    df_in = producer.allViewsSorted[ColsToTrim]
    q1 = df_in.quantile(floor)
    q3 = df_in.quantile(ceiling)
    iqr = q3-q1 #Interquartile range
    fence_low  = q1-1.5*iqr
    fence_high = q3+1.5*iqr
    df_trim = df_in[(df_in > fence_low) & (df_in < fence_high)]
    df_out = pd.concat([df_trim, producer.allViewsSorted], axis=1).dropna(subset = ColsToTrim)
    if len(df_trim) * 10 > len(df_in):
        print('Waring! - More than 10% of data will be removed with set input columns, Floor and Ceiling combo!')
    return df_out
remove_outlier(ColsToTrim, 0.4, 0.65)

