#!/usr/bin/env python
# coding: utf-8

# # Data Engineer Challenge 

# # Setup
# 

# In[2]:


## Library import
import pandas as pd


# In[3]:



# Options for pandas
pd.options.display.max_columns = 50
pd.options.display.max_rows = 30


# Autoreload extension
if 'autoreload' not in get_ipython().extension_manager.loaded:
    get_ipython().run_line_magic('load_ext', 'autoreload')
    
get_ipython().run_line_magic('autoreload', '2')

## Notebook Settings

# Change notebook wideth wth the percentage after the width: keyword 
from IPython.core.display import display, HTML
display(HTML("<style>.container { width:75% !important; }</style>"))

#By default, Jupyter Notebook sets cells to show output without scrolling. To see the entire view, please either:
# 1. press the escape key, then shift and o (o the letter!)
# Or
# 2. Navigate to cell in the toolbar, then current outputs, then toggle scrolling 



# In[20]:


# We define four empty lists, which we will use to derive our insights  
list1 = []
list2 = []
list3 = []
list4 = []


# In[44]:


# The producer function wil produce insights in batches of ten. To view these clearly, I suggest selecting the cell, pressing the escape key and then shift and o together. When you'd like to see the rest of the notebook, press the same combinaton again 


# List of filenames to read in
DATA_FILES = [
'Site_0.csv',
'Site_1.csv',
'Site_2.csv']

# Data import and Preprocessing

# The provided view data and a time window across all domains form the default arguments 
def producer(filenames = DATA_FILES, Ceiling_Minute = 60, Floor_Minute =  0, Ceiling_Second = 60, Floor_Second = 0):
    
    for filename in filenames:
        views = pd.read_csv(filename)
        
        #We check the validity of the input time window here so code does not run before time indexes are out of date 
        if Ceiling_Minute < Floor_Minute:
            raise Exception("Sorry, the lowest input minute, the Floor_ Minute, is greater than input Ceiling_ Minute, no  would be returned")
        if Ceiling_Second < Floor_Second:
            raise Exception("Sorry, the lowest input second, the Floor_ Second, is greater than input Ceiling_ Second, no  would be returned")
        else: 
            print('Input time window, the minimum and maximum input second and minute, are valid')
        
        # Feature engineering -
        views['Site'] = filename.replace('Site_','').replace('.csv','')
        views['SiteViews'] = views.shape[0]
        assert  len(views) > 1, 'Ingested SiteVew file appears empty'
        
        #We sort before we split into blocks of ten rows 
        views = views.sort_values(by = 'timestamp')
        
        views.Timestamp = pd.to_datetime(views.timestamp) 

        views.Timestamp = pd.to_datetime(views.Timestamp,format='%y/%m/%d %H:%M:%S')




        assert (views.Timestamp > pd.datetime.now()).sum() < views.Timestamp.notna().count(), "View time appears to contain labels in the future"

        if (views.Timestamp > pd.datetime.now()).sum() > views.Timestamp.notna().count():
            print("Warining! View time appears to contain labels with future dates")
        elif views.Timestamp.dt.hour.nunique() > 1:
            print("Warining! View time contains labels stretching over multiple hours! Consider reformatting code to analyse hours")

        elif views.Timestamp.dt.date.nunique() >1:
            print("Warining! View time contains labels stretching over multiple days! Consider reformitting code to analyse dates")

        else: 
            print("View time is always labeled with historic dates, as we'd hope! All views appear to be within the same date, within the same hour")
        
        

         # Creating Second and Minute features, used three and four lines below to ensuring time labels are within expected periods
         # Also used to generate insights 
        views['Second'] = views.Timestamp.dt.second
        views['Minute'] = views.Timestamp.dt.minute
        
        
        views = views[views.Second.between(Floor_Second, Ceiling_Second)]
        views = views[views.Minute.between(Floor_Minute, Ceiling_Minute)]
        
        
        #Scaling data type - 
        '''Although not as relevant to the small data sets you kindly provided, streamed data can become quite large!
        df.info() will give us some high level information about our dataframe, including its size, information about data types and memory usage.
        By default, pandas approximates of the memory usage of the dataframe to save time. Because we’re interested in accuracy, we’ll set the memory_usage parameter to 'deep' to get an accurate number.
        Pandas Int8 ranges between [-128 : 127], and Int16 between [-32768 : 32767]. With the describe method, we know 3/5 of our data types fit into int8 dtypes, 2/5 into int 36. However, other batches may produce different streaming methods.
        I have therefore used the errors='raise' argument to allert felow SN collegues if data will not fit in the provided type. In such a situation, use a larger datatype. 
        If this results in memory being problematc, we could load in smaller chunks, or use libaries instead of pandas that allow for lazy evaluation, where  computation is executed only when necessary '''
        
        print('------------------------------------------------------------------------------------------------------------------------------------')

        
        print('Data description before data types are changed:')
        print(views.describe())
        print('-------------------------------------------------------------')
   
        print('Memory use and DTypes before data types are changed:')
        info = views.info(memory_usage='deep')

        #Henceforth, pre-processing and insight extraction is performed ten rows at a time, by iterating through the length of each file in Steps of ten 
        for i in range(0,views.shape[0],10):  
              
            
            tenr = views.iloc[i:i+10]
            
            #Could also use replace, someonelse decide 
            tenr.columns = ['ArticleId', 'UserId', 'Timestamp', 'Site', 'SiteViews', 'Second', 'Minute']


            tenr = tenr[['Timestamp', 'Second', 'Minute', 'ArticleId', 'UserId', 'Site',  'SiteViews']]
            

            









            #Reducing data type after splitting into rows of tens allows specific sizing of the correct data types.  
            if (tenr.SiteViews.astype('int64')).any() > 32767:
                tenr.SiteViews =  tenr.SiteViews.astype('int64', errors='raise')
            elif tenr.SiteViews.astype('int32').between(127, 32767).any():
                tenr.SiteViews =  tenr.SiteViews.astype('int32', errors='raise')
            else:
                tenr.SiteViews =  tenr.SiteViews.astype('int16', errors='raise')


            if tenr.UserId.astype('int64').max() > 32767:
                tenr.UserId =  tenr.UserId.astype('int64', errors='raise')
            elif tenr.UserId.astype('int32').between(127, 32767).any():
                tenr.UserId =  tenr.UserId.astype('int32', errors='raise')
            else:
                tenr.UserId =  tenr.UserId.astype('int16', errors='raise')    

           


            tenr[['Second', 'Minute', 'Site']] = tenr[['Second', 'Minute', 'Site']].astype('int8', errors='raise')
            tenr[['ArticleId']] = tenr[['ArticleId']].astype('category')
            
            print('------------------------------------------------------------------------------------------------------------------------------------')
            print('Memory usage after data types are changed:')
            tenr.info(memory_usage='deep')            
            
            #Here I create a clone of the Article Id feature of our dataset. This is not necessary for calculations, but does mean we can group one whilst aggregate on the other Id, making outputing a clean verson of our results for part two slighly easier. 
            #This is not necessary for part one, as we are aggregating on seperate features, nor part three or four, where we cam aggregate on the generic Timestamp column, rather than the minute or second features  
            tenr['ArticleIdForCount'] =tenr['ArticleId'] 
            tenr['MinuteForCount'] =tenr['Minute'] 
            
            

            # Analysis 
            ## Generate insights in batches of ten and store in iterable, mutable objects (lists)  
    
                  
            # 1. The Site with the most article views


                                                                   #The pandas group by function will return a multi-coluumn index. This can make calling columns, for sorting for example, a little fiddly.
                                                                                                                        #For legibility, and my efficiency, I like to transpose the groupby result, the reset its index, and transpose back 
            MaxSiteViewsDF =  tenr.groupby(['Site'], as_index = False).agg({'SiteViews': ['mean']}).T.reset_index(drop=True).T
            MaxSiteViewsDF.columns = ['Site', 'MeanSiteViews']

            columns=['SingleSiteWithMaxView', 'MaxSiteViewsRow']
            # Here, we sort by the number of Site views in each Site, highest ontop with the asceding False argument, then extract just the first row. We could also extract the first row with the .head method. 
            MaxSiteViewsRow = MaxSiteViewsDF.sort_values(by = MaxSiteViewsDF.columns[-1], ascending=False).iloc[:1]
            SiteWithMaxView = MaxSiteViewsRow.iloc[:,0]
            SiteWithMaxView.columns = ['Site With Highest Views', 'Site Views']

            
            SingleSiteWithMaxView = SiteWithMaxView.iloc[-1] 
            
            
            print('Site With Highest Views in batch:', SingleSiteWithMaxView)
            print('------------------------------------------------------------------------------------------------------------------------------------')

            list1.append(SingleSiteWithMaxView) 

            temporary_df1 = pd.DataFrame([SingleSiteWithMaxView], columns=['MaxSiteViewsRow'])                                                         






            #2. The most common paper


            MaxArtcleviewsDF = tenr.groupby(['ArticleId'], as_index = False).agg({'ArticleIdForCount': ['count']}).T.reset_index(drop=True).T
            MaxArtcleviewsDF.columns = ['ArticleId', 'ArticleIdForCount']

            ArticleIdviewsRow = MaxArtcleviewsDF.sort_values(by = MaxArtcleviewsDF.columns[-1], ascending=False).iloc[:1]
            ArticleIdwithMaxView = ArticleIdviewsRow.iloc[:,0]
            ArticleIdwithMaxView.columns = ['Article With Highest views', 'Article views']
            ArticleIdOnlywithMaxView = ArticleIdwithMaxView.iloc[-1]
            
            list2.append(ArticleIdOnlywithMaxView)




            print('Article With Highest Views in batch:', ArticleIdOnlywithMaxView)
            print('------------------------------------------------------------------------------------------------------------------------------------')


            # 3. Busiest 1 minute period


            MaxArtcleviewsDF = tenr.groupby(['Minute'], as_index = False).agg({'MinuteForCount': ['count']}).T.reset_index(drop=True).T
            MaxArtcleviewsDF.columns = ['Minute', 'MinuteForCount']

            MinuteviewsRow = MaxArtcleviewsDF.sort_values(by = MaxArtcleviewsDF.columns[-1], ascending=False).iloc[:1]
            MinutewithMaxView = MinuteviewsRow.iloc[:,0]
            MinutewithMaxView.columns = ['Minute With Highest views', 'Minute views']
            MinuteOnlywithMaxView = MinutewithMaxView.iloc[-1]

            list3.append(MinuteOnlywithMaxView)


            ''' I decided to display the minute the the greatest number of views in addition to the views of that minute. 
             SN's sites will go under significantly more stress at peak times, spotting temporal patterns could help prevent problems in the past, or identify the aetiology of known patterns. 
             The two string method removes the index from the displayed output. The index returns the dataframe row number, and, although corresponding to time in our sorted dataframe, is unlikely to generate meaningful insights to our user. '''
            print('Article With Highest Views in batch:', MinutewithMaxView.to_string())
            print('------------------------------------------------------------------------------------------------------------------------------------')




             # 4.  Suspected Robot Users - continuous clicks on different articles by the same


            ''' I have also explained how I defined a robot user. 
            We could also define a robot user as having greater clicks in a time window than the user average by a certain quantity, 
            eg.MaxSecondviewsDFSorted[MaxSecondviewsDFSorted['views in Suspect Second:'] >  (MaxSecondviewsDFSorted['views in Suspect Second:'].mean() * 3)] will return information on users with more than three times the sample average '''
            
            
            tenr['ix'] = tenr.index

            RoboGroup = tenr.groupby(['UserId', 'Timestamp'], as_index = False).agg({'ArticleId': ['count']}).T.reset_index(drop=True).T
            RoboGroup.columns = ['UserId', 'Second','ArticleIdCount']
            RoboGroup.set_index('UserId')
            topRoboRow = RoboGroup.sort_values(by = 'ArticleIdCount', ascending=False).head(1)
            topRoboRowID = topRoboRow.UserId
            if topRoboRow.ArticleIdCount.any() > 5:  
                list4.append(topRoboRowID)
                print('Warning a potential robot is present - A robot is identified when a user is responsible for more than 12 views a second') 
            else: 
                print('A robot is identified when a user is responsible for more than 12 views a second, no robots appear to be present in this ten view batch') 
           


producer(filenames = DATA_FILES, Ceiling_Minute = 60, Floor_Minute =  6, Ceiling_Second = 60, Floor_Second = 0)


# In[ ]:


# Consumer function to display inisghts from the entirity of viewed data =


# In[45]:


def consumer(Q1, Q2, Q3, Q4): 
    if Q1 == 1:
        print('Site With Greatest Views from all batches:', max(list1,key=list1.count)) 
    if Q2 == 1:
        print('Article With Greatest Views from all batches:', max(list2,key=list2.count)) 
    if Q3 == 1:
        print('Minute With Greatest Views from all batches:', max(list3,key=list3.count)) 
    if Q4 == 1:
        print('No & Id: of Potential Robots')
        print(list4[0])
consumer(Q1 = 1, Q2 = 1,  Q3 = 1, Q4 = 1) 


# # Additional outlier function
# 
# ## Often streamed data produces anomolies we would not expect  
# ## Outlier removal was not included in the question and so could affect exped results. Hence I have withdrawn presence from within the core function 

# In[ ]:


# Lists numeric columns we can pick to extract outliers 
print('possibleColsToTrim', producer.viewsSorted.select_dtypes('number').columns) 

# Pick columns to extract/view outliers  

ColsToTrim = [ 'UserId', 'SiteViews']

  


# In[ ]:


def remove_outlier(ColsToTrim, floor = 0.05, ceiling = 0.95):
    df_in = producer.viewsSorted[ColsToTrim]
    q1 = df_in.quantile(floor)
    q3 = df_in.quantile(ceiling)
    iqr = q3-q1 #Interquartile range
    fence_low  = q1-1.5*iqr
    fence_high = q3+1.5*iqr
    df_trim = df_in[(df_in > fence_low) & (df_in < fence_high)]
    df_out = pd.concat([df_trim, producer.viewsSorted], axis=1).dropna(subset = ColsToTrim)
    if len(df_trim) * 10 > len(df_in):
        print('Waring! - More than 10% of data will be removed with set input columns, Floor and Ceiling combo!')
    return df_out
remove_outlier(ColsToTrim, 0.4, 0.65)

