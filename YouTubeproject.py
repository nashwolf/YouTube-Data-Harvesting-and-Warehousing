from googleapiclient.discovery import build
import mysql.connector
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import streamlit as st
import matplotlib.pyplot as plt
import re


api_service_name= 'youtube'
api_version = "v3"
api_key= 'AIzaSyAlR99WN9ipMef0YDBJnk505lidMZqIW0c'
youtube = build(api_service_name, api_version, developerKey= api_key)


def channel_data (channel_id):
    api_service_name = "youtube"
    api_version = "V3"
    api_key ='AIzaSyAlR99WN9ipMef0YDBJnk505lidMZqIW0c'
    youtube = build(api_service_name,api_version,developerKey=api_key)
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response=request.execute()
    channel_id=response ['items'][0]['id']
    channel_name = response['items'][0]['snippet']['title']
    channel_description = response['items'][0]['snippet']['description']
    channel_published = response['items'][0]['snippet']['publishedAt']
    channel_playlist = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    channel_subcount = response['items'][0]['statistics']['subscriberCount']
    channel_videocount = response['items'][0]['statistics']['videoCount']
    channel_viewcount = response['items'][0]['statistics']['viewCount']
    
    d = {
        'channel_Id':channel_id,
        'channel_name': channel_name,
        'channel_description':channel_description,
        'channel_publishedat': channel_published,
        'channel_playlist': channel_playlist,
        'channel_subscount': channel_subcount,
        'channel_videocount': channel_videocount,
        'channel_viewcount': channel_viewcount
       
    }
    return d

def get_videoIds(channel_id):
    video_ids=[]
    response= youtube.channels().list(id=channel_id,
                                   part='contentDetails').execute()
    Playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                                
    next_page_token=None
    
    while True:
        Result=youtube.playlistItems().list(
        part='snippet',
        playlistId=Playlist_id,
        maxResults=50,
        pageToken = next_page_token).execute()
        for i in range(len(Result['items'])):
            video_ids.append(Result['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=Result.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids

def Video_Details(video_ids):
    Video_Data=[]
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        for item in response['items']:
            data=dict( Channel_Name=item['snippet']['channelTitle'],
                       Channel_id=item['snippet']['channelId'],
                       Video_Id=item['id'],
                       Title=item['snippet']['title'],
                       Tags=item['snippet'].get('tags'),
                       Thumbnail=item['snippet']['thumbnails']['default']['url'],
                       Description=item['snippet'].get('description'),
                       Published_Date=item['snippet']['publishedAt'] ,
                       Duration=item['contentDetails']['duration'],
                       Views=item['statistics'].get('viewCount'),
                       Likes=item['statistics'].get('likeCount'),
                       Comments=item['statistics'].get('commentCount'),
                       Favorite=item['statistics']['favoriteCount'],
                       Definition=item['contentDetails']['definition'],
                       Caption=item['contentDetails']['caption'])
            Video_Data.append(data)
    return Video_Data

def comment_data(video_ids):
    comment_details=[]
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(    
                part='snippet',  
                videoId=video_id,
                maxResults=100
            )
            response = request.execute()

            for item in response['items']:
                data=dict(
                        Comment_ID=item['id'],
                        Video_ID=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])

                comment_details.append(data)
    except:
        pass
    return comment_details 

#Adding the data to mongoDB
import pymongo
from pymongo import MongoClient
client= MongoClient("mongodb://localhost:27017")
db=client["Youtube_Harvesting"]

def channel_details(channel_id):
    ch_details=channel_data(channel_id)
    vi_ids=get_videoIds(channel_id)
    vi_details=Video_Details(vi_ids)
    comm_details=comment_data(vi_ids)
    
    coll1=db["Channel_details"]
    coll1.insert_one({"channel_information":ch_details,
                     "comment_information":comm_details,
                     "video_information":vi_details})
    
    return "Uploaded"

import mysql.connector
import pandas as pd
from pymongo import MongoClient

def populate_channels_table():
    # MySQL Connection
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        password='nash123$',
        database='youtube_data',
        port='3306'
    )
    cursor = mydb.cursor()

    # Drop table if exists
    drop_query = '''DROP TABLE IF EXISTS channels'''
    cursor.execute(drop_query)
    mydb.commit()

    # Create table if not exists
    create_query = '''
        CREATE TABLE IF NOT EXISTS channels(
            channel_name varchar(100),
            channel_Id varchar(80) PRIMARY KEY,
            channel_description text,
            channel_publishedat varchar(80),
            channel_playlist varchar(80),
            channel_subscount bigint,
            channel_videocount bigint,
            channel_viewcount bigint
        )
    '''
    cursor.execute(create_query)
    mydb.commit()

    # MongoDB Connection
    client = MongoClient("mongodb://localhost:27017")
    db = client["Youtube_Harvesting"]

    # Retrieve data from MongoDB and populate the MySQL table
    ch_list = []
    coll1 = db['Channel_details']
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data['channel_information'])
    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''
            INSERT INTO channels(
                channel_name, channel_Id, channel_description,
                channel_publishedat, channel_playlist,
                channel_subscount, channel_videocount, channel_viewcount
            )
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
        '''

        values = (
            row['channel_name'], row['channel_Id'], row['channel_description'],
            row['channel_publishedat'], row['channel_playlist'],
            row['channel_subscount'], row['channel_videocount'], row['channel_viewcount']
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Values already inserted or duplicate key violation occurred.")
            

import mysql.connector
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

def populate_comments_table():
    try:
        mydb = mysql.connector.connect(
            host='localhost',
            user='root',
            password='nash123$',
            database='youtube_data',
            port='3306'
        )
        cursor = mydb.cursor()

        # Drop table if exists
        drop_query = '''DROP TABLE IF EXISTS comments'''
        cursor.execute(drop_query)
        mydb.commit()

        # Create table if not exists
        create_query = '''
            CREATE TABLE IF NOT EXISTS comments(
                                             Comment_ID VARCHAR(100) PRIMARY KEY,
                                             Video_ID VARCHAR(100),
                                             Comment_Text TEXT,
                                             Author VARCHAR(100),
                                             Comment_Published timestamp

            )
        '''
        cursor.execute(create_query)
        mydb.commit()


    except mysql.connector.Error as error:
        print("Error occurred:", error)

    com_list = []
    db = client["Youtube_Harvesting"]
    coll1 = db['Channel_details']
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        comment_info = com_data.get("comment_information") 
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

    df2=pd.DataFrame(com_list)


    for index, row in df2.iterrows():
        insert_query = '''
            INSERT INTO comments(
                Comment_ID, Video_ID, Comment_Text,
                Author, Comment_Published
            )
            VALUES(%s, %s, %s, %s, %s)
        '''

        # Converting the datetime string to the MySQL datetime format
        comment_published = datetime.strptime(row['Comment_Published'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

        values = (
            row['Comment_ID'], row['Video_ID'], row['Comment_Text'],
            row['Author'], comment_published  # converted datetime
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Values already inserted or duplicate key violation occurred.")


    
import re
from datetime import datetime
import mysql.connector
import pandas as pd
from pymongo import MongoClient


def populate_videos_table():
    try:
        mydb = mysql.connector.connect(
            host='localhost',
            user='root',
            password='nash123$',
            database='youtube_data',
            port='3306'
        )
        cursor = mydb.cursor()
    
        # Drop table if exists
        drop_query = '''DROP TABLE IF EXISTS videos'''
        cursor.execute(drop_query)
        mydb.commit()
    
        # Create table if not exists
        create_query = '''
            CREATE TABLE IF NOT EXISTS videos(
                Channel_Name VARCHAR(100),
                Channel_id VARCHAR(100),
                Video_Id VARCHAR(30) PRIMARY KEY,
                Title VARCHAR(150),
                Tags TEXT,
                Thumbnail VARCHAR(200),
                Description TEXT,
                Published_Date timestamp,
                Duration TIME,
                Views BIGINT,
                Likes BIGINT,
                Comments INT,
                Favorite INT,
                Definition VARCHAR(10),
                Caption VARCHAR(30)
            )
        '''
        cursor.execute(create_query)
        mydb.commit()
    
    except mysql.connector.Error as error:
        print("Error occurred:", error)
    
    vi_list = []
    db = client["Youtube_Harvesting"]
    coll1 = db['Channel_details']
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        video_info = vi_data.get("video_information") 
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
            
    df1=pd.DataFrame(vi_list)
    
    def parse_duration(duration_str):
        pattern = re.compile(r'PT(\d+)M(\d+)S')  
        match = pattern.match(duration_str)
        
        if match:
            minutes = int(match.group(1))
            seconds = int(match.group(2))
            return f"00:{minutes:02d}:{seconds:02d}"
        else:
            return "00:00:00"  # Return a default value for invalid formats
    
    for index, row in df1.iterrows():
        insert_query = '''
            INSERT INTO videos(
                Channel_Name, Channel_id, Video_Id, Title,
                Tags, Thumbnail, Description, Published_Date,
                Duration, Views, Likes, Comments, Favorite,
                Definition, Caption 
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        formatted_published_date = datetime.strptime(row['Published_Date'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
        
        # Converting a list to a string (comma-separated)
        tags_str = ', '.join(row['Tags']) if isinstance(row['Tags'], list) else row['Tags']
        thumbnail_str = ', '.join(row['Thumbnail']) if isinstance(row['Thumbnail'], list) else row['Thumbnail']
    
        # Extracting duration from MongoDB format and convert it
        duration = parse_duration(row['Duration'])
    

        values = (
            row['Channel_Name'], row['Channel_id'], row['Video_Id'],
            row['Title'], tags_str, thumbnail_str, row['Description'],
            formatted_published_date, duration, row['Views'],
            row['Likes'], row['Comments'], row['Favorite'],
            row['Definition'], row['Caption']
        )
    
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except mysql.connector.IntegrityError as e:
            print("Error:", e)
            print("Values already inserted or duplicate key violation occurred.")


def tables():
    populate_channels_table()
    populate_videos_table()
    populate_comments_table()

    return"Tables created successfully"

import streamlit as st

def show_channel_table():
    ch_list = []
    db = client["Youtube_Harvesting"]
    coll1 = db['Channel_details']
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data['channel_information'])
    df = st.dataframe(ch_list)
    
    return df


import streamlit as st

def show_video_table():
    vi_list = []
    db = client["Youtube_Harvesting"]
    coll1 = db['Channel_details']
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        video_info = vi_data.get("video_information") 
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df1=st.dataframe(vi_list)
    
    return df1

import streamlit as st

def show_comment_table():
    com_list = []
    db = client["Youtube_Harvesting"]
    coll1 = db['Channel_details']
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        comment_info = com_data.get("comment_information") 
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

    df2=st.dataframe(com_list)
    
    return df2

#streamlit code

with st.sidebar:
    st.title(":red[YouTube Data Harvesting and Warehousing]")
    st.header("Skills Take Away")
    st.caption("Python Scripting")
    st.caption("API Integration")
    st.caption("Data Extraction")
    st.caption("Data Storage in MongoDB")
    st.caption("Data handling using MongoDB and SQL")
    

channel_id=st.text_input("Enter the Channel ID")

if st.button("collect and store data"):
    ch_list = []
    db = client["Youtube_Harvesting"]
    coll1 = db['Channel_details']
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data['channel_information']['channel_Id'])
    
    if channel_id in ch_list:
        st.success("The entered Channel ID details already exists")
        
    else:
        insert=channel_details(channel_id)
        st.success(insert)
        
        
if st.button("Migrate data to SQL"):
    Tables=tables()
    st.success(Tables)
    
show_table = st.radio("SELECT THE TABLE TO VIEW", ("CHANNELS", "VIDEOS", "COMMENTS"))

if show_table=="CHANNELS":
    show_channel_table()
    
elif show_table=="VIDEOS":
    show_video_table()
    
elif show_table=="COMMENTS":
    show_comment_table()

    
#SQL connection
mydb = mysql.connector.connect(
    host='localhost',
    user='root',
    password='nash123$',
    database='youtube_data',
    port='3306'
)
cursor = mydb.cursor()

questions= st.selectbox("Select Your Question",("1.What are the names of all the video and their corresponding channels?",
                                               "2.Which channels have the most number of videos, and how many videos do they have?",
                                               "3.What are the top 10 most viewed videos and their respective channels?",
                                               "4.How  many comments were made on each video, and what are their corresponding channel names?",
                                               "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                               "6.What is the total number of likes for each video, and what are their corresponding video names?",
                                               "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                               "8.What are the names of all the channels that have published videos in the year 2022?",
                                               "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                               "10.Which videos have the highest number of comments and what are their corresponding channel names?",))

if questions=="1.What are the names of all the video and their corresponding channels?":
    query1 = '''SELECT Title AS videos, Channel_Name AS channelname FROM videos'''
    cursor.execute(query1)
    
    t1 = cursor.fetchall()
    df1 = pd.DataFrame(t1, columns=["video title", "channel name"])
    
    cursor.close()
    mydb.close()
    st.write(df1)
  

elif questions=="2.Which channels have the most number of videos, and how many videos do they have?":
    query2 = '''SELECT Channel_Name AS channelname,channel_videocount AS no_videos FROM channels
            order by channel_videocount desc'''
    cursor.execute(query2)
    
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns=["channel name","No of Videos"])
    
    cursor.close()
    mydb.close()
    st.write(df2)
    plt.figure(figsize=(10, 6))
    plt.bar(df2['channel name'], df2['No of Videos'], color=['blue', 'green', 'orange',"yellow", 'red',"purple","pink", "violet","skyblue"])  
    plt.xticks(rotation=45)
    plt.xlabel('Channel Name')
    plt.ylabel('Number of Videos')
    plt.title('Channels with the Most Number of Videos')

    st.pyplot(plt)

elif questions=="3.What are the top 10 most viewed videos and their respective channels?":
    query3 = '''SELECT Views AS views, Channel_Name AS channelname, Title AS videotitle FROM videos
                where Views is not null order by Views desc limit 10'''
    cursor.execute(query3)
    
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns=["views","channel name","video title"])
    
    cursor.close()
    mydb.close()
    st.write(df3)
    df = df3.sort_values(by='views', ascending=True)

    plt.figure(figsize=(8, 6))
    bars = plt.barh(df3['video title'], df3['views'], color='skyblue')
    plt.xlabel('views')
    plt.ylabel('video title')
    plt.title('Top 10 Most Viewed Videos')
    
    # Annotating bars with view counts
    for bar, view_count in zip(bars, df3['views']):
        plt.text(bar.get_width(), bar.get_y() + bar.get_height()/2, f'{view_count}', 
                 ha='left', va='center')
    
    st.pyplot(plt)

elif questions=="4.How  many comments were made on each video, and what are their corresponding channel names?":
    query4 = '''SELECT Comments AS no_comments, Title AS videotitle FROM videos
                where Comments is not null'''
    cursor.execute(query4)
    
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns=["No of Comments","video title"])
    
    cursor.close()
    mydb.close()
    st.write(df4)

elif questions=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5 = '''SELECT Title AS videotitle, Channel_Name AS channelname, Likes AS likecount FROM videos
                where Likes is not null order by Likes desc'''
    cursor.execute(query5)
    
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5, columns=["Video Title","Channel name","Like count"])
    
    cursor.close()
    mydb.close()
    st.write(df5)
    st.bar_chart(df5.set_index('Video Title')['Like count'])

elif questions=="6.What is the total number of likes for each video, and what are their corresponding video names?":
    query6 = '''SELECT Title AS videotitle, Likes AS likecount FROM videos'''
    
    cursor.execute(query6)
    
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6, columns=["Video Title","Like count"])
    
    cursor.close()
    mydb.close()
    st.write(df6)
    st.area_chart(df6['Like count'])


elif questions=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    query7 = '''SELECT channel_name AS channelname, channel_viewcount AS totalviews FROM channels'''
    
    cursor.execute(query7)
    
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7, columns=["Channel name","Total views"])
    
    cursor.close()
    mydb.close()
    st.write(df7)
    st.bar_chart(df7.set_index('Channel name'))

elif questions=="8.What are the names of all the channels that have published videos in the year 2022?":
    query8 = '''SELECT Title AS videotitle, Published_Date AS videorelease, Channel_Name AS channelname FROM videos
                where extract(year from Published_Date)=2022'''
    
    cursor.execute(query8)
    
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8, columns=["Video title","Published date","Channel name"])
    
    cursor.close()
    mydb.close()
    st.write(df8)
    channel_counts = df8['Channel name'].value_counts()
    unique_channels = channel_counts.index.tolist()
    colors = ['skyblue', 'orange', 'green', 'red', 'purple']  # Define colors for bars
    
    # Map each channel to a color
    color_mapping = {channel: colors[i % len(colors)] for i, channel in enumerate(unique_channels)}
    
    # Create bar chart with specified colors
    plt.figure(figsize=(8, 6))
    for i, channel in enumerate(unique_channels):
        plt.bar(channel, channel_counts[channel], color=color_mapping[channel])
    
    plt.xlabel('Channel Name')
    plt.ylabel('Number of Videos')
    plt.title('Channels with Videos in 2022')
    plt.xticks(rotation=90) 
    
    st.pyplot(plt)

elif questions=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9 = '''SELECT Channel_Name AS channelname, SEC_TO_TIME(AVG(Duration)) AS averageduration FROM videos 
                GROUP BY Channel_Name'''
    
    cursor.execute(query9)
    
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns=["Channel name", "Average duration"])
    
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["Channel name"]
        average_durtion=row["Average duration"]
        average_duration_str=str(average_durtion)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
            
    cursor.close()
    mydb.close()
    st.write(df1)
    st.line_chart(df1.set_index('avgduration'))

elif questions=="10.Which videos have the highest number of comments and what are their corresponding channel names?":
    query10 = '''SELECT Title AS videotitle, Channel_Name AS channelname, Comments AS comments FROM videos 
                WHERE comments is not null ORDER BY Comments DESC '''
    
    cursor.execute(query10)
    
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10, columns=["Video Title", "Channel Name","Comments"])
            
    cursor.close()
    mydb.close()
    st.write(df10)
        
