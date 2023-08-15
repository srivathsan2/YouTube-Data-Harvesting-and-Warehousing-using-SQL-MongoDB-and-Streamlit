import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
from datetime import datetime
import pymongo
import pymysql
from googleapiclient.discovery import build
from PIL import Image
myconnect = pymysql.connect(host="127.0.0.1",user='root',passwd='sv2002..',database='project_f')
cue = myconnect.cursor() 
client=pymongo.MongoClient('mongodb://127.0.0.1:27017/')
db=client['project_f']
info = db.data
api_key = "AIzaSyCLEIdDgI1XV19udvFZIAMy8-dSwn4VCd0"
youtube = build('youtube', 'v3', developerKey=api_key)
def channel_data(channel_id):
    channel_data1=[]
    request = youtube.channels().list(part="snippet,contentDetails,statistics",id= channel_id)
    response = request.execute()
    channel_info ={'channel_name' : response['items'][0]['snippet']['title'],'channel_id':response['items'][0]['id'],'channel_description' : response['items'][0]['snippet']['description'],'channel_views': int(response['items'][0]['statistics']['viewCount']),'channel_subcribercount':int(response['items'][0]['statistics']['subscriberCount']),'playlists' : response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],'Total_videos' : int(response['items'][0]['statistics']['videoCount']) } 
    channel_data1.append(channel_info)
    return channel_data1
def playlist(channel_id):
    play=[]
    request = youtube.channels().list(part="snippet,contentDetails,statistics",id= channel_id)
    response = request.execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    play.append(playlist_id)
    return play
def video_id(channel_id):
    video_ids = []
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=25,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids
def vde(video_id):
    d=[]
    for i in video_id:
        request2 = youtube.videos().list(part="snippet,contentDetails,statistics",id=i)
        response2 = request2.execute()
            #pprint(response2)
        try : 
            video_details = dict(Channel_name = response2['items'][0]['snippet']['channelTitle'],Channel_id = response2['items'][0]['snippet']['channelId'],Video_id = response2['items'][0]['id'],Title = response2['items'][0]['snippet']['title'],
                                            Tags = response2['items'][0]['snippet'].get('tags'),
                                            Thumbnail = response2['items'][0]['snippet']['thumbnails']['default']['url'],
                                            Description = response2['items'][0]['snippet']['description'],
                                            Published_date = datetime.strptime(response2['items'][0]['snippet']['publishedAt'],"%Y-%m-%dT%H:%M:%SZ").year,
                                            Duration = response2['items'][0]['contentDetails']['duration'],
                                            Views = int(response2['items'][0]['statistics']['viewCount']),
                                            Likes = int(response2['items'][0]['statistics'].get('likeCount')),
                                            Comments = int(response2['items'][0]['statistics'].get('commentCount')),
                                            Favorite_count = int(response2['items'][0]['statistics']['favoriteCount']),
                                            Definition = response2['items'][0]['contentDetails']['definition'],
                                            Caption_status = response2['items'][0]['contentDetails']['caption'])
            d.append(video_details)
        except:
            pass

    return d 
def cmmt(video_id):
    cmt=[]
    try:
        for i in video_id:
            request3 = youtube.commentThreads().list(part="snippet,replies",videoId = i)
            response3 = request3.execute()
            for j in response3['items']:
                cmts=dict(video_id =j['snippet']['topLevelComment']['snippet']['videoId'], author = j['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                             publish = j['snippet']['topLevelComment']['snippet']['publishedAt'],
                             comments=j['snippet']['topLevelComment']['snippet']['textOriginal'],
                             likes = int(j['snippet']['topLevelComment']['snippet']['likeCount']))
                cmt.append(cmts)
    except:
        pass
    return cmt
def main(channel_id):
    c = channel_data(channel_id)
    vi = video_id(channel_id)
    v = vde(vi)
    cm = cmmt(vi)
    data = {'channel_info':c,'video_info':v,'comment_info':cm}
    return data

with st.sidebar:
    selected = option_menu(None, ["Home","Extract","View","Data"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "20px", "text-align": "centre", "margin": "2px", 
                                                "--hover-color": "#8e44ad"},
                                   "icon": {"font-size": "30px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#8e44ad "}})
if selected == "Home":
    col1,col2 = st.columns(2,gap= 'medium')
    col1.markdown("##### :blue[Area of project] : Youtube:")
    col1.markdown("##### :red[About the Project] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
    im = Image.open(r'C:\Users\hp\Downloads\08OnTech-YouTube-mediumSquareAt3X-v2.jpg')
    col2.image(im, width=300)
if selected == "Extract":
    st.write("#### Enter YouTube Channel ID:")
    ch_id = st.text_input("### Channel ID")
    datas = main(ch_id)
    if st.checkbox('Upload to mongoDB and sql'):
        info.insert_one(datas)
        st.write("The data has been inserted to the mongodb....")
        #if st.checkbox('upload to sql'):
        def da(a):
            for i in info.find({},{'id':0}):
                if i['channel_info'][0]['channel_id'] == a:
                    d=(i['channel_info'])
                    v=(i['video_info'])
                    cm=(i['comment_info'])
            di=pd.DataFrame(d)
            vi=pd.DataFrame(v)
            ci=pd.DataFrame(cm)
            return [di,vi,ci]
        b = da(ch_id)
        def in_ch(c):
            sql = "insert into channel_info(channel_name,channel_id,channel_description,channel_views,channel_subcribercount,playlists,video_count) values(%s,%s,%s,%s,%s,%s,%s)"
            for i in range(len(c)):
                r= tuple(c.iloc[i])
                cue.execute(sql,r)
                myconnect.commit()
        in_ch(b[0])
        def in_v(vi):
            sql1 = "insert into video_info (Channel_name, Channel_id, Video_id, Title, Tags, Thumbnail, Description, Published_date, Duration, Views, Likes, Comments, Favorite_count, Definition, Caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            for i in range(len(vi)):
                r1 = list(vi.iloc[i])
                r1[4] = str(r1[4])  
                r1 = tuple(r1)  
                cue.execute(sql1, r1)
            myconnect.commit()  
        in_v(b[1])
        def in_cm(ci):
            sql3 = "insert into comment_info(video_id,author,publish,comments,likes) values(%s,%s,%s,%s,%s)"
            for i in range(len(ci)):
                r= tuple(ci.iloc[i])
                cue.execute(sql3,r)
            myconnect.commit()
        in_cm(b[2])
        st.write('The data has been inserted to sql...')
if selected == 'View':
    st.write("## :blue[Select any question to get Insights]")
    questions = st.selectbox('Questions',('What are the names of all the videos and their corresponding channels?',
        'Which channels have the most number of videos, and how many videos do they have?',
        'What are the top 10 most viewed videos and their respective channels?',
        'How many comments were made on each video, and what are their corresponding video names?',
        'Which videos have the highest number of likes, and what are their corresponding channel names?',
        'What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
        'What is the total number of views for each channel, and what are their corresponding channel names?',
        'What are the names of all the channels that have published videos in the year 2022?',
        'What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        'Which videos have the highest number of comments, and what are their corresponding channel names?'))
    st.write(questions)
    if questions == 'What are the names of all the videos and their corresponding channels?':
        df = pd.read_sql_query("select Title,Channel_name from video_info order by Channel_name",myconnect)
        st.write(df)
    if questions == 'Which channels have the most number of videos, and how many videos do they have?':
        df = pd.read_sql_query("select video_count,channel_name from channel_info order by video_count desc",myconnect)
        st.write(df)
        fig = px.bar(df,x='channel_name',y='video_count',color='channel_name')
        st.plotly_chart(fig)
    if questions == 'What are the top 10 most viewed videos and their respective channels?':
        df = pd.read_sql_query("select Views , channel_name from video_info order by Views desc limit 10",myconnect)
        st.write(df)
        fig = px.bar(df,x='channel_name',y='Views',color='channel_name')
        st.plotly_chart(fig)
    if questions == 'How many comments were made on each video, and what are their corresponding video names?':
        df = pd.read_sql_query("select Comments,Title,Channel_name from video_info",myconnect)
        st.write(df)
        fig = px.bar(df,x='Channel_name',y='Comments',color ='Channel_name')
        st.plotly_chart(fig)
    if questions =='Which videos have the highest number of likes, and what are their corresponding channel names?':
        df = pd.read_sql_query("select Likes,Title, Channel_name from video_info order by Likes desc",myconnect)
        st.write(df)
        fig=px.bar(df,x='Channel_name',y='Likes',color='Channel_name')
        st.plotly_chart(fig)
    if questions =='What is the total number of views for each channel, and what are their corresponding channel names?':
        df = pd.read_sql_query("select channel_views,channel_name from channel_info order by channel_views desc",myconnect)
        st.write(df)
        fig = px.bar(df,x='channel_name',y='channel_views',color='channel_name')
        st.plotly_chart(fig)
    if questions == 'What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        df = pd.read_sql_query("select Likes,Title, Channel_name from video_info",myconnect)
        st.write(df)
        fig = px.bar(df,x='Channel_name',y='Likes',color='Channel_name')
        st.plotly_chart(fig)
    if questions == 'What are the names of all the channels that have published videos in the year 2022?':
        df = pd.read_sql_query("select Title, Channel_name ,Published_date from video_info where Published_date ='2022'",myconnect)
        st.write(df)
        df1 = pd.read_sql_query("select Channel_name,Published_date from video_info",myconnect)
        fig = px.bar(df1,x='Channel_name',y='Published_date',color = 'Channel_name')
        st.plotly_chart(fig)
    if questions == 'Which videos have the highest number of comments, and what are their corresponding channel names?':
        df = pd.read_sql_query("select Comments,Title,Channel_name from video_info order by Comments desc",myconnect)
        st.write(df)
        fig = px.bar(df,x='Channel_name',y='Comments',color = 'Channel_name')
        st.plotly_chart(fig)
#if selected == 'Data':
   # questions = st.selectbox('select the data',('channel data','video data','comment data'))
