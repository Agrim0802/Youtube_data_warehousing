import streamlit as st
import googleapiclient.discovery
import mysql.connector as sql
import pandas as pd
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey='AIzaSyCKj-VBpZ7zpfBNTnTaAs8Ya4zVLwF2Zrk')

#connecting to SQL server
def create_db():
    mydb= sql.connect(host='localhost',
                  port='3307',
                  user='root',
                  password='08021999',
                  )
    mycursor1 = mydb.cursor()
    mycursor1.execute("CREATE DATABASE IF NOT EXISTS YouTube_Data")
    mydb.commit()

def create_tables():
    mydb1=sql.connect(host='localhost',
                  port='3307',
                  user='root',
                  password='08021999',
                  database='YouTube_Data'
                  )
    mycursor = mydb1.cursor()

    mycursor.execute("""create table if not exists channel_data( 
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    channel_ID varchar(255) UNIQUE,
                    channel_name VARCHAR(255) NOT NULL,
                    subscribers int,
                    description text,
                    total_videos int)""")
    mycursor.execute("""create table if not exists video_data(
                    video_id varchar(255) unique,
                    channel_id varchar(255),
                    video_name VARCHAR(255) NOT NULL,
                    video_description text,
                    tags text,
                    publish_date varchar(255),
                    video_type varchar(255),
                    video_views int,
                    like_count int,
                    comment_count int)""")
    mycursor.execute("""create table if not exists comments_data(
                    comment_ID varchar(255) UNIQUE,
                    video_ID varchar(255), 
                    comment_text text,
                    comment_author varchar(255),
                    comment_date varchar(255)
                     )""")
    mydb1.commit()


def save_channel_data(youtube,channel_id):
     mydb1=sql.connect(host='localhost',
                  port='3307',
                  user='root',
                  password='08021999',
                  database='YouTube_Data'
                  )
     mycursor = mydb1.cursor()
     Channel_data = get_youtube_data(youtube,channel_id)
     insert_query = '''REPLACE INTO channel_data (channel_ID, channel_name, subscribers, description,total_videos)
                    values(%s,%s,%s,%s,%s)'''  # ON DUPLICATE KEY UPDATE(channel_ID=%s)       
     mycursor.execute(insert_query,(Channel_data['channel_id'],Channel_data['channel_name'],Channel_data['sub_count']
                                   ,Channel_data['description'],Channel_data['video_count']))
     mydb1.commit()

def save_video_data(channel_id):
      mydb1=sql.connect(host='localhost',
                  port='3307',
                  user='root',
                  password='08021999',
                  database='YouTube_Data'
                  )
      mycursor = mydb1.cursor()
      video_detail = get_videos_ID(channel_id)
      insert_query = '''REPLACE INTO video_data (video_id,channel_id, video_name, video_description, tags, publish_date,
                        video_views,like_count, comment_count)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
      mycursor.execute(insert_query,(video_detail[0][0],video_detail[0][8],video_detail[0][1],video_detail[0][2],video_detail[0][3],video_detail[0][4],
                                   video_detail[0][5],video_detail[0][6],video_detail[0][7]))
      mycursor.execute(insert_query,(video_detail[1][0],video_detail[1][8],video_detail[1][1],video_detail[1][2],video_detail[1][3],video_detail[1][4],
                                   video_detail[1][5],video_detail[1][6],video_detail[1][7]))
      mydb1.commit()

def save_comment_data(channel_id):
    mydb1=sql.connect(host='localhost',
                  port='3307',
                  user='root',
                  password='08021999',
                  database='YouTube_Data'
                  )
    mycursor = mydb1.cursor()
    video_detail = get_videos_ID(channel_id)

    insert_query = '''insert INTO comments_data (comment_ID, video_ID, comment_text, comment_author, comment_date)
                    values(%s,%s,%s,%s,%s)'''
    mycursor.execute(insert_query,(video_detail[0][9][0],video_detail[0][0],video_detail[0][9][1],video_detail[0][9][2],video_detail[0][9][3]))
    mycursor.execute(insert_query,(video_detail[0][9][4],video_detail[0][0],video_detail[0][9][5],video_detail[0][9][6],video_detail[0][9][7]))
    mycursor.execute(insert_query,(video_detail[1][9][0],video_detail[1][0],video_detail[1][9][1],video_detail[1][9][2],video_detail[1][9][3]))
    mycursor.execute(insert_query,(video_detail[1][9][4],video_detail[1][0],video_detail[1][9][5],video_detail[1][9][6],video_detail[1][9][7]))
    mydb1.commit()

# Function to get the YouTube data based on the input ID
def get_youtube_data(youtube,youtube_id):
    request = youtube.channels().list( part="snippet,contentDetails,statistics,status",
                                      id=youtube_id )
    response = request.execute()
    Channel_data=dict(
                  channel_name=response['items'][0]['snippet']['title'],
                  channel_id=response['items'][0]['id'],
                  sub_count=response['items'][0]['statistics']['subscriberCount'],
                  description=response['items'][0]['snippet']['description'],
                  video_count=response['items'][0]['statistics']['videoCount'],
                  icon=response['items'][0]['snippet']['thumbnails']['default']['url']
    )
    
    return Channel_data


def get_videos_ID(channel_id):
    # Retrieve the latest two videos from the channel
    video_response = youtube.search().list(
        part='snippet',
        channelId=channel_id,
        maxResults=2,  # Get the latest two videos
        order='date'  # Order by upload date
    ).execute()
    video_ids=[]
    result=[]
    for item in video_response['items']:
        video_id = item['id']['videoId']
        video_ids.append(video_id)
    print(video_ids)

    for item in video_ids:
      result.append(get_videos_and_comments(item))
    
    return result   

def get_videos_and_comments(video_id):
  request = youtube.videos().list(
          part="snippet,statistics,status",id=video_id
          )
  response = request.execute()
  video_data_with_comments = []

  for item in response['items']:
    video_id = item['id']
    video_title = item['snippet']['title']
    video_description = item['snippet']['description']
    category_id= item['snippet']['categoryId']
    published_at =item['snippet']['publishedAt']
    view_count =item['statistics']['viewCount']
    like_count =item['statistics']['likeCount']
    comment_count =item['statistics']['commentCount']
    channel_ID= item['snippet']['channelId']
    video_data_with_comments.append(video_id)
    video_data_with_comments.append(video_title)
    video_data_with_comments.append(video_description)
    video_data_with_comments.append(category_id)
    video_data_with_comments.append(published_at)
    video_data_with_comments.append(view_count)
    video_data_with_comments.append(like_count)
    video_data_with_comments.append(comment_count)
    video_data_with_comments.append(channel_ID)
    # Retrieve comments for the video
    comment_response = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=2  # Get the latest two comments
            ).execute()

    comments = []
    for comment_item in comment_response['items']:
      comment_Id= comment_item['snippet']['topLevelComment']['id']
      comment_Description = comment_item['snippet']['topLevelComment']['snippet']['textDisplay']
      comment_author= comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName']
      PublishedAt= comment_item['snippet']['topLevelComment']['snippet']['publishedAt']
      comments.append(comment_Id)
      comments.append(comment_Description)
      comments.append(comment_author)
      comments.append(PublishedAt)
        # Store video data with comments 
    video_data_with_comments.append(comments)

    
  return video_data_with_comments
  
                
# Streamlit application
def main():
    # Set the title of the application
    st.title(":red[YouTube Data Harvesting]")

    st.markdown('<img src="https://upload.wikimedia.org/wikipedia/commons/0/09/YouTube_full-color_icon_%282017%29.svg" alt="YouTube Icon" width="100">', unsafe_allow_html=True)

    # Create a text input box to take YouTube ID as input
    youtube_id = st.text_input("Enter YouTube ID:")
    

    # Create a button to trigger data fetching
    if st.button("Search"):
        # Fetch data based on the provided YouTube ID
        youtube_data=get_youtube_data(youtube,youtube_id)
        st.subheader(youtube_data["channel_name"])
        st.image(youtube_data["icon"])

    #     Display fetched data
        st.subheader(":blue[Channel ID:]:id:")
        st.write(youtube_data["channel_id"])

        st.subheader(":blue[Channel_Description:]:book:")
        st.write(youtube_data["description"])

        st.subheader(":blue[subscribers:]:performing_arts:")
        st.write(youtube_data["sub_count"])

        st.subheader(":blue[videos in channel:]:movie_camera:")
        st.write(youtube_data["video_count"])
        st.divider()
        st.subheader(":green[videos Details:]")

        tab1, tab2 = st.tabs(["video1:video_camera:", "video2:video_camera:"])

        video_detail=get_videos_ID(youtube_id)
        with tab1:
           st.header(video_detail[0][1]) # title of video1
           with st.expander("***:blue[video_id]***"):
                st.text(video_detail[0][0] )
           with st.expander(":blue[description]"):
              st.text(video_detail[0][2] )
           with st.expander(":blue[category_id]"):
                st.text(video_detail[0][3] )
           with st.expander(":blue[published at]"):
                st.text(video_detail[0][4] )
           with st.expander(":blue[view count]"):
                st.text(video_detail[0][5] )
           with st.expander(":blue[Like count]"):
                st.text(video_detail[0][6] )
           with st.expander(":blue[comment count]"):
                st.text(video_detail[0][7] )
           st.divider()
           tab3, tab4 = st.tabs(["comment1:pencil:", "comment2:pencil:"])
           with tab3:
               st.write(":orange[commentid:]",video_detail[0][9][0])
               st.write(":orange[comment:]",video_detail[0][9][1])
               st.write(":orange[comment_author:]",video_detail[0][9][2])
               st.write(":orange[comment_publishdate:]",video_detail[0][9][3])
           with tab4:
               st.write(":orange[commentid:]",video_detail[0][9][4])
               st.write(":orange[comment:]",video_detail[0][9][5])
               st.write(":orange[comment_author]",video_detail[0][9][6])
               st.write(":orange[comment_publishdate]",video_detail[0][9][7])

        with tab2:
            st.header(video_detail[1][1]) #title of video2
            with st.expander(":blue[video_id]"):
             st.text(video_detail[1][0] )
            with st.expander(":blue[description]"):
                st.text(video_detail[1][2] )
            with st.expander(":blue[tags]"):
                st.text(video_detail[1][3] )
            with st.expander(":blue[published at]"):
                st.text(video_detail[1][4] )
            with st.expander(":blue[view count]"):
                st.text(video_detail[1][5] )
            with st.expander(":blue[Like count]"):
                st.text(video_detail[1][6] )
            with st.expander(":blue[comment count]"):
                st.text(video_detail[1][7] )
            st.divider()
            tab3, tab4 = st.tabs(["comment1:pencil:", "comment2:pencil:"])
            with tab3:
                st.write(":orange[commentid]",video_detail[1][9][0])
                st.write(":orange[comment]",video_detail[1][9][1])
                st.write(":orange[comment_author]",video_detail[1][9][2])
                st.write(":orange[comment_publishdate]",video_detail[1][9][3])
            with tab4:
                st.write(":orange[commentid]",video_detail[1][9][4])
                st.write(":orange[comment]",video_detail[1][9][5])
                st.write(":orange[comment_author]",video_detail[1][9][6])
                st.write(":orange[comment_publishdate]",video_detail[1][9][7])   
        # saving data in SQL
    if st.button("Save Data"):      
        create_db()
        create_tables()
        save_channel_data(youtube,youtube_id)
        save_video_data(youtube_id)
        save_comment_data(youtube_id)
        st.success("Your Youtube data is saved successfully!")

    selected_id = st.selectbox("Select a YouTube Video ID:", options= ("1 What are the names of all the videos and their corresponding channels? ",
                                                                       "2 Which channels have the most number of videos, and how many videos do they have?",
                                                                       "3 What are the top 10 most viewed videos and their respective channels?",
                                                                       "4 How many comments were made on each video, and what are their corresponding video names?",
                                                                       "5 Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                                       "6 What is the total number of likes for each video, and what are their corresponding video names?",
                                                                       "7 What is the total number of subcribers for each channel, and what are their corresponding channel names?",
                                                                       "8 What are the names of all the channels that have published videos in the year 2024?",
                                                                       "9 Which videos have the highest number of comments, and what are their corresponding channel names?"
                                                                       ), placeholder="Select the output you want..", index=None)
    mydb1=sql.connect(host='localhost',
                  port='3307',
                  user='root',
                  password='08021999',
                  database='YouTube_Data'
                  )
    
    
    if selected_id=="1 What are the names of all the videos and their corresponding channels? ":
        df= pd.read_sql_query('select video_name, channel_name from video_data inner join channel_data on video_data.channel_id = channel_data.channel_id',mydb1)
        st.dataframe(df)

    if selected_id=="2 Which channels have the most number of videos, and how many videos do they have?":
        df= pd.read_sql_query('select channel_name, total_videos from channel_data where total_videos =(select max(total_videos) from channel_data)',mydb1)
        st.dataframe(df)

    if selected_id=="3 What are the top 10 most viewed videos and their respective channels?":
        df= pd.read_sql_query('select video_name, channel_name from video_data inner join channel_data on video_data.channel_id = channel_data.channel_id order by video_views desc limit 10',mydb1)
        st.dataframe(df)

    if selected_id=="4 How many comments were made on each video, and what are their corresponding video names?":
        df= pd.read_sql_query('select video_name, comment_count from video_data',mydb1)
        st.dataframe(df)

    if selected_id=="5 Which videos have the highest number of likes, and what are their corresponding channel names?":
        df= pd.read_sql_query('select channel_name, video_name, like_count from video_data inner join channel_data on video_data.channel_id = channel_data.channel_id where like_count =(select max(like_count) from video_data)',mydb1)
        st.dataframe(df)

    if selected_id=="6 What is the total number of likes for each video, and what are their corresponding video names?":
        df= pd.read_sql_query('select video_name,like_count from video_data',mydb1)
        st.dataframe(df)
    
    if selected_id=="7 What is the total number of subcribers for each channel, and what are their corresponding channel names?":
        df= pd.read_sql_query('select channel_name , subscribers from channel_data',mydb1)
        st.dataframe(df)

    if selected_id=="8 What are the names of all the channels that have published videos in the year 2024?":
        df= pd.read_sql_query("select channel_name , video_name ,publish_date from video_data inner join channel_data on video_data.channel_id = channel_data.channel_id where publish_date between '2024-01-01T00:00:00Z' and '2024-12-31T23:59:59Z'",mydb1)
        st.dataframe(df)

    if selected_id=="9 Which videos have the highest number of comments, and what are their corresponding channel names?":
        df= pd.read_sql_query('select channel_name , video_name from video_data inner join channel_data on video_data.channel_id = channel_data.channel_id where comment_count =(select max(comment_count) from video_data)',mydb1)
        st.dataframe(df)

# Run the application
if __name__ == "__main__":
    main()