--2016-02-23  First created
--            Exclude data of today for ease of Tableau incremental refresh
--


add jar /usr/lib/hive/lib/json-serde-1.3.1-SNAPSHOT-jar-with-dependencies.jar;
select a.*, b.Playlist_Title from 
(select dayid,concat(new_subsessionid,"|",dayid) as session_key,playlistid,publisher,show,asset_name,assetid,platform,     
sum(playback) as playbacks,sum(video_played_in_minute) as play_minutes,sum(num_follow) as num_follow,sum(num_unfollow) as num_unfollow,
sum(num_copyurlsharevideo) as num_copyurlsharevideo,sum(num_emailsharevideo) as num_emailsharevideo,sum(num_facebooksharevideo) as num_facebooksharevideo,sum(num_twittersharevideo) as num_twittersharevideo,
sum(num_copyurlshareplaylist) as num_copyurlshareplaylist,sum(num_emailshareplaylist) as num_emailshareplaylist,sum(num_facebookshareplaylist) as num_facebookshareplaylist,sum(num_twittershareplaylist) as num_twittershareplaylist
from aggregates.events_summ_hourly_playlist where dayid > "2015-12-31" and dayid<from_unixtime(unix_timestamp(),"yyyy-MM-dd") 
group by dayid,concat(new_subsessionid,"|",dayid),playlistid,publisher,show,asset_name,assetid,platform) a left join
(select playlist_key, max(split(title,'[=\;]')[1]) as Playlist_Title from watchable.playlists group by playlist_key) b on a.playlistid=b.playlist_key