with source as (
    select
        event_id,
        event_ts,
        org,
        gateway,
        data,
        JSONExtractArrayRaw(data, 'stats', 'track_stats') as tracks
    from naap.accepted_raw_events final
    where event_type = 'stream_ingest_metrics'
      and event_id != ''
)
select
    event_id,
    event_ts,
    org,
    gateway,
    JSONExtractString(data, 'stream_id') as stream_id,
    JSONExtractString(data, 'request_id') as request_id,
    JSONExtractString(data, 'stats', 'conn_quality') as conn_quality,
    if(video_track != '', JSONExtractFloat(video_track, 'jitter'), 0.) as video_jitter_ms,
    if(video_track != '', toUInt64OrDefault(toString(JSONExtractUInt(video_track, 'packets_lost'))), 0) as video_packets_lost,
    if(video_track != '', toUInt64OrDefault(toString(JSONExtractUInt(video_track, 'packets_received'))), 0) as video_packets_received,
    if(audio_track != '', JSONExtractFloat(audio_track, 'jitter'), 0.) as audio_jitter_ms,
    if(audio_track != '', toUInt64OrDefault(toString(JSONExtractUInt(audio_track, 'packets_lost'))), 0) as audio_packets_lost,
    if(audio_track != '', toUInt64OrDefault(toString(JSONExtractUInt(audio_track, 'packets_received'))), 0) as audio_packets_received,
    data
from (
    select
        *,
        arrayFirst(x -> JSONExtractString(x, 'type') = 'video', tracks) as video_track,
        arrayFirst(x -> JSONExtractString(x, 'type') = 'audio', tracks) as audio_track
    from source
)
