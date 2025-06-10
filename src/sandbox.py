import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timezone
import streamlit as st
 
chunk_size = 100
max_workers = 10  # Number of parallel threads
comparison_results = []

# Define function to fetch series metadata
def fetch_series_metadata(series_id, dt_utc):

    try:
        series = Ceic.series_metadata(series_id)
        series_last_update_time = series.data[0].metadata.last_update_time

        # Ensure timezone-aware
        if series_last_update_time.tzinfo is None:
            series_last_update_time = series_last_update_time.replace(tzinfo=timezone.utc)
 
        is_larger_than_process_report_time = series_last_update_time >= dt_utc
 
        return {
            "Series_id": series_id,
            "Last_Update_Time": series_last_update_time,
            "Update_greater_than_report_time": is_larger_than_process_report_time,
        }
 
    except Exception as e:
        return {
            "Series_id": series_id,
            "Last_Update_Time": "Error",
            "Update_greater_than_report_time": "Error",
            "Error_Message": str(e),
        }
 
# Process in chunks
total_series = len(series_ids)
for chunk_start in range(0, total_series, chunk_size):
    chunk = series_ids[chunk_start:chunk_start + chunk_size]
    st.markdown(f"🔄 Processing series {chunk_start + 1} to {min(chunk_start + chunk_size, total_series)}")
 
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_series_metadata, sid, dt_utc): sid for sid in chunk}
        for future in as_completed(futures):
            result = future.result()
            comparison_results.append(result)
 
# Build final DataFrame
comparison_df = pd.DataFrame(comparison_results)
comparison_df["Status"] = comparison_df["Update_greater_than_report_time"].apply(lambda x: "✅" if x is True else "❌")
 
# Count updated series
timepointupdated_series_count = comparison_df[comparison_df["Update_greater_than_report_time"] == True].shape[0]
 
# Display results
mismatched_df = comparison_df[comparison_df["Status"] == "❌"].drop(columns=["Update_greater_than_report_time"])
st.markdown("**🚨Series NOT updated within 3 days:**")
st.dataframe(mismatched_df)
 