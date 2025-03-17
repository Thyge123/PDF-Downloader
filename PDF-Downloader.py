def main():
    # Import necessary libraries
    import pandas as pd
    import glob
    import os
    import os.path
    from urllib.request import urlretrieve
    import threading
    import time

    # Define path to excel file
    path = 'Data\\GRI_2017_2020 (1).xlsx'

    # Path to metadata excel file
    metadata_path = 'Data\\Metadata2006_2016.xlsx'

    # Define path to save the files
    path_to_save = 'Data\\Downloads'

    # Define path for output files
    output_path = 'Data\\Output'

    # Define ID column
    ID_COLUMN = 'BRnum'

    # Read excel file with sustainability reports index
    reports_data = pd.read_excel(path, sheet_name=0, index_col=ID_COLUMN)

    # Function to get list of already downloaded PDF files
    def get_existing_downloads():
        """Return list of IDs for PDF files that already exist in the download folder"""
        downloaded_files = glob.glob(os.path.join(path_to_save, "*.pdf")) 
        existing_ids = [os.path.basename(f)[:-4] for f in downloaded_files]
        return existing_ids
    
    # Filter out rows with no valid download URL
    has_valid_url = (reports_data.Pdf_URL.notnull()) | (reports_data['Report Html Address'].notnull())
    reports_data = reports_data[has_valid_url]
    
    # Make a working copy for download processing
    download_queue = reports_data.copy()

    # Remove files that have already been downloaded
    existing_downloads = get_existing_downloads()
    to_download = [idx for idx in download_queue.index if idx not in existing_downloads]
    download_queue = download_queue.loc[to_download]

    # Limit batch size to prevent overloading
    MAX_DOWNLOADS = 10
    download_queue = download_queue.head(MAX_DOWNLOADS)

    # Track download errors
    downloadError = []

    # Function to download a single PDF file (for threading)
    def download_file(index, row):
        success = False
        try:
            if pd.notna(row['Pdf_URL']):
                url = row['Pdf_URL']
            else:
                url = row['Report Html Address']
            urlretrieve(url, os.path.join(path_to_save, f"{index}.pdf"))
            success = True
        except Exception as e:
            downloadError.append(index)
            downloadError.append(e)
            print(f"Error downloading {index}: {e}")
        finally:
            if success:
                print(f"Downloaded {index}")
            else:
                print(f"Failed to download {index}")


    # Download PDF files using multiple threads
    def download_pdf():
        # Create directories if they don't exist
        os.makedirs(path_to_save, exist_ok=True)
        os.makedirs(output_path, exist_ok=True)
        
        print(f"Starting download of {len(download_queue)} files...")
        
        # Maximum number of concurrent threads
        MAX_CONCURRENT = 5
        
        # Create and start threads for each download
        threads = []
        for index, row in download_queue.iterrows():
            # Create a thread for this download
            thread = threading.Thread(
                target=download_file,
                args=(index, row),
                name=f"Download-{index}"
            )
            threads.append(thread)
            
            # Start the thread
            thread.start()
            
            # If we've reached the max concurrent downloads, wait for one to finish
            active_threads = [t for t in threads if t.is_alive()]
            if len(active_threads) >= MAX_CONCURRENT:
                # Wait for at least one thread to finish before continuing
                for t in active_threads:
                    if len(active_threads) <= MAX_CONCURRENT - 1:
                        break
                    t.join(0.5)  # Wait up to 0.5 seconds
                    active_threads = [t for t in threads if t.is_alive()]
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
            
        print("All downloads finished")

    download_pdf()

    print("All downloads finished")

    # Make new excel file with overview of successful and failed downloads
    def create_output():
        print("Creating download status file...")
    output = []
    for index, row in download_queue.iterrows():
        if os.path.exists(os.path.join(path_to_save, f"{index}.pdf")):
            output.append([index, "Downloaded", ""])
        else:
            output.append([index, "Failed", downloadError[downloadError.index(index) + 1] if index in downloadError else "File not found"])
   
    output_df = pd.DataFrame(output, columns=["Index", "Status", "Error"])
    output_df.to_excel(os.path.join(output_path, "Download_Status.xlsx"), index=False)
    print('Download status file created and saved to ' + os.path.join(output_path, 'Download_Status.xlsx'))

    create_output()

    # Update metadata sheet with newly downloaded files
    def update_metadata():
        print("Updating metadata...")
        metadataFile = pd.read_excel(metadata_path, sheet_name=0)
    
        # Get list of successfully downloaded files
        downloaded_files = get_existing_downloads()
    
        # Create a new DataFrame for metadata updates
        new_records = []
    
        # For each file we attempted to download, create a new record
        for idx in download_queue.index:
            status = 'Yes' if str(idx) in downloaded_files else 'No'
            # Create a record with the same columns as the metadata file
            new_record = {ID_COLUMN: idx, 'pdf_downloaded': status}
            # Copy any other columns from the source file that we want to preserve
            if idx in reports_data.index:
                for col in reports_data.columns:
                    if col in metadataFile.columns and col not in new_record:
                        new_record[col] = reports_data.loc[idx, col]
        
            new_records.append(new_record)
    
        # Create DataFrame from new records
        new_data = pd.DataFrame(new_records)
    
        # Append the new data to the existing metadata
        updated_metadata = pd.concat([metadataFile, new_data], ignore_index=True)
    
        # Remove duplicates if any, keeping the latest entry
        updated_metadata.drop_duplicates(subset=[ID_COLUMN], keep='last', inplace=True)
    
        # Save the updated metadata to a new file
        updated_metadata.to_excel(os.path.join(output_path, "updated_metadata.xlsx"), index=False)
        print('Metadata updated and saved to ' + os.path.join(output_path, 'updated_metadata.xlsx'))
    
    update_metadata()

if __name__ == '__main__':
    main()