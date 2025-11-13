# ChatGPT_Code_for_Library_Authority_Maintenance

This series of Python and Windows Batch files were created with ChatGPT and refactored with CodexGPT to automatically download, convert, and join authority file updates from the Library of Congress's daily LCSH and LCNAF activity feeds for import into (or deletion from) integrated library systems. 

download_LCNAF_activity_stream.bat - downloads daily LCNAF activity streams in JSON-LD (expressed as JSON)

download_LCSH_activity_stream.bat - downloads daily LCSH activity streams in JSON-LD (expressed as JSON)

LC_Activity_Streams_MARCXML_Downloader.py - harvests, downloads, converts, and joins MARC authority files to be imported into ILSes.

run_lc_activity_streams_marcxml_downloader.bat - initializes "LC_Activity_Streams_MARCXML_Downloader.py" from a Windows environment
