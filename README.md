This is a simple program used to get data from Alpaca for use in AmiBroker backtesting.  It is for education purposes as an example of a way to get free data from Alpaca and no warrantees of any kind are implied nor stated, use at your own risk.

BULLET POINTS FOR NEW USERS

1. It assumes a free account with Alpaca, which you will need to set up and find out how to get your Alpaca key and Alpaca secret
2. As this downloader is for backtesting, it gets data upto the date before it is run.
3. It saves individual .CSV files with the name of each symbol as a separate file.
4. The saved files come in easy to import AmiBroker format.
5. You need to create a file called config_local.py or the script will not run.  (you can copy config_local_template.py and insert your API keys)
6. config_local.py is part of .gitignore (it will never sync with GitHub) and contains private data and personal customization variables
7. This code gets all available symbols from Alpaca US Equities and filteres them to exclude non-tradable, non-shortable and inactive (you can modify the code if you want something different)
8. After sorting between 11,000 and 12,000 symbol names are returned by the API as of the date of this ReadMe, that is a LOT of data.
9. I recommend setting the BASE_DATA_DIR in your config_local file to a NON_SYNCED (ex. not part of OneDrive, or a similar syncing system) folder on your hard drive, or you may wipe out your synced space.
10. The first 90 days of all sysmbols will comsume 8-10 GB of harddrive space.  If you want extended history, it will take a very large amount of hard drive space.
