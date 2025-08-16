This is a simple program used to get data from Alpaca for use in AmiBroker backtesting.  It is for education purposes as an example of a way to get free data from Alpaca and no warranties of any kind are implied nor stated, use at your own risk.

BULLET POINTS FOR NEW USERS

1. This was made with the help of ChatGPT and Claude.ai in about 2 days, so there may be errors and it may not function in a polished way (feel free to fix it for yourself and share your suggestions with the repo and refer to the no warranty statement above).
2.  It assumes a free account with Alpaca, which you will need to set up and find out how to get your Alpaca key and Alpaca secret
3. As this downloader is for backtesting, it gets data upto the date before it is run.
4. It saves individual .CSV files with the name of each symbol as a separate file.
5. The saved files come in easy to import AmiBroker format.
6. You need to create a file called config_local.py or the script will not run.  (you can copy config_local_template.py and insert your API keys)
7. config_local.py is part of .gitignore (it will never sync with GitHub) and contains private data and personal customization variables
8. ***** PLEASE PAY EXTRA PAY ATTENTION TO THE NEXT LINE*****
9. ***** DO NOT PUT PRIVATE DATA ANYWHERE PUT your own version of config_local.py IF YOU INTEND TO SYNC SUGGESTIONS WITH THIS REPO. *******
10. ***** DID YOU READ THE ABOVE LINE?  PLEASE PROTECT YOUR DATA ******
11. This code gets all available symbols from Alpaca US Equities and filteres them to exclude non-tradable, non-shortable and inactive (you can modify the code if you want something different)
12. After sorting between 11,000 and 12,000 symbol names are returned by the API as of the date of this ReadMe, that is a LOT of 1 minute data.
13. I recommend setting the BASE_DATA_DIR in your config_local file to a NON_SYNCED (ex. not part of OneDrive, or a similar syncing system) folder on your hard drive, or you may wipe out your synced space.
14. The first 90 days of all sysmbols will comsume more than 20 GB of harddrive space.  If you want extended history, it will take a very large amount of hard drive space.
