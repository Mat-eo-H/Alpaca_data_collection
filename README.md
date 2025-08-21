This is a simple program used to get data from Alpaca for use in AmiBroker backtesting.  It is for education purposes as an example of a way to get free data from Alpaca and no warranties of any kind are implied nor stated, use at your own risk.

BULLET POINTS FOR NEW USERS

1. This was made with the help of ChatGPT and Claude.ai and GitHub CoPilot $100/year version (Using ChatGPT), so there may be errors and it may not function in a polished way (feel free to fix it for yourself and share your suggestions with the repo and refer to the no warranty statement above).
    - It is working on my machine.  As of this update I have about 111 GB of data acquired over about 3 days of letting it run.
2. This bot assumes a free account with Alpaca, which you will need to set up and find out how to get your Alpaca key and Alpaca secret
    - As this downloader is for backtesting, and assumes a free account it will only get "yesterday" and older data.
4. It saves individual .CSV files with the name of each symbol as a separate file.  This makes it easy to import to AmiBroker.
    - When running the import wizard in AmiBroker, change the first column to Date and the second column to Time.
    - if you want number of trades in the bar and the VWAP of the bar (the average price of THAT bar, not the VWAP from the beginning of the day) you can add columns and put them say in "Aux1" and "Aux2"
6. You need to create a file called config_local.py or the script will not run.  (you can copy config_local_template.py and insert your API keys)
7. config_local.py is part of .gitignore (it will never sync with GitHub) and once you updated it will contain YOUR private data and personal customization variables (which is why it doesn't sync)
8. ***** PLEASE PAY EXTRA PAY ATTENTION TO THE ABOVE LINE - do not update config_local_template.py, only your own copy of config_local.py *****
9. ***** DO NOT PUT PRIVATE DATA ANYWHERE BUT your own version of config_local.py IF YOU INTEND TO SYNC SUGGESTIONS WITH THIS REPO. *******
10. ***** DID YOU READ THE ABOVE LINE?  PLEASE PROTECT YOUR DATA and DON'T update config_local_template.py by ACCIDENT.  IF you do, go to Alpaca and regenerate your API keys ******
11. This code gets all available symbols from Alpaca US Equities and filteres them to exclude non-tradable, non-shortable and inactive (you can modify the code if you want something different)
12. After sorting between 11,000 and 12,000 symbol names are returned by the API as of the date of this ReadMe, that is a LOT of 1 minute data.
13. I recommend setting the BASE_DATA_DIR in your config_local file to a NON_SYNCED (ex. not part of OneDrive, or a similar syncing system) folder on your hard drive, or you may wipe out your synced space and make other files you want to sync slower.
14. my current file list has accumulated about 3 years of data and is about 111 GB.  If you want extended history, it will take a very large amount of hard drive space.
