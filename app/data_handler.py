import pandas as pd
import os
from typing import Optional, List, Union
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

NY_TZ = ZoneInfo("America/New_York")


class DataHandler:
    """
    A class to handle CSV file operations and data manipulation.
    Designed to work with relative paths from the project root.
    """
    
    def __init__(self, data_folder: str = "data"):
        """
        Initialize DataHandler with a default data folder relative to project root.
        
        Parameters:
        -----------
        data_folder : str
            The folder where CSV files are stored (relative to project root)
        """
        # Get the project root directory (where main.py is located)
        # This ensures consistent relative paths regardless of where the script is run from
        if __name__ == "__main__":
            # If running this file directly
            project_root = Path(__file__).parent.parent
        else:
            # If imported from another module
            # Look for main.py or setup.py to identify project root
            current_dir = Path(__file__).parent
            project_root = current_dir.parent
            
            # Alternative: try to find project root by looking for common project files
            while project_root != project_root.parent:
                if any((project_root / marker).exists() for marker in ['main.py', 'setup.py', '.git', 'requirements.txt']):
                    break
                project_root = project_root.parent
            else:
                # Fallback to parent directory of this module
                project_root = Path(__file__).parent.parent
        
        self.project_root = project_root
        self.data_folder = project_root / data_folder
        self.data_folder.mkdir(exist_ok=True)  # Create folder if it doesn't exist
        
        print(f"📁 Project root: {self.project_root}")
        print(f"📁 Data folder: {self.data_folder}")
    
    def load_csv(
        self, 
        filename: str, 
        encoding: str = 'utf-8',
        parse_dates: Optional[List[str]] = None,
        date_parser: Optional[str] = None,
        index_col: Optional[Union[str, int]] = None
    ) -> pd.DataFrame:
        """
        Load a CSV file and return as pandas DataFrame.
        
        Parameters:
        -----------
        filename : str
            Name of the CSV file (with or without .csv extension)
        encoding : str, optional
            File encoding (default: 'utf-8')
        parse_dates : List[str], optional
            List of column names to parse as dates
        date_parser : str, optional
            Date format string (e.g., '%Y-%m-%d')
        index_col : str or int, optional
            Column to use as index
        
        Returns:
        --------
        pd.DataFrame
            The loaded DataFrame
        """
        try:
            # Add .csv extension if not present
            if not filename.endswith('.csv'):
                filename += '.csv'
            
            file_path = self.data_folder / filename
            
            # Check if file exists
            if not file_path.exists():
                print(f"❌ File not found: {file_path}")
                return pd.DataFrame()
            
            # Load CSV with various options
            kwargs = {
                'encoding': encoding,
                'index_col': index_col
            }
            
            # Handle date parsing
            if parse_dates:
                kwargs['parse_dates'] = parse_dates
                if date_parser:
                    kwargs['date_format'] = date_parser
            
            df = pd.read_csv(file_path, **kwargs)
            
            print(f"✅ Successfully loaded {filename}")
            print(f"   Shape: {df.shape}")
            print(f"   Columns: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            print(f"❌ Error loading {filename}: {e}")
            return pd.DataFrame()
    
    def save_csv(
        self, 
        df: pd.DataFrame, 
        filename: str, 
        index: bool = False,
        encoding: str = 'utf-8'
    ) -> bool:
        """
        Save DataFrame to CSV file.
        
        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame to save
        filename : str
            Name of the CSV file (with or without .csv extension)
        index : bool, optional
            Whether to include index in saved file (default: False)
        encoding : str, optional
            File encoding (default: 'utf-8')
        
        Returns:
        --------
        bool
            True if successful, False otherwise
        """
        try:
            # Add .csv extension if not present
            if not filename.endswith('.csv'):
                filename += '.csv'
            
            file_path = self.data_folder / filename
            
            df.to_csv(file_path, index=index, encoding=encoding)
            
            print(f"✅ Successfully saved {filename}")
            print(f"   Shape: {df.shape}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error saving {filename}: {e}")
            return False
    
    def list_csv_files(self) -> List[str]:
        """
        List all CSV files in the data folder.
        
        Returns:
        --------
        List[str]
            List of CSV filenames
        """
        try:
            csv_files = [f.name for f in self.data_folder.glob("*.csv")]
            
            if csv_files:
                print(f"📁 Found {len(csv_files)} CSV files:")
                for i, file in enumerate(csv_files, 1):
                    print(f"   {i}. {file}")
            else:
                print("📁 No CSV files found in data folder")
            
            return csv_files
            
        except Exception as e:
            print(f"❌ Error listing files: {e}")
            return []
    
    def get_file_info(self, filename: str) -> dict:
        """
        Get basic information about a CSV file.
        
        Parameters:
        -----------
        filename : str
            Name of the CSV file
        
        Returns:
        --------
        dict
            Dictionary with file information
        """
        try:
            if not filename.endswith('.csv'):
                filename += '.csv'
            
            file_path = self.data_folder / filename
            
            if not file_path.exists():
                return {"error": "File not found"}
            
            # Get file stats
            stat = file_path.stat()
            
            # Load just the first few rows to get column info
            df_sample = pd.read_csv(file_path, nrows=5)
            
            info = {
                "filename": filename,
                "file_size_mb": round(stat.st_size / (1024 * 1024), 2),
                "columns": list(df_sample.columns),
                "num_columns": len(df_sample.columns),
                "data_types": df_sample.dtypes.to_dict(),
                "sample_data": df_sample.head(3).to_dict('records')
            }
            
            return info
            
        except Exception as e:
            return {"error": str(e)}
    
    def load_multiple_csv(
        self, 
        filenames: List[str], 
        concat: bool = False
    ) -> Union[List[pd.DataFrame], pd.DataFrame]:
        """
        Load multiple CSV files.
        
        Parameters:
        -----------
        filenames : List[str]
            List of CSV filenames to load
        concat : bool, optional
            Whether to concatenate all DataFrames into one (default: False)
        
        Returns:
        --------
        Union[List[pd.DataFrame], pd.DataFrame]
            List of DataFrames or single concatenated DataFrame
        """
        dataframes = []
        
        for filename in filenames:
            df = self.load_csv(filename)
            if not df.empty:
                dataframes.append(df)
        
        if concat and dataframes:
            try:
                combined_df = pd.concat(dataframes, ignore_index=True)
                print(f"✅ Combined {len(dataframes)} files into single DataFrame")
                print(f"   Final shape: {combined_df.shape}")
                return combined_df
            except Exception as e:
                print(f"❌ Error combining DataFrames: {e}")
                return dataframes
        
        return dataframes

# Convenience functions for direct import
def load_csv(filename: str, data_folder: str = "data", **kwargs) -> pd.DataFrame:
    """
    Quick function to load a CSV file with automatic project root detection.
    
    Parameters:
    -----------
    filename : str
        Name of the CSV file
    data_folder : str, optional
        Folder containing the CSV file relative to project root (default: 'data')
    **kwargs
        Additional arguments passed to pandas.read_csv()
    
    Returns:
    --------
    pd.DataFrame
        The loaded DataFrame
    """
    handler = DataHandler(data_folder)
    return handler.load_csv(filename, **kwargs)

def save_csv(df: pd.DataFrame, filename: str, data_folder: str = "data", **kwargs) -> bool:
    """
    Quick function to save a DataFrame to CSV with automatic project root detection.
    
    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame to save
    filename : str
        Name of the CSV file
    data_folder : str, optional
        Folder to save the CSV file relative to project root (default: 'data')
    **kwargs
        Additional arguments passed to pandas.to_csv()
    
    Returns:
    --------
    bool
        True if successful, False otherwise
    """
    handler = DataHandler(data_folder)
    return handler.save_csv(df, filename, **kwargs)

def get_project_root() -> Path:
    """
    Get the project root directory by looking for common project markers.
    
    Returns:
    --------
    Path
        Path to the project root directory
    """
    current_dir = Path(__file__).parent
    project_root = current_dir.parent
    
    # Look for common project files to identify root
    markers = ['main.py', 'setup.py', '.git', 'requirements.txt', 'pyproject.toml', 'README.md']
    
    while project_root != project_root.parent:
        if any((project_root / marker).exists() for marker in markers):
            return project_root
        project_root = project_root.parent
    
    # Fallback to parent directory of this module
    return Path(__file__).parent.parent

# Example usage and testing
if __name__ == "__main__":
    # Create a DataHandler instance
    handler = DataHandler("data")
    
    # List available CSV files
    files = handler.list_csv_files()
    
    # Example: Load a specific file (replace with your actual filename)
    if files:
        # Load the first CSV file found
        df = handler.load_csv(files[0])
        
        if not df.empty:
            print(f"\n📊 Data Preview:")
            print(df.head())
            
            print(f"\n📋 Data Info:")
            print(f"Shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            print(f"Data Types:\n{df.dtypes}")
    else:
        print("No CSV files found. Please add some CSV files to the 'data' folder.")

# For saving historical data
def save_bars_to_csv(df, symbol, data_dir):
    """
    Save bars DataFrame to CSV. Handles MultiIndex from Alpaca-py.
    Adds 'date' and 'time' columns. Appends to existing CSV if it exists.
    """
    if df.empty:
        return

    df = df.copy()

    # Handle MultiIndex: extract 'timestamp' level if exists
    if isinstance(df.index, pd.MultiIndex):
        if 'timestamp' in df.index.names:
            df.index = df.index.get_level_values('timestamp')
        else:
            # fallback: use last level as datetime
            df.index = df.index.get_level_values(-1)

    # Convert timestamps to New York time
    df.index = df.index.tz_convert(NY_TZ)

    # Extract date and time columns
    df['date'] = df.index.date
    df['time'] = df.index.time

    # Reset index for CSV
    df.reset_index(drop=False, inplace=True)

    # Reorder columns: date, time, standard OHLCV, then extras
    cols = ['date', 'time', 'open', 'high', 'low', 'close', 'volume','trade_count','vwap']
    extra_cols = [c for c in df.columns if c not in cols]
    df = df[cols + extra_cols]

    # CSV file path
    file_path = os.path.join(data_dir, f"{symbol}.csv")

    # Append to existing CSV without duplicates
    if os.path.exists(file_path):
        existing_df = pd.read_csv(file_path)
        df = pd.concat([existing_df, df])
        df = df.drop_duplicates(subset=['date', 'time']).sort_values(['date', 'time'])

    df.to_csv(file_path, index=False)
    print(f"Saved {symbol} bars to {file_path}")
