import requests
from pathlib import Path


class NYCParquetDownloader:
    """
    A class to download NYC taxi parquet files for a given year.
    Downloads files for all 12 months (01-12) to the specified output directory.
    """
    
    BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    
    def __init__(
        self,
        year: int = 2023,
        taxi_type: str = "yellow",
        output_dir: str = "ny_taxi_files"
    ):
        """
        Initialize the downloader.
        
        Args:
            year: Year to download data for (default: 2023)
            taxi_type: Type of taxi data ('yellow' or 'green', default: 'yellow')
            output_dir: Directory to save downloaded files (default: 'ny_taxi_files')
        """
        self.year = year
        self.taxi_type = taxi_type
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def _get_url(self, month: int) -> str:
        """
        Generate the URL for a specific month.
        
        Args:
            month: Month number (1-12)
            
        Returns:
            URL string for the parquet file
        """
        month_str = f"{month:02d}"
        filename = f"{self.taxi_type}_tripdata_{self.year}-{month_str}.parquet"
        return f"{self.BASE_URL}/{filename}"
    
    def _get_output_path(self, month: int) -> Path:
        """
        Get the output file path for a specific month.
        
        Args:
            month: Month number (1-12)
            
        Returns:
            Path object for the output file
        """
        month_str = f"{month:02d}"
        filename = f"{self.taxi_type}_tripdata_{self.year}-{month_str}.parquet"
        return self.output_dir / filename
    
    def download_month(self, month: int, overwrite: bool = False) -> bool:
        """
        Download parquet file for a specific month.
        
        Args:
            month: Month number (1-12)
            overwrite: Whether to overwrite existing files (default: False)
            
        Returns:
            True if download was successful, False otherwise
        """
        if month < 1 or month > 12:
            raise ValueError("Month must be between 1 and 12")
        
        url = self._get_url(month)
        output_path = self._get_output_path(month)
        
        # Skip if file exists and overwrite is False
        if output_path.exists() and not overwrite:
            print(f"File {output_path.name} already exists. Skipping...")
            return True
        
        try:
            print(f"Downloading {output_path.name}...")
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Write file in chunks to handle large files
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            file_size_mb = output_path.stat().st_size / (1024 * 1024)
            print(f"✓ Downloaded {output_path.name} ({file_size_mb:.2f} MB)")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"✗ Error downloading {output_path.name}: {e}")
            return False
        except Exception as e:
            print(f"✗ Unexpected error downloading {output_path.name}: {e}")
            return False
    
    def download_all_months(self, overwrite: bool = False) -> dict:
        """
        Download parquet files for all 12 months.
        
        Args:
            overwrite: Whether to overwrite existing files (default: False)
            
        Returns:
            Dictionary with download results: {'successful': [...], 'failed': [...]}
        """
        results = {'successful': [], 'failed': []}
        
        print(f"Starting download for {self.taxi_type} taxi data, year {self.year}")
        print(f"Output directory: {self.output_dir.absolute()}\n")
        
        for month in range(1, 13):
            success = self.download_month(month, overwrite=overwrite)
            if success:
                results['successful'].append(month)
            else:
                results['failed'].append(month)
        
        print(f"\n{'='*60}")
        print(f"Download complete!")
        print(f"Successful: {len(results['successful'])} files")
        print(f"Failed: {len(results['failed'])} files")
        if results['failed']:
            print(f"Failed months: {results['failed']}")
        print(f"{'='*60}")
        
        return results


def main():
    """
    Main function to run the downloader.
    Can be customized to change year, taxi type, or output directory.
    """
    downloader = NYCParquetDownloader(
        year=2025,
        taxi_type="yellow",
        output_dir="ny_taxi_files"
    )
    
    downloader.download_all_months(overwrite=False)


if __name__ == "__main__":
    main()
