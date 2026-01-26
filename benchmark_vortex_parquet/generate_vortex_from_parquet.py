import pyarrow.parquet as pq
import vortex as vx
from pathlib import Path


class ParquetToVortexConverter:
    """
    A class to convert NYC taxi parquet files to Vortex format.
    Converts all parquet files in the input directory to vortex files in the output directory.
    """
    
    def __init__(
        self,
        input_dir: str = "ny_taxi_files",
        output_dir: str = "ny_taxi_files/vortex",
        taxi_type: str = "yellow",
        year: int = 2025
    ):
        """
        Initialize the converter.
        
        Args:
            input_dir: Directory containing parquet files (default: 'ny_taxi_files')
            output_dir: Directory to save vortex files (default: 'ny_taxi_files/vortex')
            taxi_type: Type of taxi data ('yellow' or 'green', default: 'yellow')
            year: Year of the data files (default: 2025)
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.taxi_type = taxi_type
        self.year = year
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        if not self.input_dir.exists():
            raise ValueError(f"Input directory does not exist: {self.input_dir}")
    
    def _get_parquet_path(self, month: int) -> Path:
        """
        Get the path to a parquet file for a specific month.
        
        Args:
            month: Month number (1-12)
            
        Returns:
            Path object for the parquet file
        """
        month_str = f"{month:02d}"
        filename = f"{self.taxi_type}_tripdata_{self.year}-{month_str}.parquet"
        return self.input_dir / filename
    
    def _get_vortex_path(self, month: int) -> Path:
        """
        Get the output path for a vortex file for a specific month.
        
        Args:
            month: Month number (1-12)
            
        Returns:
            Path object for the vortex file
        """
        month_str = f"{month:02d}"
        filename = f"{self.taxi_type}_tripdata_{self.year}-{month_str}.vortex"
        return self.output_dir / filename
    
    def convert_month(self, month: int, overwrite: bool = False) -> bool:
        """
        Convert parquet file for a specific month to vortex format.
        
        Args:
            month: Month number (1-12)
            overwrite: Whether to overwrite existing files (default: False)
            
        Returns:
            True if conversion was successful, False otherwise
        """
        if month < 1 or month > 12:
            raise ValueError("Month must be between 1 and 12")
        
        parquet_path = self._get_parquet_path(month)
        vortex_path = self._get_vortex_path(month)
        
        # Check if parquet file exists
        if not parquet_path.exists():
            print(f"Parquet file {parquet_path.name} does not exist. Skipping...")
            return False
        
        # Skip if vortex file exists and overwrite is False
        if vortex_path.exists() and not overwrite:
            print(f"Vortex file {vortex_path.name} already exists. Skipping...")
            return True
        
        try:
            print(f"Converting {parquet_path.name} to {vortex_path.name}...")
            
            # Read parquet file
            parquet_table = pq.read_table(parquet_path)
            
            # Convert to vortex array
            vtx = vx.array(parquet_table)
            
            # Write vortex file
            vx.io.write(vtx, str(vortex_path))
            
            # Get file sizes for comparison
            parquet_size_mb = parquet_path.stat().st_size / (1024 * 1024)
            vortex_size_mb = vortex_path.stat().st_size / (1024 * 1024)
            compression_ratio = vortex_path.stat().st_size / parquet_path.stat().st_size
            
            print(f"✓ Converted {parquet_path.name} to {vortex_path.name}")
            print(f"  Parquet size: {parquet_size_mb:.2f} MB")
            print(f"  Vortex size: {vortex_size_mb:.2f} MB")
            print(f"  Compression ratio: {compression_ratio:.2f}x")
            
            return True
            
        except Exception as e:
            print(f"✗ Error converting {parquet_path.name}: {e}")
            return False
    
    def convert_all_months(self, overwrite: bool = False) -> dict:
        """
        Convert all parquet files in the input directory to vortex format.
        
        Args:
            overwrite: Whether to overwrite existing files (default: False)
            
        Returns:
            Dictionary with conversion results: {'successful': [...], 'failed': [...]}
        """
        results = {'successful': [], 'failed': []}
        
        print(f"Starting conversion from parquet to vortex format")
        print(f"Input directory: {self.input_dir.absolute()}")
        print(f"Output directory: {self.output_dir.absolute()}\n")
        
        # Try to convert all 12 months
        for month in range(1, 13):
            success = self.convert_month(month, overwrite=overwrite)
            if success:
                results['successful'].append(month)
            else:
                results['failed'].append(month)
        
        print(f"\n{'='*60}")
        print(f"Conversion complete!")
        print(f"Successful: {len(results['successful'])} files")
        print(f"Failed: {len(results['failed'])} files")
        if results['failed']:
            print(f"Failed months: {results['failed']}")
        print(f"{'='*60}")
        
        return results
    
    def convert_all_available(self, overwrite: bool = False) -> dict:
        """
        Convert all parquet files found in the input directory (not just 12 months).
        This is useful if you have a different number of files.
        
        Args:
            overwrite: Whether to overwrite existing files (default: False)
            
        Returns:
            Dictionary with conversion results: {'successful': [...], 'failed': [...]}
        """
        results = {'successful': [], 'failed': []}
        
        print(f"Starting conversion from parquet to vortex format")
        print(f"Input directory: {self.input_dir.absolute()}")
        print(f"Output directory: {self.output_dir.absolute()}\n")
        
        # Find all parquet files matching the pattern
        pattern = f"{self.taxi_type}_tripdata_{self.year}-*.parquet"
        parquet_files = list(self.input_dir.glob(pattern))
        
        if not parquet_files:
            print(f"No parquet files found matching pattern: {pattern}")
            return results
        
        print(f"Found {len(parquet_files)} parquet file(s) to convert\n")
        
        for parquet_path in sorted(parquet_files):
            # Extract month from filename
            try:
                # Filename format: yellow_tripdata_2025-01.parquet
                month_str = parquet_path.stem.split('-')[-1]
                month = int(month_str)
                
                success = self.convert_month(month, overwrite=overwrite)
                if success:
                    results['successful'].append(month)
                else:
                    results['failed'].append(month)
            except (ValueError, IndexError) as e:
                print(f"✗ Could not extract month from filename {parquet_path.name}: {e}")
                results['failed'].append(parquet_path.name)
        
        print(f"\n{'='*60}")
        print(f"Conversion complete!")
        print(f"Successful: {len(results['successful'])} files")
        print(f"Failed: {len(results['failed'])} files")
        if results['failed']:
            print(f"Failed files: {results['failed']}")
        print(f"{'='*60}")
        
        return results


def main():
    """
    Main function to run the converter.
    Can be customized to change input/output directories, taxi type, or year.
    """
    converter = ParquetToVortexConverter(
        input_dir="ny_taxi_files",
        output_dir="ny_taxi_files/vortex",
        taxi_type="yellow",
        year=2025
    )
    
    # Use convert_all_available to convert all parquet files found
    # or use convert_all_months() to convert exactly 12 months
    converter.convert_all_available(overwrite=False)


if __name__ == "__main__":
    main()
