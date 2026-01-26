import pyarrow.parquet as pq
import pyarrow as pa
import vortex as vx
from pathlib import Path
from typing import Optional


class FileFusion:
    """
    A class to merge multiple Parquet or Vortex files into single files.
    Useful for testing single-file vs multi-file performance scenarios.
    """
    
    def __init__(
        self,
        input_dir: str = "ny_taxi_files",
        output_dir: str = "ny_taxi_files",
        taxi_type: str = "yellow",
        year: int = 2025
    ):
        """
        Initialize the file fusion class.
        
        Args:
            input_dir: Directory containing input files (default: 'ny_taxi_files')
            output_dir: Directory to save merged files (default: 'ny_taxi_files')
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
    
    def _get_parquet_files(self) -> list[Path]:
        """Get all parquet files matching the pattern."""
        pattern = f"{self.taxi_type}_tripdata_{self.year}-*.parquet"
        files = sorted(self.input_dir.glob(pattern))
        return files
    
    def _get_vortex_files(self) -> list[Path]:
        """Get all vortex files matching the pattern."""
        vortex_dir = self.input_dir / "vortex"
        if not vortex_dir.exists():
            raise ValueError(f"Vortex directory does not exist: {vortex_dir}")
        
        pattern = f"{self.taxi_type}_tripdata_{self.year}-*.vortex"
        files = sorted(vortex_dir.glob(pattern))
        return files
    
    def fuse_parquet_files(
        self,
        output_filename: Optional[str] = None,
        overwrite: bool = False
    ) -> Path:
        """
        Merge all parquet files into a single parquet file.
        
        Args:
            output_filename: Name of the output file (default: auto-generated)
            overwrite: Whether to overwrite existing file (default: False)
            
        Returns:
            Path to the merged parquet file
        """
        parquet_files = self._get_parquet_files()
        
        if not parquet_files:
            raise ValueError(f"No parquet files found matching pattern: {self.taxi_type}_tripdata_{self.year}-*.parquet")
        
        if output_filename is None:
            output_filename = f"{self.taxi_type}_tripdata_{self.year}_merged.parquet"
        
        output_path = self.output_dir / output_filename
        
        if output_path.exists() and not overwrite:
            print(f"File {output_path.name} already exists. Use overwrite=True to replace.")
            return output_path
        
        print(f"Merging {len(parquet_files)} parquet files into {output_path.name}...")
        
        # Read all parquet files
        tables = []
        total_rows = 0
        for parquet_file in parquet_files:
            print(f"  Reading {parquet_file.name}...", end=" ", flush=True)
            table = pq.read_table(parquet_file)
            tables.append(table)
            total_rows += len(table)
            print(f"✓ ({len(table):,} rows)")
        
        # Concatenate all tables
        print(f"  Concatenating {len(tables)} tables...", end=" ", flush=True)
        merged_table = pa.concat_tables(tables)
        print(f"✓ ({len(merged_table):,} total rows)")
        
        # Write merged parquet file
        print(f"  Writing merged file...", end=" ", flush=True)
        pq.write_table(merged_table, output_path)
        print(f"✓")
        
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"✓ Merged {output_path.name} ({file_size_mb:.2f} MB, {len(merged_table):,} rows)")
        
        return output_path
    
    def fuse_vortex_files(
        self,
        output_filename: Optional[str] = None,
        overwrite: bool = False
    ) -> Path:
        """
        Merge all vortex files into a single vortex file.
        
        Args:
            output_filename: Name of the output file (default: auto-generated)
            overwrite: Whether to overwrite existing file (default: False)
            
        Returns:
            Path to the merged vortex file
        """
        vortex_files = self._get_vortex_files()
        
        if not vortex_files:
            raise ValueError(f"No vortex files found matching pattern: {self.taxi_type}_tripdata_{self.year}-*.vortex")
        
        if output_filename is None:
            output_filename = f"{self.taxi_type}_tripdata_{self.year}_merged.vortex"
        
        # Save to vortex subdirectory
        vortex_output_dir = self.output_dir / "vortex"
        vortex_output_dir.mkdir(parents=True, exist_ok=True)
        output_path = vortex_output_dir / output_filename
        
        if output_path.exists() and not overwrite:
            print(f"File {output_path.name} already exists. Use overwrite=True to replace.")
            return output_path
        
        print(f"Merging {len(vortex_files)} vortex files into {output_path.name}...")
        
        # Read all vortex files and convert to Arrow tables
        tables = []
        total_rows = 0
        for vortex_file in vortex_files:
            print(f"  Reading {vortex_file.name}...", end=" ", flush=True)
            vf = vx.open(str(vortex_file))
            table = vf.scan().read_all().to_arrow_table()
            tables.append(table)
            total_rows += len(table)
            print(f"✓ ({len(table):,} rows)")
        
        # Concatenate all tables
        print(f"  Concatenating {len(tables)} tables...", end=" ", flush=True)
        merged_table = pa.concat_tables(tables)
        print(f"✓ ({len(merged_table):,} total rows)")
        
        # Convert to vortex array and write
        print(f"  Converting to vortex format...", end=" ", flush=True)
        vtx = vx.array(merged_table)
        print(f"✓")
        
        print(f"  Writing merged file...", end=" ", flush=True)
        vx.io.write(vtx, str(output_path))
        print(f"✓")
        
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"✓ Merged {output_path.name} ({file_size_mb:.2f} MB, {len(merged_table):,} rows)")
        
        return output_path
    
    def fuse_all(
        self,
        parquet_output: Optional[str] = None,
        vortex_output: Optional[str] = None,
        overwrite: bool = False
    ) -> dict:
        """
        Merge both parquet and vortex files.
        
        Args:
            parquet_output: Name of the merged parquet file (default: auto-generated)
            vortex_output: Name of the merged vortex file (default: auto-generated)
            overwrite: Whether to overwrite existing files (default: False)
            
        Returns:
            Dictionary with paths to merged files
        """
        results = {}
        
        print("=" * 60)
        print("File Fusion: Merging Parquet and Vortex Files")
        print("=" * 60)
        print()
        
        # Merge parquet files
        try:
            print("Merging Parquet files...")
            parquet_path = self.fuse_parquet_files(parquet_output, overwrite)
            results['parquet'] = parquet_path
            print()
        except Exception as e:
            print(f"✗ Error merging parquet files: {e}")
            results['parquet'] = None
        
        # Merge vortex files
        try:
            print("Merging Vortex files...")
            vortex_path = self.fuse_vortex_files(vortex_output, overwrite)
            results['vortex'] = vortex_path
            print()
        except Exception as e:
            print(f"✗ Error merging vortex files: {e}")
            results['vortex'] = None
        
        print("=" * 60)
        print("File fusion complete!")
        print("=" * 60)
        
        return results


def main():
    """
    Main function to run the file fusion.
    """
    fusion = FileFusion(
        input_dir="ny_taxi_files",
        output_dir="ny_taxi_files",
        taxi_type="yellow",
        year=2025
    )
    
    fusion.fuse_all(overwrite=False)


if __name__ == "__main__":
    main()
