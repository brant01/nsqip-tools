"""Configuration management for NSQIP Tools."""

import os
from pathlib import Path
from typing import Optional


def get_data_directory() -> Optional[Path]:
    """Get the NSQIP data directory from environment variables.
    
    Returns:
        Path to NSQIP data directory, or None if not configured.
    
    Environment Variables:
        NSQIP_DATA_DIR: Path to directory containing NSQIP .txt files
    """
    data_dir = os.getenv('NSQIP_DATA_DIR')
    if data_dir:
        return Path(data_dir)
    
    # Check for .env file in current directory
    env_file = Path('.env')
    if env_file.exists():
        try:
            import dotenv
            dotenv.load_dotenv()
            data_dir = os.getenv('NSQIP_DATA_DIR')
            if data_dir:
                return Path(data_dir)
        except ImportError:
            pass  # python-dotenv not installed, continue without it
    
    return None


def get_output_directory() -> Optional[Path]:
    """Get the output directory from environment variables."""
    output_dir = os.getenv('NSQIP_OUTPUT_DIR')
    return Path(output_dir) if output_dir else None


def get_memory_limit() -> str:
    """Get memory limit from environment variables."""
    return os.getenv('NSQIP_MEMORY_LIMIT', '4GB')


def validate_data_directory(data_dir: Path) -> bool:
    """Validate that a directory contains NSQIP data files.
    
    Args:
        data_dir: Directory to validate
        
    Returns:
        True if directory contains NSQIP files
    """
    if not data_dir.exists():
        return False
    
    # Look for typical NSQIP file patterns
    txt_files = list(data_dir.glob('*.txt'))
    if not txt_files:
        return False
    
    # Check for NSQIP-like filenames
    nsqip_patterns = ['nsqip', 'acs', 'adult', 'pediatric', 'peds']
    has_nsqip_file = any(
        any(pattern in f.name.lower() for pattern in nsqip_patterns)
        for f in txt_files
    )
    
    return has_nsqip_file