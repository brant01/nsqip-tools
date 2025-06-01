"""Memory utilities for optimizing data processing performance."""
import platform
import psutil
from typing import Optional


def get_available_memory() -> int:
    """Get available system memory in bytes.
    
    Returns:
        Available memory in bytes.
    """
    return psutil.virtual_memory().available


def get_total_memory() -> int:
    """Get total system memory in bytes.
    
    Returns:
        Total memory in bytes.
    """
    return psutil.virtual_memory().total


def format_bytes(num_bytes: int) -> str:
    """Format bytes into human-readable string.
    
    Args:
        num_bytes: Number of bytes.
        
    Returns:
        Formatted string (e.g., "4.5GB", "512MB").
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if num_bytes < 1024.0:
            return f"{num_bytes:.1f}{unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f}PB"


def get_recommended_memory_limit(conservative: bool = True) -> str:
    """Get recommended memory limit for data processing based on available RAM.
    
    This provides a soft limit for operations to ensure the system remains
    responsive while processing large datasets.
    
    Args:
        conservative: If True, use more conservative memory allocation.
                     Recommended for systems running other applications.
    
    Returns:
        Memory limit string (e.g., "4GB").
    """
    total_memory = get_total_memory()
    available_memory = get_available_memory()
    
    # Use the lesser of total or available memory as base
    base_memory = min(total_memory, available_memory)
    
    if conservative:
        # Use 50% of available memory or 60% of total, whichever is less
        # This is more appropriate for data processing tasks
        recommended = min(
            int(available_memory * 0.5),
            int(total_memory * 0.6)
        )
    else:
        # Use 70% of available memory or 80% of total, whichever is less
        recommended = min(
            int(available_memory * 0.7),
            int(total_memory * 0.8)
        )
    
    # Set minimum and maximum bounds
    min_memory = 1 * 1024 * 1024 * 1024  # 1GB minimum
    max_memory = 32 * 1024 * 1024 * 1024  # 32GB maximum (reasonable for most systems)
    
    recommended = max(min_memory, min(recommended, max_memory))
    
    # Round to nearest GB for cleaner settings
    recommended_gb = max(1, round(recommended / (1024 * 1024 * 1024)))
    
    return f"{recommended_gb}GB"


def get_memory_info() -> dict:
    """Get detailed memory information.
    
    Returns:
        Dictionary with memory information including:
        - total: Total system memory
        - available: Currently available memory
        - used: Currently used memory
        - percent: Percentage of memory used
        - recommended_limit: Recommended DuckDB memory limit
    """
    mem = psutil.virtual_memory()
    
    return {
        "total": format_bytes(mem.total),
        "available": format_bytes(mem.available),
        "used": format_bytes(mem.used),
        "percent": mem.percent,
        "recommended_limit": get_recommended_memory_limit(),
        "platform": platform.system()
    }