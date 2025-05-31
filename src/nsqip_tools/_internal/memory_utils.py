"""Memory utilities for optimizing DuckDB performance."""
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
    """Get recommended DuckDB memory limit based on available RAM.
    
    DuckDB can use more memory than specified, but this sets a soft limit
    for its buffer pool. We recommend conservative settings to ensure
    the system remains responsive.
    
    Args:
        conservative: If True, use more conservative memory allocation.
                     Recommended for systems running other applications.
    
    Returns:
        Memory limit string suitable for DuckDB (e.g., "4GB").
    """
    total_memory = get_total_memory()
    available_memory = get_available_memory()
    
    # Use the lesser of total or available memory as base
    base_memory = min(total_memory, available_memory)
    
    if conservative:
        # Use 40% of available memory or 50% of total, whichever is less
        recommended = min(
            int(available_memory * 0.4),
            int(total_memory * 0.5)
        )
    else:
        # Use 60% of available memory or 70% of total, whichever is less
        recommended = min(
            int(available_memory * 0.6),
            int(total_memory * 0.7)
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