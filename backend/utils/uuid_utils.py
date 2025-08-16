"""
UUID v7 utilities for consistent ID generation
"""
import random
import time
from uuid import UUID


def uuid7() -> str:
    """
    Generate a UUID v7 (time-ordered UUID)

    UUID v7 format:
    - 48 bits: Unix timestamp in milliseconds
    - 4 bits: Version (0111 for v7)
    - 12 bits: Random data
    - 2 bits: Variant (10 for RFC4122)
    - 62 bits: Random data
    """
    # Get current timestamp in milliseconds
    timestamp_ms = int(time.time() * 1000)

    # Create timestamp part (48 bits)
    timestamp_hex = format(timestamp_ms & 0xFFFFFFFFFFFF, "012x")

    # Version and random bits for time_hi_and_version (4 bits version + 12 bits random)
    version_and_random = 0x7000 | (random.getrandbits(12))
    version_hex = format(version_and_random, "04x")

    # Variant and random bits for clock_seq_hi_and_reserved (2 bits variant + 6 bits random)
    variant_and_random = 0x80 | (random.getrandbits(6))

    # Rest of clock_seq and node (8 + 48 = 56 bits random)
    clock_seq_low = random.getrandbits(8)
    node = random.getrandbits(48)

    # Combine all parts
    return (
        timestamp_hex[:8]
        + "-"
        + timestamp_hex[8:12]
        + "-"
        + version_hex
        + "-"
        + format(variant_and_random, "02x")
        + format(clock_seq_low, "02x")
        + "-"
        + format(node, "012x")
    )


def uuid7_prefix(prefix: str) -> str:
    """Generate a prefixed UUID v7 for readability"""
    return f"{prefix}_{uuid7()}"


def is_uuid7(value: str) -> bool:
    """Check if a string is a valid UUID v7"""
    try:
        # Remove any prefix
        if "_" in value:
            value = value.split("_", 1)[1]

        uuid_obj = UUID(value)
        # Check version field (should be 7)
        uuid_version_7 = 7
        return uuid_obj.version == uuid_version_7
    except Exception:
        return False


def extract_timestamp_from_uuid7(uuid7_str: str) -> int:
    """Extract timestamp in milliseconds from UUID v7"""
    try:
        # Remove any prefix
        if "_" in uuid7_str:
            uuid7_str = uuid7_str.split("_", 1)[1]

        uuid_obj = UUID(uuid7_str)
        # Extract first 48 bits (timestamp)
        return (uuid_obj.int >> 80) & 0xFFFFFFFFFFFF
    except Exception:
        return 0


# Global instance for consistent generation
_uuid7_generator = uuid7


def get_uuid7() -> str:
    """Get a UUID v7 string"""
    return _uuid7_generator()
