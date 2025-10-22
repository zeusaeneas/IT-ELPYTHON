def shout(s):
    """Return the uppercase version of the string."""
    return s.upper()

"IMPROVED CODE"
def shout(s: str) -> str:
    """Return the uppercase version of the string."""
    if not isinstance(s, str):
        raise TypeError("Input must be a string")
    return s.upper()

