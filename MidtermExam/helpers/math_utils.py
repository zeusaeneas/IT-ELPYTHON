def area(l, w):
    """Return the area of a rectangle."""
    return l * w

" IMPROVED CODE" 
def area(l: float, w: float) -> float:
    """Return the area of a rectangle."""
    if not (isinstance(l, (int, float)) and isinstance(w, (int, float))):
        raise TypeError("Length and width must be numbers")
    return l * w
