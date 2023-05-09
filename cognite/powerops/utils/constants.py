import string

# � character is used to represent unrecognizable characters in utf-8.
UNRECOGNIZABLE_CHARACTER = "�"
VALID_CHARACTERS = set(
    string.ascii_lowercase
    + string.ascii_uppercase
    + string.digits
    + UNRECOGNIZABLE_CHARACTER
    + string.punctuation
    + string.whitespace
    + "æøåÆØÅ"
)
