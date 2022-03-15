

def remove_prefix(text, prefix):
    if text.startswith(prefix):
        text = text[len(prefix) :]
        text = text.strip()
    return text