from transforms.chunking import chunk_text


def test_chunk_text_short_string_single_chunk():
    text = "hello world"
    chunks = chunk_text(text)
    assert len(chunks) == 1
    assert chunks[0] == text


def test_chunk_text_splits_long_string():
    text = "word " * 500
    chunks = chunk_text(text)
    assert len(chunks) >= 2
    assert all(len(c) <= 1000 for c in chunks)


def test_chunk_text_overlap_means_naive_join_differs_from_source():
    """chunk_overlap > 0: naive join duplicates overlap; source != ''.join(chunks)."""
    text = "word " * 500
    chunks = chunk_text(text)
    assert len(chunks) >= 2
    joined = "".join(chunks)
    assert len(joined) >= len(text)
