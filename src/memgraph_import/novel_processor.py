"""
Novel text processing functionality.
"""

import re
import pandas as pd
from typing import List, Optional
from datetime import datetime
from .models import NovelChunk, NovelData


def novel_to_dataframe(novel_text: str) -> pd.DataFrame:
    """
    Takes the full plain text of a novel, identifies chapters,
    chunks the text by paragraphs within each chapter, and returns a pandas DataFrame.

    Args:
        novel_text: A string containing the full plain text of the novel.

    Returns:
        A pandas DataFrame with columns 'chapter', 'chunk', and 'chunk_order_number'.
    """
    # Remove Project Gutenberg headers/footers
    start_marker_pattern = r"\*\*\* START OF THE PROJECT GUTENBERG EBOOK [^*]+\*\*\*"
    end_marker_pattern = r"\*\*\* END OF THE PROJECT GUTENBERG EBOOK [^*]+\*\*\*"

    # Remove text before the start marker
    start_match = re.search(start_marker_pattern, novel_text)
    if start_match:
        novel_text = novel_text[start_match.end() :]

    # Remove text after the end marker
    end_match = re.search(end_marker_pattern, novel_text)
    if end_match:
        novel_text = novel_text[: end_match.start()]

    novel_text = novel_text.strip()

    chapters_data = []
    chunk_order_counter = 0

    # Regex to find chapter titles
    chapter_pattern = re.compile(r"^(CHAPTER [IVXLCDM]+\.)", re.MULTILINE)
    matches = list(chapter_pattern.finditer(novel_text))

    if not matches:
        # If no chapters found, treat whole text as chunks under 'Unknown' chapter
        paragraphs = re.split(r"\n\s*\n+", novel_text)
        for para_content in paragraphs:
            para_cleaned = para_content.strip()
            if para_cleaned:
                chunk_order_counter += 1
                chapters_data.append(
                    {
                        "chapter": "Unknown",
                        "chunk": para_cleaned,
                        "chunk_order_number": chunk_order_counter,
                    }
                )
        if chapters_data:
            return pd.DataFrame(chapters_data)
        else:
            return pd.DataFrame(columns=["chapter", "chunk", "chunk_order_number"])

    # Process text before first chapter
    first_chapter_start_index = matches[0].start()
    text_before_first_chapter = novel_text[:first_chapter_start_index].strip()
    if text_before_first_chapter:
        paragraphs_before = re.split(r"\n\s*\n+", text_before_first_chapter)
        for para_content in paragraphs_before:
            para_cleaned = para_content.strip()
            if para_cleaned:
                chunk_order_counter += 1
                chapters_data.append(
                    {
                        "chapter": "Preamble",
                        "chunk": para_cleaned,
                        "chunk_order_number": chunk_order_counter,
                    }
                )

    # Process each chapter
    for i, match in enumerate(matches):
        chapter_title = match.group(1)
        content_start_index = match.end()
        content_end_index = (
            matches[i + 1].start() if i + 1 < len(matches) else len(novel_text)
        )
        chapter_content = novel_text[content_start_index:content_end_index].strip()

        # Split chapter content into paragraphs
        paragraphs = re.split(r"\n\s*\n+", chapter_content)
        for para_content in paragraphs:
            para_cleaned = para_content.strip()
            if para_cleaned:
                chunk_order_counter += 1
                chapters_data.append(
                    {
                        "chapter": chapter_title,
                        "chunk": para_cleaned,
                        "chunk_order_number": chunk_order_counter,
                    }
                )

    return pd.DataFrame(chapters_data)


def dataframe_to_novel_data(df: pd.DataFrame, author: str, book: str) -> NovelData:
    """
    Convert a DataFrame of novel chunks to a NovelData object.

    Args:
        df: DataFrame with novel chunks
        author: Author name
        book: Book title

    Returns:
        NovelData object containing the novel chunks
    """
    chunks = [
        NovelChunk(
            chapter=row["chapter"],
            chunk=row["chunk"],
            chunk_order_number=row["chunk_order_number"],
            author=author,
            book=book,
        )
        for _, row in df.iterrows()
    ]
    return NovelData(chunks=chunks, author=author, book=book)


def process_novel_file(file_path: str, author: str, book: str) -> NovelData:
    """
    Process a novel text file into a NovelData object.

    Args:
        file_path: Path to the novel text file
        author: Author name
        book: Book title

    Returns:
        NovelData object containing the processed novel
    """
    with open(file_path, "r") as file:
        novel_text = file.read()

    df = novel_to_dataframe(novel_text)
    df["author"] = author
    df["book"] = book
    return dataframe_to_novel_data(df, author, book)
