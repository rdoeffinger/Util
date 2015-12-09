package com.hughes.util;

import java.util.List;

// List that works more efficiently when fetching multiple elements
public interface ChunkedList<T> {
    // Get the maximum chunk size
    public int getMaxChunkSize();
    // Get the position where the chunk containing index starts
    public int getChunkStart(int index);
    // Get the chunk starting at index
    // (must be return value of getChunkStart)
    public List<T> getChunk(int index);
}
