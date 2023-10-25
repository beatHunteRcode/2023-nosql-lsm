package ru.vk.itmo.sherepanikita;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;

public class PeekIterator implements Iterator<Entry<MemorySegment>> {

    private final Iterator<Entry<MemorySegment>> iterator;
    private final int priorityIndex;
    private Entry<MemorySegment> currentEntry;

    PeekIterator(Iterator<Entry<MemorySegment>> iterator, int priorityIndex) {
        this.iterator = iterator;
        this.priorityIndex = priorityIndex;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        currentEntry = iterator.next();
        return currentEntry;
    }

    public Entry<MemorySegment> peek() {
        if (currentEntry == null) {
            currentEntry = iterator.next();
        }
        return currentEntry;
    }

    public Entry<MemorySegment> getCurrentEntry() {
        return currentEntry;
    }

    public int getPriorityIndex() {
        return priorityIndex;
    }
}
