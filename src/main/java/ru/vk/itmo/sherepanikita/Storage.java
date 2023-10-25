package ru.vk.itmo.sherepanikita;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Storage {

    private static final String FILE_NAME = "storage";

    private final Path filePath;

    MemorySegment dataSegment;

    private final Arena arena;

    public Storage(Config config) throws IOException {
        this.filePath = config.basePath().resolve(FILE_NAME);
        arena = Arena.ofShared();

        try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)){
            indexSegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0)
            dataSegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(filePath), arena);
        }

    }

    public PeekIterator getIterator(MemorySegment from, MemorySegment to) {
        return new PeekIterator(new StorageIterator(from, to));
    }

    private long binarySearch(MemorySegment segmentToSearch, MemorySegment key) {
        long first = 0;
        long last = (segmentToSearch.byteSize() / Long.BYTES) - 1;

        while (first < last) {
            long middle = first + (last - first) / 2;
            long offset = segmentToSearch.get(ValueLayout.JAVA_LONG_UNALIGNED, middle * Long.BYTES);
            long keySize = segmentToSearch.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            MemorySegment middleKey = segmentToSearch.asSlice(offset, keySize);

            int compareResult = new MemorySegmentComparator().compare(middleKey, key);

            if (compareResult < 0) {
                first = middle + 1;
            }
            else if (compareResult > 0) {
                last = middle - 1;
            }
            else {
                return middle * Long.BYTES;
            }
        }

        return first * Long.BYTES;
    }


    class StorageIterator implements Iterator<Entry<MemorySegment>> {

        private final MemorySegment to;

        private long indexOffset;

        private Long currentOffset = null;
        private Long currentSize = null;

        public StorageIterator(MemorySegment from, MemorySegment to) {
            indexOffset = binarySearch(dataSegment, from);
            this.to = to;
        }

        @Override
        public boolean hasNext() {
            if (indexOffset == dataSegment.byteSize()) {
                return false;
            }
            if (to == null) {
                return true;
            }

            currentSize = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, currentOffset);

            long fromOffset = currentOffset + Long.BYTES;

            return new MemorySegmentComparator().compareWithOffset(
                    to,
                    dataSegment,
                    fromOffset,
                    fromOffset + currentSize
            ) > 0;
        }

        @Override
        public Entry<MemorySegment> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            long offset;
            long size;
            if (currentOffset == null || currentSize == null) {
                offset = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset);
                size = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            } else {
                offset = currentOffset;
                size = currentSize;
            }

            indexOffset += Long.BYTES;
            offset += Long.BYTES;

            long valueSize = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);

            MemorySegment key = dataSegment.asSlice(offset, size);
            MemorySegment value = dataSegment.asSlice(offset + Long.BYTES, valueSize);

            return new BaseEntry<>(key, value);

        }
    }

}
