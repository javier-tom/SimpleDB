package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    // Fields
    private final File file;
    private final TupleDesc tupleDesc;
    private final int pageId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
        this.pageId = file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.pageId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        // To read a page from disk, you will first need to calculate the correct offset in the file.
        // Hint: you will need random access to the file in order to read and write pages at arbitrary offsets.
        // You should not call BufferPool methods when reading a page from disk.

        // create a byte array that will hold data read.
        int pageSize = BufferPool.getPageSize();
        byte[] data = new byte[pageSize];

        // create a RandomAccessFile to read file from disk.
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(getFile(), "r");
            int offset = pid.getPageNumber() * pageSize;
            // check to make sure we don't try to read more than is available to read.
            randomAccessFile.seek(offset);
            randomAccessFile.read(data);
            randomAccessFile.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public synchronized void writePage(Page page) throws IOException {
        // some code goes here
        RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "rws");
        int offset = page.getId().getPageNumber() * BufferPool.getPageSize();
        randomAccessFile.seek(offset);
        randomAccessFile.write(page.getPageData());
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil((double) this.file.length() / BufferPool.getPageSize());
//        return (int) (this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // iterate through each page on this file and see if we can insert
        // tuple to that page.
        for (int i = 0; i < numPages(); i++) {
            PageId pageId = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);

            // found a page with available space
            if (page.getNumEmptySlots() > 0) {
                page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
                page.insertTuple(t);
                return new ArrayList<>(Arrays.asList(page));
            }
        }
        // create new page to hold tuple
        PageId pageId = new HeapPageId(getId(), numPages());
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.insertTuple(t);
        writePage(page);
        return new ArrayList<>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pageId = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

}

