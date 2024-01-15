package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator{
    private final HeapFile heapFile;
    private final TransactionId transactionId;
    private int currentPage;
    private Iterator<Tuple> tupleIterator;
    private boolean STATUS;

    HeapFileIterator(HeapFile hf, TransactionId tid) {
        this.heapFile = hf;
        this.transactionId = tid;
    }


    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.STATUS = true;
        this.currentPage = 0;
        this.tupleIterator = getNextPage();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (STATUS) {
            if (this.tupleIterator.hasNext())
                return true;
            while (this.currentPage + 1 < this.heapFile.numPages()) {
                this.currentPage++;
                this.tupleIterator = getNextPage();
                if (this.tupleIterator.hasNext())
                    return true;
            }
        }
        return false;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (hasNext()) {
            return this.tupleIterator.next();
        }
        throw new NoSuchElementException();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        open();
    }

    @Override
    public void close() {
        this.tupleIterator = null;
        this.STATUS = false;
    }

    private Iterator<Tuple> getNextPage() throws TransactionAbortedException, DbException {
        // create new HeapPageId
        HeapPageId pageId = new HeapPageId(this.heapFile.getId(), this.currentPage);
        // read page from BufferPool
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.transactionId, pageId, Permissions.READ_ONLY);
        return page.iterator();
    }
}
