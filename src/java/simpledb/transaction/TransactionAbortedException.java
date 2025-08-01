package simpledb.transaction;

/** Exception that is thrown when a transaction has aborted. */
public class TransactionAbortedException extends Exception {
    private static final long serialVersionUID = 1L;

    public TransactionAbortedException(String msg) {
        System.out.println(msg);
    }
}
